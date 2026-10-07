use super::RunDslError;
use super::ast::{
    AggregateAst, AggregateFunctionAst, CaseModeAst, CastTypeAst, CompiledRunTask, DedupeKeepAst,
    FilterPredicateAst, MissingModeAst, PrepareStepAst, RUN_DSL_SCHEMA_VERSION, RunExportAst,
    RunPrepareAst, RunSourceAst, RunTaskAst, RunTransformAst, SourceFormat, SourceIdAst,
    SourceIdMode, TrainingSplitsAst, canonicalize_run_task,
};
use super::lexer::{Token, TokenKind, lex};
use serde_json::{Number, Value};
use std::collections::BTreeSet;
use std::path::{Component, Path};

const RESERVED_SOURCE_ID: &str = "_datajig_source_id";

pub fn compile_dsl(input: &str) -> Result<CompiledRunTask, RunDslError> {
    let tokens = lex(input)?;
    reject_known_unsupported(&tokens)?;
    Parser::new(tokens, input.len()).parse()
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
    end_position: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>, end_position: usize) -> Self {
        Self {
            tokens,
            cursor: 0,
            end_position,
        }
    }

    fn parse(mut self) -> Result<CompiledRunTask, RunDslError> {
        self.expect_keyword("from", "source", "start the task with `from <path>`")?;
        let (path, path_position) =
            self.take_value("source.path", "provide a relative source path after `from`")?;
        validate_path(&path, path_position)?;

        let explicit_format = if self.consume_keyword("format") {
            let (value, position) = self.take_value(
                "source.format",
                "use one of: format csv, format jsonl, format parquet",
            )?;
            Some(parse_format(&value, position)?)
        } else {
            None
        };
        let format = explicit_format.or_else(|| infer_format(&path));

        if !self.consume_keyword("source-id-field") {
            return Err(self.error(
                "INVALID_DSL",
                "source.source-id-field",
                "the source row identity is required",
                "add `source-id-field <field>` or explicitly opt in with `source-id-field auto`",
            ));
        }
        let (source_id, source_id_position) = self.take_value(
            "source.source-id-field",
            "provide a field name or `auto` after `source-id-field`",
        )?;
        let source_id_field = if source_id.eq_ignore_ascii_case("auto") {
            SourceIdAst {
                mode: SourceIdMode::Auto,
                field: RESERVED_SOURCE_ID.into(),
            }
        } else {
            reject_reserved_field(&source_id, "source.source-id-field", source_id_position)?;
            SourceIdAst {
                mode: SourceIdMode::Field,
                field: source_id,
            }
        };

        let mut prepare_steps = if self.consume_keyword("prepare") {
            self.parse_prepare_steps()?
        } else {
            Vec::new()
        };

        let parsed_transform = if self.consume_keyword("transform") {
            Some(self.parse_sql_transform()?)
        } else if self.consume_keyword("aggregate") {
            Some(self.parse_aggregate_transform()?)
        } else {
            None
        };

        if !self.consume_keyword("export") {
            return Err(self.error(
                "INVALID_DSL",
                "export",
                "the M0 parser expected the export segment",
                "finish the task with `export id-field <field>`",
            ));
        }
        let splits = if self.consume_keyword("train") {
            let train = self.take_percentage("export.splits")?;
            self.expect_keyword("val", "export.splits", "use `train 80 val 10 test 10`")?;
            let val = self.take_percentage("export.splits")?;
            self.expect_keyword("test", "export.splits", "use `train 80 val 10 test 10`")?;
            let test = self.take_percentage("export.splits")?;
            validate_splits(train, val, test, self.position())?;
            TrainingSplitsAst { train, val, test }
        } else {
            TrainingSplitsAst::default()
        };

        let aggregate_default_id = match &parsed_transform {
            Some(RunTransformAst::Aggregate { by, .. }) => Some(by.clone()),
            _ => None,
        };
        let id_field = if self.consume_keyword("id-field") {
            self.take_value(
                "export.id-field",
                "provide the final record field after `id-field`",
            )?
            .0
        } else if let Some(by) = aggregate_default_id {
            by
        } else {
            return Err(self.error(
                "INVALID_DSL",
                "export.id-field",
                "the final record identity is required for a non-aggregate task",
                "add `id-field <field>` after the export split settings",
            ));
        };
        if self.cursor != self.tokens.len() {
            return Err(self.error(
                "INVALID_DSL",
                "task",
                "unexpected input after the export segment",
                "remove trailing input or use the DataJig DSL v1 grammar",
            ));
        }

        let generated_source_id =
            (source_id_field.mode == SourceIdMode::Auto).then(|| RESERVED_SOURCE_ID.to_owned());
        if generated_source_id.is_some() {
            preserve_generated_source_id(&mut prepare_steps);
        } else {
            reject_reserved_field(&id_field, "export.id-field", self.end_position)?;
        }
        if let Some(RunTransformAst::Aggregate {
            by, aggregations, ..
        }) = &parsed_transform
        {
            let is_output = id_field == *by
                || aggregations
                    .iter()
                    .any(|aggregate| aggregate.alias == id_field);
            if !is_output {
                return Err(RunDslError::new(
                    "INVALID_DSL",
                    "export.id-field",
                    self.end_position,
                    format!("id-field `{id_field}` is not present in the aggregate output"),
                    "use the group field or an aggregate alias as id-field",
                ));
            }
        }
        let transform = parsed_transform.unwrap_or_else(|| RunTransformAst::Sql {
            sql: format!(
                "SELECT * FROM source ORDER BY {}",
                sql_identifier(&id_field)
            ),
            generated: true,
        });
        let ast = RunTaskAst {
            namespace: "datajig".into(),
            kind: "run_task".into(),
            schema_version: RUN_DSL_SCHEMA_VERSION,
            source: RunSourceAst {
                path,
                format,
                source_id_field,
            },
            prepare: RunPrepareAst {
                generated_source_id,
                steps: prepare_steps,
            },
            transform,
            export: RunExportAst { splits, id_field },
        };
        canonicalize_run_task(ast)
    }

    fn parse_sql_transform(&mut self) -> Result<RunTransformAst, RunDslError> {
        let Some(token) = self.tokens.get(self.cursor).cloned() else {
            return Err(self.error(
                "INVALID_DSL",
                "transform.sql",
                "the explicit transform SQL is missing",
                "wrap SQL in backticks, for example `SELECT id FROM source ORDER BY id`",
            ));
        };
        let TokenKind::Backtick(sql) = token.kind else {
            return Err(RunDslError::new(
                "INVALID_DSL",
                "transform.sql",
                token.start,
                "explicit transform SQL must be enclosed in backticks",
                "use `transform `SELECT ... FROM source ...``",
            ));
        };
        if sql.trim().is_empty() {
            return Err(RunDslError::new(
                "INVALID_DSL",
                "transform.sql",
                token.start,
                "explicit transform SQL cannot be empty",
                "provide one SELECT query over the `source` alias",
            ));
        }
        self.cursor += 1;
        Ok(RunTransformAst::Sql {
            sql: sql.trim().to_owned(),
            generated: false,
        })
    }

    fn parse_aggregate_transform(&mut self) -> Result<RunTransformAst, RunDslError> {
        self.expect_keyword(
            "by",
            "transform.aggregate.by",
            "use `aggregate by <field> <alias>=<function>(<field>)`",
        )?;
        let (by, by_position) = self.take_value(
            "transform.aggregate.by",
            "provide exactly one grouping field after `by`",
        )?;
        reject_reserved_field(&by, "transform.aggregate.by", by_position)?;

        let mut aggregations = Vec::new();
        let mut aliases = BTreeSet::new();
        loop {
            let index = aggregations.len();
            let (alias, alias_position) = self.take_value(
                &format!("transform.aggregate[{index}].alias"),
                "use `<alias>=count(<field>)` or sum/avg/min/max",
            )?;
            if alias == by || alias == RESERVED_SOURCE_ID || !aliases.insert(alias.clone()) {
                return Err(RunDslError::new(
                    "INVALID_DSL",
                    format!("transform.aggregate[{index}].alias"),
                    alias_position,
                    format!("aggregate alias `{alias}` collides with another output field"),
                    "choose an alias distinct from the group field, reserved field, and other aliases",
                ));
            }
            self.expect_symbol(
                TokenKind::Equals,
                &format!("transform.aggregate[{index}].alias"),
                "write aggregate specs as `<alias>=<function>(<field>)`",
            )?;
            let function_location = format!("transform.aggregate[{index}].function");
            let (function, function_position) =
                self.take_value(&function_location, "use one of count/sum/avg/min/max")?;
            let function =
                parse_aggregate_function(&function, &function_location, function_position)?;
            self.expect_symbol(
                TokenKind::LeftParen,
                &format!("transform.aggregate[{index}].field"),
                "wrap the aggregate field in parentheses",
            )?;
            let (field, field_position) = self.take_value(
                &format!("transform.aggregate[{index}].field"),
                "provide one field inside the aggregate parentheses",
            )?;
            reject_reserved_field(
                &field,
                &format!("transform.aggregate[{index}].field"),
                field_position,
            )?;
            self.expect_symbol(
                TokenKind::RightParen,
                &format!("transform.aggregate[{index}].field"),
                "close the aggregate field with `)`",
            )?;
            aggregations.push(AggregateAst {
                alias,
                function,
                field,
            });
            if matches!(self.peek_kind(), Some(TokenKind::Comma)) {
                self.cursor += 1;
                continue;
            }
            break;
        }
        Ok(RunTransformAst::Aggregate {
            order_by: by.clone(),
            by,
            aggregations,
        })
    }

    fn expect_symbol(
        &mut self,
        expected: TokenKind,
        location: &str,
        remediation: &str,
    ) -> Result<(), RunDslError> {
        if self.peek_kind() == Some(&expected) {
            self.cursor += 1;
            Ok(())
        } else {
            Err(self.error(
                "INVALID_DSL",
                location,
                "aggregate punctuation is malformed",
                remediation,
            ))
        }
    }

    fn parse_prepare_steps(&mut self) -> Result<Vec<PrepareStepAst>, RunDslError> {
        let mut steps = Vec::new();
        loop {
            let step_index = steps.len();
            steps.push(self.parse_prepare_step(step_index)?);
            if !matches!(self.peek_kind(), Some(TokenKind::Comma)) {
                break;
            }
            if !self
                .tokens
                .get(self.cursor + 1)
                .is_some_and(is_prepare_step_start)
            {
                return Err(self.error(
                    "INVALID_DSL",
                    &format!("prepare[{step_index}]"),
                    "a prepare step has a trailing or malformed field separator",
                    "separate fields with commas and start the next step with a supported op",
                ));
            }
            self.cursor += 1;
        }
        Ok(steps)
    }

    fn parse_prepare_step(&mut self, index: usize) -> Result<PrepareStepAst, RunDslError> {
        let Some(token) = self.tokens.get(self.cursor) else {
            return Err(self.error(
                "INVALID_DSL",
                &format!("prepare[{index}]"),
                "a prepare operation is missing",
                "add one of select/filter/rename/cast/trim/case/replace/fill/drop-missing/dedupe",
            ));
        };
        let position = token.start;
        let operation = token.value().unwrap_or_default().to_ascii_lowercase();
        self.cursor += 1;
        match operation.as_str() {
            "select" => Ok(PrepareStepAst::Select {
                fields: self.parse_field_list(&format!("prepare[{index}].fields"))?,
            }),
            "filter" => self.parse_filter(index),
            "rename" => self.parse_rename(index),
            "cast" => self.parse_cast(index),
            "trim" => Ok(PrepareStepAst::Trim {
                fields: self.parse_field_list(&format!("prepare[{index}].fields"))?,
            }),
            "case" => self.parse_case(index),
            "replace" => self.parse_replace(index),
            "fill" => self.parse_fill(index),
            "drop-missing" => {
                let mode = if self.consume_keyword("all") {
                    MissingModeAst::All
                } else {
                    MissingModeAst::Any
                };
                Ok(PrepareStepAst::DropMissing {
                    fields: self.parse_field_list(&format!("prepare[{index}].fields"))?,
                    mode,
                })
            }
            "dedupe" => self.parse_dedupe(index),
            _ => Err(RunDslError::new(
                "INVALID_DSL",
                format!("prepare[{index}]"),
                position,
                format!("unsupported prepare operation `{operation}`"),
                "use one of select/filter/rename/cast/trim/case/replace/fill/drop-missing/dedupe",
            )),
        }
    }

    fn parse_filter(&mut self, index: usize) -> Result<PrepareStepAst, RunDslError> {
        let (field, field_position) = self.take_value(
            &format!("prepare[{index}].field"),
            "use `filter <field> <eq|ne|lt|lte|gt|gte> <scalar>`",
        )?;
        reject_reserved_field(&field, &format!("prepare[{index}].field"), field_position)?;
        let location = format!("prepare[{index}].predicate");
        let (predicate, position) = self.take_value(&location, "use one of eq/ne/lt/lte/gt/gte")?;
        Ok(PrepareStepAst::Filter {
            field,
            predicate: parse_filter_predicate(&predicate, &location, position)?,
            value: self.take_scalar(&format!("prepare[{index}].value"))?,
        })
    }

    fn parse_rename(&mut self, index: usize) -> Result<PrepareStepAst, RunDslError> {
        let (from, from_position) = self.take_value(
            &format!("prepare[{index}].from"),
            "use `rename <field> to <field>`",
        )?;
        reject_reserved_field(&from, &format!("prepare[{index}].from"), from_position)?;
        self.expect_keyword(
            "to",
            &format!("prepare[{index}].to"),
            "use `rename <field> to <field>`",
        )?;
        let (to, to_position) = self.take_value(
            &format!("prepare[{index}].to"),
            "provide the new field name after `to`",
        )?;
        reject_reserved_field(&to, &format!("prepare[{index}].to"), to_position)?;
        Ok(PrepareStepAst::Rename { from, to })
    }

    fn parse_cast(&mut self, index: usize) -> Result<PrepareStepAst, RunDslError> {
        let (field, field_position) = self.take_value(
            &format!("prepare[{index}].field"),
            "use `cast <field> as <type>`",
        )?;
        reject_reserved_field(&field, &format!("prepare[{index}].field"), field_position)?;
        self.expect_keyword(
            "as",
            &format!("prepare[{index}].type"),
            "use `cast <field> as <type>`",
        )?;
        let location = format!("prepare[{index}].type");
        let (value_type, position) =
            self.take_value(&location, "use one of string/integer/number/boolean")?;
        Ok(PrepareStepAst::Cast {
            field,
            value_type: parse_cast_type(&value_type, &location, position)?,
        })
    }

    fn parse_case(&mut self, index: usize) -> Result<PrepareStepAst, RunDslError> {
        let fields =
            self.parse_field_list_until(&format!("prepare[{index}].fields"), &["lower", "upper"])?;
        let location = format!("prepare[{index}].mode");
        let (mode, position) = self.take_value(&location, "use upper/lower")?;
        Ok(PrepareStepAst::Case {
            fields,
            mode: parse_case_mode(&mode, &location, position)?,
        })
    }

    fn parse_replace(&mut self, index: usize) -> Result<PrepareStepAst, RunDslError> {
        let (field, field_position) = self.take_value(
            &format!("prepare[{index}].field"),
            "use `replace <field> <from> to <to>`",
        )?;
        reject_reserved_field(&field, &format!("prepare[{index}].field"), field_position)?;
        let from = self.take_scalar(&format!("prepare[{index}].from"))?;
        self.expect_keyword(
            "to",
            &format!("prepare[{index}].to"),
            "use `replace <field> <from> to <to>`",
        )?;
        let to = self.take_scalar(&format!("prepare[{index}].to"))?;
        Ok(PrepareStepAst::Replace { field, from, to })
    }

    fn parse_fill(&mut self, index: usize) -> Result<PrepareStepAst, RunDslError> {
        let (field, field_position) = self.take_value(
            &format!("prepare[{index}].field"),
            "use `fill <field> with <scalar>`",
        )?;
        reject_reserved_field(&field, &format!("prepare[{index}].field"), field_position)?;
        self.expect_keyword(
            "with",
            &format!("prepare[{index}].value"),
            "use `fill <field> with <scalar>`",
        )?;
        let value = self.take_scalar(&format!("prepare[{index}].value"))?;
        Ok(PrepareStepAst::FillMissing { field, value })
    }

    fn parse_dedupe(&mut self, index: usize) -> Result<PrepareStepAst, RunDslError> {
        self.expect_keyword(
            "by",
            &format!("prepare[{index}].by"),
            "use `dedupe by <fields> [keep first|error]`",
        )?;
        let by = self.parse_field_list_until(&format!("prepare[{index}].by"), &["keep"])?;
        let keep = if self.consume_keyword("keep") {
            let location = format!("prepare[{index}].keep");
            let (keep, position) = self.take_value(&location, "use first/error after `keep`")?;
            parse_dedupe_keep(&keep, &location, position)?
        } else {
            DedupeKeepAst::First
        };
        Ok(PrepareStepAst::Dedupe { by, keep })
    }

    fn parse_field_list(&mut self, location: &str) -> Result<Vec<String>, RunDslError> {
        self.parse_field_list_until(location, &[])
    }

    fn parse_field_list_until(
        &mut self,
        location: &str,
        stop_words: &[&str],
    ) -> Result<Vec<String>, RunDslError> {
        let (first, first_position) =
            self.take_value(location, "provide at least one field name")?;
        reject_reserved_field(&first, location, first_position)?;
        let mut fields = vec![first];
        loop {
            if self
                .tokens
                .get(self.cursor)
                .is_some_and(|token| stop_words.iter().any(|word| token.is_word(word)))
            {
                break;
            }
            if !matches!(self.peek_kind(), Some(TokenKind::Comma)) {
                break;
            }
            if self
                .tokens
                .get(self.cursor + 1)
                .is_some_and(is_prepare_step_start)
            {
                break;
            }
            self.cursor += 1;
            let (field, position) =
                self.take_value(location, "provide a field name after the comma")?;
            reject_reserved_field(&field, location, position)?;
            fields.push(field);
        }
        Ok(fields)
    }

    fn take_scalar(&mut self, location: &str) -> Result<Value, RunDslError> {
        let Some(token) = self.tokens.get(self.cursor).cloned() else {
            return Err(self.error(
                "INVALID_DSL",
                location,
                "a scalar value is missing",
                "use a quoted string, number, boolean, or null",
            ));
        };
        self.cursor += 1;
        match token.kind {
            TokenKind::Quoted(value) => Ok(Value::String(value)),
            TokenKind::Word(value) if value.eq_ignore_ascii_case("null") => Ok(Value::Null),
            TokenKind::Word(value) if value.eq_ignore_ascii_case("true") => Ok(Value::Bool(true)),
            TokenKind::Word(value) if value.eq_ignore_ascii_case("false") => Ok(Value::Bool(false)),
            TokenKind::Word(value) => parse_number(&value).map(Value::Number).ok_or_else(|| {
                RunDslError::new(
                    "INVALID_DSL",
                    location,
                    token.start,
                    format!("unquoted scalar `{value}` is not a number, boolean, or null"),
                    format!("quote a string as `'{value}'`"),
                )
            }),
            _ => Err(RunDslError::new(
                "INVALID_DSL",
                location,
                token.start,
                "the scalar has invalid punctuation",
                "use a quoted string, number, boolean, or null",
            )),
        }
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.tokens.get(self.cursor).map(|token| &token.kind)
    }

    fn take_percentage(&mut self, location: &str) -> Result<u16, RunDslError> {
        let (value, position) = self.take_value(location, "use a positive integer percentage")?;
        value.parse::<u16>().map_err(|_| {
            RunDslError::new(
                "INVALID_DSL",
                location,
                position,
                format!("split value `{value}` is not a positive integer percentage"),
                "use `train 80 val 10 test 10`",
            )
        })
    }

    fn take_value(
        &mut self,
        location: &str,
        remediation: &str,
    ) -> Result<(String, usize), RunDslError> {
        let Some(token) = self.tokens.get(self.cursor) else {
            return Err(self.error("INVALID_DSL", location, "a value is missing", remediation));
        };
        let Some(value) = token.value() else {
            return Err(RunDslError::new(
                "INVALID_DSL",
                location,
                token.start,
                "a value is missing",
                remediation,
            ));
        };
        let value = value.to_owned();
        let position = token.start;
        self.cursor += 1;
        Ok((value, position))
    }

    fn consume_keyword(&mut self, expected: &str) -> bool {
        if self
            .tokens
            .get(self.cursor)
            .is_some_and(|token| token.is_word(expected))
        {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn expect_keyword(
        &mut self,
        expected: &str,
        location: &str,
        remediation: &str,
    ) -> Result<(), RunDslError> {
        if self.consume_keyword(expected) {
            Ok(())
        } else {
            Err(self.error(
                "INVALID_DSL",
                location,
                format!("expected `{expected}`"),
                remediation,
            ))
        }
    }

    fn error(
        &self,
        code: &str,
        location: &str,
        message: impl Into<String>,
        remediation: impl Into<String>,
    ) -> RunDslError {
        RunDslError::new(code, location, self.position(), message, remediation)
    }

    fn position(&self) -> usize {
        self.tokens
            .get(self.cursor)
            .map_or(self.end_position, |token| token.start)
    }
}

fn is_prepare_step_start(token: &Token) -> bool {
    [
        "select",
        "filter",
        "rename",
        "cast",
        "trim",
        "case",
        "replace",
        "fill",
        "drop-missing",
        "dedupe",
    ]
    .iter()
    .any(|operation| token.is_word(operation))
}

fn reject_reserved_field(field: &str, location: &str, position: usize) -> Result<(), RunDslError> {
    if field == RESERVED_SOURCE_ID {
        return Err(RunDslError::new(
            "RESERVED_FIELD_CONFLICT",
            location,
            position,
            format!("`{RESERVED_SOURCE_ID}` is reserved for source-id-field auto"),
            "rename the input field or remove the explicit reserved-field reference",
        ));
    }
    Ok(())
}

fn preserve_generated_source_id(steps: &mut [PrepareStepAst]) {
    for step in steps {
        if let PrepareStepAst::Select { fields } = step {
            if !fields.iter().any(|field| field == RESERVED_SOURCE_ID) {
                fields.push(RESERVED_SOURCE_ID.into());
            }
        }
    }
}

fn parse_aggregate_function(
    value: &str,
    location: &str,
    position: usize,
) -> Result<AggregateFunctionAst, RunDslError> {
    match value.to_ascii_lowercase().as_str() {
        "count" => Ok(AggregateFunctionAst::Count),
        "sum" => Ok(AggregateFunctionAst::Sum),
        "avg" => Ok(AggregateFunctionAst::Avg),
        "min" => Ok(AggregateFunctionAst::Min),
        "max" => Ok(AggregateFunctionAst::Max),
        _ => Err(RunDslError::new(
            "INVALID_DSL",
            location,
            position,
            format!("unsupported aggregate function `{value}`"),
            "use one of count/sum/avg/min/max",
        )),
    }
}

fn parse_filter_predicate(
    value: &str,
    location: &str,
    position: usize,
) -> Result<FilterPredicateAst, RunDslError> {
    match value.to_ascii_lowercase().as_str() {
        "eq" => Ok(FilterPredicateAst::Eq),
        "ne" => Ok(FilterPredicateAst::Ne),
        "lt" => Ok(FilterPredicateAst::Lt),
        "lte" => Ok(FilterPredicateAst::Lte),
        "gt" => Ok(FilterPredicateAst::Gt),
        "gte" => Ok(FilterPredicateAst::Gte),
        _ => Err(RunDslError::new(
            "INVALID_DSL",
            location,
            position,
            format!("unsupported filter predicate `{value}`"),
            "use one of eq/ne/lt/lte/gt/gte",
        )),
    }
}

fn parse_cast_type(
    value: &str,
    location: &str,
    position: usize,
) -> Result<CastTypeAst, RunDslError> {
    match value.to_ascii_lowercase().as_str() {
        "string" => Ok(CastTypeAst::String),
        "integer" => Ok(CastTypeAst::Integer),
        "number" => Ok(CastTypeAst::Number),
        "boolean" => Ok(CastTypeAst::Boolean),
        _ => Err(RunDslError::new(
            "INVALID_DSL",
            location,
            position,
            format!("unsupported cast type `{value}`"),
            "use one of string/integer/number/boolean",
        )),
    }
}

fn parse_case_mode(
    value: &str,
    location: &str,
    position: usize,
) -> Result<CaseModeAst, RunDslError> {
    match value.to_ascii_lowercase().as_str() {
        "lower" => Ok(CaseModeAst::Lower),
        "upper" => Ok(CaseModeAst::Upper),
        _ => Err(RunDslError::new(
            "INVALID_DSL",
            location,
            position,
            format!("unsupported case mode `{value}`"),
            "use upper/lower",
        )),
    }
}

fn parse_dedupe_keep(
    value: &str,
    location: &str,
    position: usize,
) -> Result<DedupeKeepAst, RunDslError> {
    match value.to_ascii_lowercase().as_str() {
        "first" => Ok(DedupeKeepAst::First),
        "error" => Ok(DedupeKeepAst::Error),
        _ => Err(RunDslError::new(
            "INVALID_DSL",
            location,
            position,
            format!("unsupported dedupe keep mode `{value}`"),
            "use first/error",
        )),
    }
}

fn parse_number(value: &str) -> Option<Number> {
    if !value.contains(['.', 'e', 'E']) {
        if let Ok(number) = value.parse::<i64>() {
            return Some(Number::from(number));
        }
        if let Ok(number) = value.parse::<u64>() {
            return Some(Number::from(number));
        }
    }
    Number::from_f64(value.parse::<f64>().ok()?)
}

fn reject_known_unsupported(tokens: &[Token]) -> Result<(), RunDslError> {
    let unsupported = tokens.iter().find(|token| {
        token.value().is_some_and(|value| {
            value.starts_with("http://")
                || value.starts_with("https://")
                || ["join", "loop", "branch"].iter().any(|keyword| {
                    matches!(&token.kind, TokenKind::Word(_)) && value.eq_ignore_ascii_case(keyword)
                })
        })
    });
    if let Some(token) = unsupported {
        return Err(RunDslError::new(
            "UNSUPPORTED_TASK",
            "task",
            token.start,
            "the task uses a source or control-flow feature outside DataJig DSL v1",
            "use one local source and a linear from/prepare/transform/export task",
        ));
    }
    Ok(())
}

fn parse_format(value: &str, position: usize) -> Result<SourceFormat, RunDslError> {
    match value.to_ascii_lowercase().as_str() {
        "csv" => Ok(SourceFormat::Csv),
        "jsonl" => Ok(SourceFormat::Jsonl),
        "parquet" => Ok(SourceFormat::Parquet),
        _ => Err(RunDslError::new(
            "INVALID_DSL",
            "source.format",
            position,
            format!("unsupported source format `{value}`"),
            "use one of: csv, jsonl, parquet",
        )),
    }
}

fn infer_format(path: &str) -> Option<SourceFormat> {
    let extension = Path::new(path).extension()?.to_str()?;
    match extension.to_ascii_lowercase().as_str() {
        "csv" => Some(SourceFormat::Csv),
        "jsonl" => Some(SourceFormat::Jsonl),
        "parquet" => Some(SourceFormat::Parquet),
        _ => None,
    }
}

fn validate_path(path: &str, position: usize) -> Result<(), RunDslError> {
    let parsed = Path::new(path);
    if path.is_empty()
        || parsed.is_absolute()
        || parsed
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(RunDslError::new(
            "INVALID_DSL",
            "source.path",
            position,
            format!("source path `{path}` must be relative and cannot contain `..`"),
            "use a path below the current working directory",
        ));
    }
    Ok(())
}

fn validate_splits(train: u16, val: u16, test: u16, position: usize) -> Result<(), RunDslError> {
    if train == 0 || val == 0 || test == 0 || train + val + test != 100 {
        return Err(RunDslError::new(
            "INVALID_DSL",
            "export.splits",
            position,
            format!(
                "split percentages train={train}, val={val}, test={test} must be positive and total 100"
            ),
            "use `train 80 val 10 test 10` or another positive total of 100",
        ));
    }
    Ok(())
}

fn sql_identifier(field: &str) -> String {
    let mut characters = field.chars();
    let bare = characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric());
    if bare {
        field.to_owned()
    } else {
        format!("\"{}\"", field.replace('"', "\"\""))
    }
}
