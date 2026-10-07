use super::RunDslError;
use super::ast::{
    CompiledRunTask, RUN_DSL_SCHEMA_VERSION, RunExportAst, RunPrepareAst, RunSourceAst, RunTaskAst,
    RunTransformAst, SourceFormat, SourceIdAst, SourceIdMode, TrainingSplitsAst,
    canonicalize_run_task,
};
use super::lexer::{Token, TokenKind, lex};
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
        let (source_id, _) = self.take_value(
            "source.source-id-field",
            "provide a field name or `auto` after `source-id-field`",
        )?;
        let source_id_field = if source_id.eq_ignore_ascii_case("auto") {
            SourceIdAst {
                mode: SourceIdMode::Auto,
                field: RESERVED_SOURCE_ID.into(),
            }
        } else {
            SourceIdAst {
                mode: SourceIdMode::Field,
                field: source_id,
            }
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

        if !self.consume_keyword("id-field") {
            return Err(self.error(
                "INVALID_DSL",
                "export.id-field",
                "the final record identity is required for a non-aggregate task",
                "add `id-field <field>` after the export split settings",
            ));
        }
        let (id_field, _) = self.take_value(
            "export.id-field",
            "provide the final record field after `id-field`",
        )?;
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
                steps: Vec::new(),
            },
            transform: RunTransformAst::Sql {
                sql: format!(
                    "SELECT * FROM source ORDER BY {}",
                    sql_identifier(&id_field)
                ),
                generated: true,
            },
            export: RunExportAst { splits, id_field },
        };
        canonicalize_run_task(ast)
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
