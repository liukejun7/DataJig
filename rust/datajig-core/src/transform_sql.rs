use crate::TransformLimits;
use crate::identity::blake3_content_id;
use anyhow::Result;
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    DataType, Expr, Function, FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator,
    LimitClause, ObjectName, OrderByKind, Query, Select, SelectFlavor, SelectItem, SetExpr,
    SetQuantifier, Statement, TableFactor, Value as SqlValue, ValueWithSpan, Visit, Visitor,
    WildcardAdditionalOptions,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::ops::ControlFlow;

#[derive(Debug)]
pub struct TransformSqlParseError(String);

impl fmt::Display for TransformSqlParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transform SQL could not be parsed: {}", self.0)
    }
}

impl Error for TransformSqlParseError {}

#[derive(Debug)]
pub struct TransformSqlPolicyError(String);

impl fmt::Display for TransformSqlPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transform SQL policy denied: {}", self.0)
    }
}

impl Error for TransformSqlPolicyError {}

#[derive(Debug)]
pub struct TransformOrderError(String);

impl fmt::Display for TransformOrderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for TransformOrderError {}

#[derive(Clone, Debug)]
pub struct TransformOrderContract {
    id_field: String,
    has_total_order: bool,
}

impl TransformOrderContract {
    pub fn has_total_order(&self) -> bool {
        self.has_total_order
    }
}

#[derive(Clone, Debug)]
pub struct ValidatedTransformQuery {
    pub statement: Statement,
    sql_content_id: String,
    parameter_content_id: String,
    parameter_count: usize,
    output_order_contract: TransformOrderContract,
}

impl ValidatedTransformQuery {
    pub fn sql_content_id(&self) -> &str {
        &self.sql_content_id
    }

    pub fn parameter_content_id(&self) -> &str {
        &self.parameter_content_id
    }

    pub fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    pub fn output_order_contract(&self) -> &TransformOrderContract {
        &self.output_order_contract
    }

    pub fn validate_output_rows(&self, rows: u64) -> Result<()> {
        if rows > 1 && !self.output_order_contract.has_total_order {
            return Err(TransformOrderError(format!(
                "multi-row transform output requires top-level ORDER BY ending in {}",
                self.output_order_contract.id_field
            ))
            .into());
        }
        Ok(())
    }
}

pub fn validate_transform_query(
    sql: &str,
    parameters: &[JsonValue],
    aliases: &BTreeSet<String>,
    id_field: &str,
    limits: &TransformLimits,
) -> Result<ValidatedTransformQuery> {
    if sql.is_empty() || sql.len() > limits.sql_bytes {
        return Err(TransformSqlPolicyError(format!(
            "SQL must contain 1..={} UTF-8 bytes",
            limits.sql_bytes
        ))
        .into());
    }
    if parameters.len() > limits.parameters {
        return Err(TransformSqlPolicyError(format!(
            "parameters exceed {} values",
            limits.parameters
        ))
        .into());
    }
    if parameters
        .iter()
        .any(|value| matches!(value, JsonValue::Array(_) | JsonValue::Object(_)))
    {
        return Err(TransformSqlPolicyError("parameters must be JSON scalars".into()).into());
    }
    let parameter_payload = serde_json::to_vec(parameters)?;
    if parameter_payload.len() > limits.parameter_bytes {
        return Err(TransformSqlPolicyError(format!(
            "parameter JSON exceeds {} bytes",
            limits.parameter_bytes
        ))
        .into());
    }
    if aliases.is_empty() || aliases.iter().any(|alias| !valid_alias(alias)) {
        return Err(TransformSqlPolicyError("declared input aliases are invalid".into()).into());
    }
    if !valid_field(id_field) {
        return Err(TransformSqlPolicyError("output ID field is invalid".into()).into());
    }
    validate_nesting_bound(sql)?;

    let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql)
        .map_err(|error| TransformSqlParseError(error.to_string()))?;
    if statements.len() != 1 {
        return Err(
            TransformSqlPolicyError("exactly one SELECT statement is required".into()).into(),
        );
    }
    let statement = statements.remove(0);
    let query = match &statement {
        Statement::Query(query) => query,
        other => {
            return Err(TransformSqlPolicyError(format!(
                "only SELECT is allowed, found {}",
                statement_kind(other)
            ))
            .into());
        }
    };
    validate_set_expr(&query.body)?;
    let order_contract = validate_top_level_order(query, id_field)?;

    let mut visitor = PolicyVisitor {
        aliases,
        placeholders: 0,
    };
    if let ControlFlow::Break(message) = statement.visit(&mut visitor) {
        return Err(TransformSqlPolicyError(message).into());
    }
    if visitor.placeholders != parameters.len() {
        return Err(TransformSqlPolicyError(format!(
            "query has {} positional placeholders but {} parameters were supplied",
            visitor.placeholders,
            parameters.len()
        ))
        .into());
    }

    Ok(ValidatedTransformQuery {
        statement,
        sql_content_id: blake3_content_id("sql", b"datajig-transform-sql-v1\0", sql.as_bytes()),
        parameter_content_id: blake3_content_id(
            "params",
            b"datajig-transform-parameters-v1\0",
            &parameter_payload,
        ),
        parameter_count: parameters.len(),
        output_order_contract: order_contract,
    })
}

struct PolicyVisitor<'a> {
    aliases: &'a BTreeSet<String>,
    placeholders: usize,
}

impl Visitor for PolicyVisitor<'_> {
    type Break = String;

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        if matches!(statement, Statement::Query(_)) {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(format!(
                "nested {} statement is not allowed",
                statement_kind(statement)
            ))
        }
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if query.with.is_some()
            || query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return ControlFlow::Break("unsupported SELECT clause".into());
        }
        if let Some(limit_clause) = &query.limit_clause {
            match limit_clause {
                LimitClause::LimitOffset { limit_by, .. } if limit_by.is_empty() => {}
                _ => return ControlFlow::Break("unsupported LIMIT clause".into()),
            }
        }
        if let Some(order_by) = &query.order_by {
            if order_by.interpolate.is_some()
                || !matches!(order_by.kind, OrderByKind::Expressions(_))
            {
                return ControlFlow::Break("unsupported ORDER BY clause".into());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        if !select.optimizer_hints.is_empty()
            || select
                .select_modifiers
                .as_ref()
                .is_some_and(|modifiers| modifiers.is_any_set())
            || select.top.is_some()
            || select.top_before_distinct
            || select.exclude.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || select.window_before_qualify
            || select.value_table_mode.is_some()
            || select.flavor != SelectFlavor::Standard
        {
            return ControlFlow::Break("unsupported SELECT extension".into());
        }
        if !matches!(&select.group_by, GroupByExpr::Expressions(_, modifiers) if modifiers.is_empty())
        {
            return ControlFlow::Break("unsupported GROUP BY clause".into());
        }
        for projection in &select.projection {
            match projection {
                SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => {}
                SelectItem::Wildcard(options) if default_wildcard(options) => {}
                SelectItem::QualifiedWildcard(kind, options) if default_wildcard(options) => {
                    let text = kind.to_string();
                    let alias = text.strip_suffix(".*").unwrap_or(&text);
                    if !self.aliases.contains(&alias.to_ascii_lowercase()) {
                        return ControlFlow::Break(
                            "qualified wildcard must name a declared input alias".into(),
                        );
                    }
                }
                _ => return ControlFlow::Break("unsupported projection item".into()),
            }
        }
        for table in &select.from {
            if let Err(message) = validate_table_factor(&table.relation, self.aliases) {
                return ControlFlow::Break(message);
            }
            for join in &table.joins {
                if join.global {
                    return ControlFlow::Break("GLOBAL JOIN is not allowed".into());
                }
                if let Err(message) = validate_table_factor(&join.relation, self.aliases) {
                    return ControlFlow::Break(message);
                }
                if let Err(message) = validate_join(&join.join_operator) {
                    return ControlFlow::Break(message);
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        match validate_table_factor(factor, self.aliases) {
            Ok(()) => ControlFlow::Continue(()),
            Err(message) => ControlFlow::Break(message),
        }
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        let allowed = match expression {
            Expr::Identifier(identifier) => !volatile_identifier(identifier),
            Expr::CompoundIdentifier(parts) => {
                parts.len() == 2 && parts.iter().all(valid_identifier)
            }
            Expr::IsFalse(value)
            | Expr::IsNotFalse(value)
            | Expr::IsTrue(value)
            | Expr::IsNotTrue(value)
            | Expr::IsNull(value)
            | Expr::IsNotNull(value)
            | Expr::UnaryOp { expr: value, .. }
            | Expr::Nested(value) => !matches!(value.as_ref(), Expr::Wildcard(_)),
            Expr::IsDistinctFrom(_, _)
            | Expr::IsNotDistinctFrom(_, _)
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::Between { .. }
            | Expr::BinaryOp { .. }
            | Expr::Case { .. }
            | Expr::Exists { .. }
            | Expr::Subquery(_)
            | Expr::Value(_)
            | Expr::Wildcard(_) => true,
            Expr::Cast {
                data_type, format, ..
            } => format.is_none() && supported_cast(data_type),
            Expr::Function(function) => allowed_function(function),
            Expr::Ceil { .. } | Expr::Floor { .. } | Expr::Trim { .. } => true,
            _ => false,
        };
        if allowed {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(format!("expression is not allowed: {expression}"))
        }
    }

    fn pre_visit_value(&mut self, value: &ValueWithSpan) -> ControlFlow<Self::Break> {
        match &value.value {
            SqlValue::Number(_, _)
            | SqlValue::SingleQuotedString(_)
            | SqlValue::Boolean(_)
            | SqlValue::Null => ControlFlow::Continue(()),
            SqlValue::Placeholder(value) if value == "?" => {
                self.placeholders += 1;
                ControlFlow::Continue(())
            }
            SqlValue::Placeholder(_) => {
                ControlFlow::Break("only positional ? parameters are allowed".into())
            }
            _ => ControlFlow::Break("literal form is not allowed".into()),
        }
    }
}

fn validate_set_expr(expression: &SetExpr) -> Result<()> {
    match expression {
        SetExpr::Select(_) => Ok(()),
        SetExpr::Query(query) => validate_set_expr(&query.body),
        SetExpr::SetOperation {
            left,
            op: _,
            set_quantifier,
            right,
        } => {
            if matches!(
                set_quantifier,
                SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName
            ) {
                return Err(TransformSqlPolicyError(
                    "set operations BY NAME are not allowed".into(),
                )
                .into());
            }
            validate_set_expr(left)?;
            validate_set_expr(right)
        }
        _ => Err(TransformSqlPolicyError("query body must be SELECT".into()).into()),
    }
}

fn validate_top_level_order(query: &Query, id_field: &str) -> Result<TransformOrderContract> {
    let Some(order_by) = &query.order_by else {
        return Ok(TransformOrderContract {
            id_field: id_field.into(),
            has_total_order: false,
        });
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(TransformOrderError("ORDER BY ALL is not allowed".into()).into());
    };
    let Some(final_expression) = expressions.last() else {
        return Err(TransformOrderError("ORDER BY must contain the output ID field".into()).into());
    };
    let is_id = matches!(
        &final_expression.expr,
        Expr::Identifier(identifier) if normalized_identifier(identifier).as_deref() == Some(id_field)
    );
    if !is_id || final_expression.with_fill.is_some() {
        return Err(TransformOrderError(format!(
            "final top-level ORDER BY expression must be the output ID alias {id_field}"
        ))
        .into());
    }
    Ok(TransformOrderContract {
        id_field: id_field.into(),
        has_total_order: true,
    })
}

fn validate_table_factor(
    factor: &TableFactor,
    aliases: &BTreeSet<String>,
) -> std::result::Result<(), String> {
    match factor {
        TableFactor::Table {
            name,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } if args.is_none()
            && with_hints.is_empty()
            && version.is_none()
            && !with_ordinality
            && partitions.is_empty()
            && json_path.is_none()
            && sample.is_none()
            && index_hints.is_empty() =>
        {
            let relation = single_name(name)
                .ok_or_else(|| "relations must be single-part declared aliases".to_owned())?;
            if aliases.contains(&relation) {
                Ok(())
            } else {
                Err(format!("relation {relation} is not a declared input alias"))
            }
        }
        TableFactor::Derived {
            lateral,
            alias,
            sample,
            ..
        } if !lateral && alias.is_some() && sample.is_none() => Ok(()),
        _ => Err("table functions and extended relation sources are not allowed".into()),
    }
}

fn validate_join(operator: &JoinOperator) -> std::result::Result<(), String> {
    let constraint = match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint) => constraint,
        _ => return Err("join operator is not allowed".into()),
    };
    match constraint {
        JoinConstraint::On(_) | JoinConstraint::None => Ok(()),
        JoinConstraint::Using(names)
            if names
                .iter()
                .all(|name| single_name(name).is_some_and(|value| valid_field(&value))) =>
        {
            Ok(())
        }
        _ => Err("join constraint is not allowed".into()),
    }
}

fn allowed_function(function: &Function) -> bool {
    const FUNCTIONS: &[&str] = &[
        "abs",
        "avg",
        "ceil",
        "ceiling",
        "char_length",
        "coalesce",
        "concat",
        "concat_ws",
        "contains",
        "count",
        "ends_with",
        "floor",
        "greatest",
        "ifnull",
        "least",
        "length",
        "lower",
        "ltrim",
        "max",
        "min",
        "nullif",
        "round",
        "rtrim",
        "starts_with",
        "sum",
        "trim",
        "upper",
    ];
    let Some(name) = single_name(&function.name) else {
        return false;
    };
    FUNCTIONS.binary_search(&name.as_str()).is_ok()
        && !function.uses_odbc_syntax
        && matches!(function.parameters, FunctionArguments::None)
        && matches!(&function.args, FunctionArguments::List(arguments) if arguments.clauses.is_empty())
        && function.within_group.is_empty()
        && function.filter.is_none()
        && function.null_treatment.is_none()
        && function.over.is_none()
}

fn supported_cast(data_type: &DataType) -> bool {
    matches!(
        data_type.to_string().to_ascii_uppercase().as_str(),
        "BOOLEAN"
            | "BOOL"
            | "TINYINT"
            | "SMALLINT"
            | "INTEGER"
            | "INT"
            | "BIGINT"
            | "UTINYINT"
            | "USMALLINT"
            | "UINTEGER"
            | "UBIGINT"
            | "REAL"
            | "FLOAT"
            | "DOUBLE"
            | "TEXT"
            | "STRING"
            | "VARCHAR"
    )
}

fn default_wildcard(options: &WildcardAdditionalOptions) -> bool {
    options == &WildcardAdditionalOptions::default()
}

fn single_name(name: &ObjectName) -> Option<String> {
    if name.0.len() != 1 {
        return None;
    }
    normalized_identifier(name.0[0].as_ident()?)
}

fn normalized_identifier(identifier: &Ident) -> Option<String> {
    if !valid_identifier(identifier) {
        return None;
    }
    Some(identifier.value.to_ascii_lowercase())
}

fn valid_identifier(identifier: &Ident) -> bool {
    matches!(identifier.quote_style, None | Some('"'))
        && !identifier.value.is_empty()
        && identifier.value.is_ascii()
}

fn volatile_identifier(identifier: &Ident) -> bool {
    normalized_identifier(identifier).is_none_or(|value| {
        matches!(
            value.as_str(),
            "current_date" | "current_time" | "current_timestamp" | "localtime" | "localtimestamp"
        )
    })
}

fn valid_alias(value: &str) -> bool {
    let mut characters = value.chars();
    matches!(characters.next(), Some(first) if first.is_ascii_lowercase())
        && value.len() <= 64
        && characters.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

fn valid_field(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && value.is_ascii()
}

fn validate_nesting_bound(sql: &str) -> Result<()> {
    const MAX_NESTING: usize = 128;
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        SingleQuoted,
        DoubleQuoted,
        LineComment,
        BlockComment(usize),
    }

    let bytes = sql.as_bytes();
    let mut state = State::Normal;
    let mut nesting = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        let current = bytes[index];
        let next = bytes.get(index + 1).copied();
        match state {
            State::Normal => match (current, next) {
                (b'\'', _) => state = State::SingleQuoted,
                (b'"', _) => state = State::DoubleQuoted,
                (b'-', Some(b'-')) => {
                    state = State::LineComment;
                    index += 1;
                }
                (b'/', Some(b'*')) => {
                    state = State::BlockComment(1);
                    index += 1;
                }
                (b'(', _) => {
                    nesting += 1;
                    if nesting > MAX_NESTING {
                        return Err(TransformSqlPolicyError(format!(
                            "SQL nesting exceeds {MAX_NESTING} levels"
                        ))
                        .into());
                    }
                }
                (b')', _) => nesting = nesting.saturating_sub(1),
                _ => {}
            },
            State::SingleQuoted => {
                if current == b'\'' {
                    if next == Some(b'\'') {
                        index += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::DoubleQuoted => {
                if current == b'"' {
                    if next == Some(b'"') {
                        index += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::LineComment => {
                if current == b'\n' || current == b'\r' {
                    state = State::Normal;
                }
            }
            State::BlockComment(depth) => match (current, next) {
                (b'/', Some(b'*')) => {
                    state = State::BlockComment(depth + 1);
                    index += 1;
                }
                (b'*', Some(b'/')) => {
                    state = if depth == 1 {
                        State::Normal
                    } else {
                        State::BlockComment(depth - 1)
                    };
                    index += 1;
                }
                _ => {}
            },
        }
        index += 1;
    }
    Ok(())
}

fn statement_kind(statement: &Statement) -> &'static str {
    match statement {
        Statement::Query(_) => "query",
        Statement::Insert(_) => "insert",
        Statement::Copy { .. } => "copy",
        Statement::Install { .. } => "install",
        Statement::Load { .. } => "load",
        Statement::Set(_) => "set",
        Statement::CreateTable(_) => "create table",
        _ => "non-query",
    }
}
