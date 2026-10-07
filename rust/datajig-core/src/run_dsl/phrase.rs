use super::{CompiledRunTask, RunDslError, compile_dsl};

const RESERVED_ID: &str = "_datajig_source_id";

pub trait TaskTranslator {
    fn translate(&self, input: &str) -> Result<String, RunDslError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DeterministicPhraseTranslator;

impl TaskTranslator for DeterministicPhraseTranslator {
    fn translate(&self, input: &str) -> Result<String, RunDslError> {
        translate_phrase(input)
    }
}

pub fn compile_task(input: &str) -> Result<CompiledRunTask, RunDslError> {
    let trimmed = input.trim();
    if trimmed
        .split_whitespace()
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("from"))
    {
        return compile_with_phrase_guidance(trimmed);
    }
    compile_task_with_translator(trimmed, &DeterministicPhraseTranslator)
}

pub fn compile_task_with_translator(
    input: &str,
    translator: &dyn TaskTranslator,
) -> Result<CompiledRunTask, RunDslError> {
    let dsl = translator.translate(input)?;
    compile_with_phrase_guidance(&dsl)
}

fn compile_with_phrase_guidance(dsl: &str) -> Result<CompiledRunTask, RunDslError> {
    compile_dsl(dsl).map_err(|error| {
        if error.code == "UNSUPPORTED_TASK" && error.suggestions.is_empty() {
            with_default_suggestions(error)
        } else {
            error
        }
    })
}

fn translate_phrase(input: &str) -> Result<String, RunDslError> {
    let input = input.trim();
    if let Some(path) = input.strip_prefix("整理 ") {
        return non_aggregate(path, "");
    }
    if let Some(body) = input.strip_prefix("把 ") {
        if let Some(path) = body.strip_suffix(" 做成训练集") {
            return non_aggregate(path, "");
        }
        if let Some((path, rest)) = body.split_once(" 填充 ") {
            if let Some((field, value)) = rest.split_once(" 空值为 ") {
                return non_aggregate(
                    path,
                    &format!(
                        " prepare fill {} with {}",
                        quote(field),
                        phrase_scalar(value)
                    ),
                );
            }
            return Err(unsupported_phrase(
                input,
                Some("把 <路径> 填充 <字段> 空值为 <值>"),
            ));
        }
        if let Some((path, rest)) = body.split_once(" 删掉 ") {
            if let Some(field) = rest.strip_suffix(" 为空的行") {
                return non_aggregate(path, &format!(" prepare drop-missing {}", quote(field)));
            }
        }
        if let Some((path, fields)) = body.split_once(" 只保留 ") {
            let fields = phrase_field_list(fields)?;
            return non_aggregate(path, &format!(" prepare select {fields}"));
        }
        if let Some((path, rest)) = body.split_once(" 过滤 ") {
            for (label, predicate) in [("大于", "gt"), ("小于", "lt"), ("等于", "eq")] {
                let marker = format!(" {label} ");
                if let Some((field, value)) = rest.split_once(&marker) {
                    return non_aggregate(
                        path,
                        &format!(
                            " prepare filter {} {predicate} {}",
                            quote(field),
                            phrase_scalar(value)
                        ),
                    );
                }
            }
        }
        if let Some(path) = body.strip_suffix(" 切成 8:1:1") {
            return Ok(format!(
                "{} export train 80 val 10 test 10 id-field {RESERVED_ID}",
                source(path)?
            ));
        }
        if let Some((path, rest)) = body.split_once(" 的 ") {
            if let Some((field, new_name)) = rest.split_once(" 改名为 ") {
                return non_aggregate(
                    path,
                    &format!(" prepare rename {} to {}", quote(field), quote(new_name)),
                );
            }
            if let Some((field, target)) = rest.split_once(" 转成") {
                let target = match target.trim() {
                    "数字" => "number",
                    "整数" => "integer",
                    "文本" => "string",
                    "布尔" => "boolean",
                    _ => return Err(unsupported_phrase(input, None)),
                };
                return non_aggregate(path, &format!(" prepare cast {} as {target}", quote(field)));
            }
        }
        if let Some((path, rest)) = body.split_once(" 按 ") {
            if let Some(field) = rest.strip_suffix(" 去重") {
                return non_aggregate(path, &format!(" prepare dedupe by {}", quote(field)));
            }
            if let Some(field) = rest.strip_suffix(" 聚合") {
                return aggregate(path, field, "record_count", "count", field);
            }
        }
    }
    if let Some(body) = input.strip_prefix("统计 ") {
        if let Some((path, rest)) = body.split_once(" 每个 ") {
            if let Some(group) = rest.strip_suffix(" 的数量") {
                return aggregate(path, group, "record_count", "count", group);
            }
            if let Some((group, metric_operation)) = rest.split_once(" 的 ") {
                for (label, function) in [
                    ("总和", "sum"),
                    ("均值", "avg"),
                    ("最大", "max"),
                    ("最小", "min"),
                ] {
                    if let Some(metric) = metric_operation.strip_suffix(&format!(" {label}")) {
                        let alias = format!("{}_{}", metric.trim(), function);
                        return aggregate(path, group, &alias, function, metric);
                    }
                }
            }
        }
    }
    Err(unsupported_phrase(input, None))
}

fn source(path: &str) -> Result<String, RunDslError> {
    let path = path.trim();
    if path.is_empty() {
        return Err(unsupported_phrase("", Some("整理 <路径>")));
    }
    Ok(format!("from {} source-id-field auto", quote(path)))
}

fn non_aggregate(path: &str, middle: &str) -> Result<String, RunDslError> {
    Ok(format!(
        "{}{middle} export id-field {RESERVED_ID}",
        source(path)?
    ))
}

fn aggregate(
    path: &str,
    group: &str,
    alias: &str,
    function: &str,
    metric: &str,
) -> Result<String, RunDslError> {
    Ok(format!(
        "{} aggregate by {} {}={function}({}) export",
        source(path)?,
        quote(group.trim()),
        quote(alias.trim()),
        quote(metric.trim())
    ))
}

fn phrase_field_list(fields: &str) -> Result<String, RunDslError> {
    let fields: Vec<_> = fields
        .split([',', '，'])
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(quote)
        .collect();
    if fields.is_empty() {
        return Err(unsupported_phrase("", Some("把 <路径> 只保留 <字段...>")));
    }
    Ok(fields.join(","))
}

fn phrase_scalar(value: &str) -> String {
    let value = value.trim();
    if value.eq_ignore_ascii_case("null")
        || value.eq_ignore_ascii_case("true")
        || value.eq_ignore_ascii_case("false")
        || value.parse::<f64>().is_ok()
    {
        value.to_ascii_lowercase()
    } else {
        quote(value)
    }
}

fn quote(value: &str) -> String {
    format!(
        "'{}'",
        value.trim().replace('\\', "\\\\").replace('\'', "\\'")
    )
}

fn unsupported_phrase(input: &str, preferred: Option<&str>) -> RunDslError {
    let mut suggestions = vec![
        "整理 <路径>".to_owned(),
        "把 <路径> 填充 <字段> 空值为 <值>".to_owned(),
        "统计 <路径> 每个 <字段> 的数量".to_owned(),
    ];
    if let Some(preferred) = preferred {
        suggestions.retain(|suggestion| suggestion != preferred);
        suggestions.insert(0, preferred.to_owned());
    }
    let remediation = format!("try a supported template such as `{}`", suggestions[0]);
    RunDslError::new(
        "UNSUPPORTED_TASK",
        "task",
        0,
        format!("no deterministic task template matched `{input}`"),
        remediation,
    )
    .with_suggestions(suggestions)
}

fn with_default_suggestions(error: RunDslError) -> RunDslError {
    error.with_suggestions(vec![
        "整理 <路径>".into(),
        "把 <路径> 做成训练集".into(),
        "统计 <路径> 每个 <字段> 的数量".into(),
    ])
}
