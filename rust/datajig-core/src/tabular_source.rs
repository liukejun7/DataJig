use crate::prepare::{MAX_CELL_BYTES, MAX_OUTPUT_LINE_BYTES, PrepareInvalidDataError};
use crate::strict_json::reject_duplicate_json_members;
use anyhow::Result;
use chrono::{TimeZone, Utc};
use csv::ReaderBuilder;
use num_bigint::{BigInt, Sign};
use parquet::basic::{ConvertedType, LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::data_type::Decimal;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use parquet::schema::types::Type as ParquetType;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

const MAX_PARQUET_ROW_GROUP_BYTES: i64 = 256 * 1024 * 1024;
const MILLIS_PER_DAY: i64 = 86_400_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;

pub(crate) type TabularRow = BTreeMap<String, Value>;

pub(crate) trait TabularConsumer {
    fn headers(&mut self, headers: &[String]) -> Result<()>;
    fn row(&mut self, row: TabularRow) -> Result<()>;
}

pub(crate) fn stream_csv<C: TabularConsumer>(
    source: &Path,
    delimiter: u8,
    consumer: &mut C,
) -> Result<()> {
    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(false)
        .from_path(source)
        .map_err(|error| {
            PrepareInvalidDataError::new(format!("cannot read CSV source: {error}"))
        })?;
    let headers = reader
        .headers()
        .map_err(|error| PrepareInvalidDataError::new(format!("cannot read CSV header: {error}")))?
        .iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    consumer.headers(&headers)?;
    for (index, record) in reader.records().enumerate() {
        let source_row = index + 1;
        let record = record.map_err(|error| {
            PrepareInvalidDataError::new(format!("invalid CSV record at row {source_row}: {error}"))
        })?;
        let mut row = BTreeMap::new();
        for (header, value) in headers.iter().zip(record.iter()) {
            if value.len() > MAX_CELL_BYTES {
                return Err(PrepareInvalidDataError::new(format!(
                    "CSV row {source_row} cell exceeds {MAX_CELL_BYTES} bytes"
                ))
                .into());
            }
            row.insert(header.clone(), Value::String(value.into()));
        }
        consumer.row(row)?;
    }
    Ok(())
}

pub(crate) fn stream_parquet<C: TabularConsumer>(source: &Path, consumer: &mut C) -> Result<()> {
    let file = File::open(source)
        .map_err(|_| PrepareInvalidDataError::new("cannot open Parquet source"))?;
    let reader = SerializedFileReader::new(file)
        .map_err(|_| PrepareInvalidDataError::new("cannot read Parquet source"))?;
    if reader.metadata().row_groups().iter().any(|group| {
        group.total_byte_size() < 0 || group.total_byte_size() > MAX_PARQUET_ROW_GROUP_BYTES
    }) {
        return Err(PrepareInvalidDataError::new(format!(
            "Parquet row group exceeds the {MAX_PARQUET_ROW_GROUP_BYTES}-byte decode budget"
        ))
        .into());
    }
    let root = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema();
    let fields = root.get_fields();
    for field in fields {
        validate_parquet_field(field)?;
    }
    let headers = fields
        .iter()
        .map(|field| field.name().to_owned())
        .collect::<Vec<_>>();
    consumer.headers(&headers)?;
    let rows = reader
        .get_row_iter(None)
        .map_err(|_| PrepareInvalidDataError::new("cannot decode Parquet rows"))?;
    for (index, row) in rows.enumerate() {
        let source_row = index + 1;
        let row = row.map_err(|_| {
            PrepareInvalidDataError::new(format!("invalid Parquet row {source_row}"))
        })?;
        let mut values = BTreeMap::new();
        for (name, field) in row.get_column_iter() {
            let value = parquet_value(field, source_row)?;
            if values.insert(name.clone(), value).is_some() {
                return Err(
                    PrepareInvalidDataError::new("Parquet row contains duplicate fields").into(),
                );
            }
        }
        if values.len() != headers.len() || headers.iter().any(|name| !values.contains_key(name)) {
            return Err(PrepareInvalidDataError::new(format!(
                "Parquet row {source_row} does not match its schema"
            ))
            .into());
        }
        consumer.row(values)?;
    }
    Ok(())
}

pub(crate) fn stream_jsonl<C: TabularConsumer>(source: &Path, consumer: &mut C) -> Result<()> {
    let file =
        File::open(source).map_err(|_| PrepareInvalidDataError::new("cannot open JSONL source"))?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut expected_headers: Option<Vec<String>> = None;
    let mut line_number = 0_usize;
    while read_bounded_jsonl_line(&mut reader, &mut line)? {
        line_number += 1;
        if line.iter().all(u8::is_ascii_whitespace) {
            return Err(PrepareInvalidDataError::new(format!(
                "JSONL line {line_number} must not be blank"
            ))
            .into());
        }
        let text = std::str::from_utf8(&line).map_err(|_| {
            PrepareInvalidDataError::new(format!("JSONL line {line_number} is not valid UTF-8"))
        })?;
        reject_duplicate_json_members(text).map_err(|_| {
            PrepareInvalidDataError::new(format!(
                "JSONL line {line_number} contains duplicate object members"
            ))
        })?;
        let value: Value = serde_json::from_str(text).map_err(|_| {
            PrepareInvalidDataError::new(format!("JSONL line {line_number} is invalid JSON"))
        })?;
        let object = value.as_object().ok_or_else(|| {
            PrepareInvalidDataError::new(format!(
                "JSONL line {line_number} must be a top-level object"
            ))
        })?;
        let mut headers = object.keys().cloned().collect::<Vec<_>>();
        headers.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        match &expected_headers {
            Some(expected) if expected != &headers => {
                return Err(PrepareInvalidDataError::new(format!(
                    "JSONL line {line_number} does not match the file schema"
                ))
                .into());
            }
            None => {
                consumer.headers(&headers)?;
                expected_headers = Some(headers);
            }
            _ => {}
        }
        let row = object
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        consumer.row(row)?;
    }
    if expected_headers.is_none() {
        return Err(PrepareInvalidDataError::new("JSONL source has no records").into());
    }
    Ok(())
}

fn read_bounded_jsonl_line<R: BufRead>(reader: &mut R, line: &mut Vec<u8>) -> Result<bool> {
    line.clear();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(!line.is_empty());
        }
        let end = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        let payload_end = if available.get(end - 1) == Some(&b'\n') {
            end - 1
        } else {
            end
        };
        if line.len().saturating_add(payload_end) > MAX_OUTPUT_LINE_BYTES {
            return Err(PrepareInvalidDataError::new(format!(
                "JSONL line exceeds {MAX_OUTPUT_LINE_BYTES} bytes"
            ))
            .into());
        }
        line.extend_from_slice(&available[..payload_end]);
        let complete = payload_end < end;
        reader.consume(end);
        if complete {
            return Ok(true);
        }
    }
}

fn validate_parquet_field(field: &ParquetType) -> Result<()> {
    if !field.is_primitive() || field.get_basic_info().repetition() == Repetition::REPEATED {
        return Err(PrepareInvalidDataError::new(format!(
            "Parquet field {:?} uses an unsupported nested type",
            field.name()
        ))
        .into());
    }
    if matches!(
        field.get_basic_info().logical_type_ref(),
        Some(LogicalType::Time(value)) | Some(LogicalType::Timestamp(value))
            if value.unit == TimeUnit::NANOS
    ) {
        return Err(PrepareInvalidDataError::new(format!(
            "Parquet field {:?} uses unsupported nanosecond time precision",
            field.name()
        ))
        .into());
    }
    let converted = field.get_basic_info().converted_type();
    match field.get_physical_type() {
        PhysicalType::BYTE_ARRAY
            if !matches!(
                converted,
                ConvertedType::UTF8
                    | ConvertedType::ENUM
                    | ConvertedType::JSON
                    | ConvertedType::DECIMAL
            ) =>
        {
            Err(PrepareInvalidDataError::new(format!(
                "Parquet field {:?} uses unsupported binary data",
                field.name()
            ))
            .into())
        }
        PhysicalType::FIXED_LEN_BYTE_ARRAY
            if converted != ConvertedType::DECIMAL
                && field.get_basic_info().logical_type_ref() != Some(&LogicalType::Float16) =>
        {
            Err(PrepareInvalidDataError::new(format!(
                "Parquet field {:?} uses unsupported binary data",
                field.name()
            ))
            .into())
        }
        _ => Ok(()),
    }
}

fn parquet_value(field: &Field, source_row: usize) -> Result<Value> {
    let value = match field {
        Field::Null => Value::Null,
        Field::Bool(value) => Value::Bool(*value),
        Field::Byte(value) => Value::Number((*value).into()),
        Field::Short(value) => Value::Number((*value).into()),
        Field::Int(value) => Value::Number((*value).into()),
        Field::Long(value) => Value::Number((*value).into()),
        Field::UByte(value) => Value::Number((*value).into()),
        Field::UShort(value) => Value::Number((*value).into()),
        Field::UInt(value) => Value::Number((*value).into()),
        Field::ULong(value) => Value::Number((*value).into()),
        Field::Float16(value) if value.is_finite() => {
            number_or_invalid(f64::from(*value), source_row)?
        }
        Field::Float(value) if value.is_finite() => {
            number_or_invalid(f64::from(*value), source_row)?
        }
        Field::Double(value) if value.is_finite() => number_or_invalid(*value, source_row)?,
        Field::Float16(_) | Field::Float(_) | Field::Double(_) => {
            return Err(non_finite_parquet(source_row).into());
        }
        Field::Decimal(value) => Value::String(decimal_string(value, source_row)?),
        Field::Str(value) => Value::String(value.clone()),
        Field::Date(value) => Value::String(
            Utc.timestamp_opt(i64::from(*value) * 86_400, 0)
                .single()
                .ok_or_else(|| invalid_logical_value(source_row))?
                .format("%Y-%m-%d")
                .to_string(),
        ),
        Field::TimeMillis(value) if (0..MILLIS_PER_DAY as i32).contains(value) => {
            Value::String(format_time(i64::from(*value), 1_000, 3))
        }
        Field::TimeMicros(value) if (0..MICROS_PER_DAY).contains(value) => {
            Value::String(format_time(*value, 1_000_000, 6))
        }
        Field::TimestampMillis(value) => Value::String(
            Utc.timestamp_millis_opt(*value)
                .single()
                .ok_or_else(|| invalid_logical_value(source_row))?
                .format("%Y-%m-%d %H:%M:%S%.3f %:z")
                .to_string(),
        ),
        Field::TimestampMicros(value) => Value::String(
            Utc.timestamp_micros(*value)
                .single()
                .ok_or_else(|| invalid_logical_value(source_row))?
                .format("%Y-%m-%d %H:%M:%S%.6f %:z")
                .to_string(),
        ),
        Field::TimeMillis(_) | Field::TimeMicros(_) => {
            return Err(invalid_logical_value(source_row).into());
        }
        Field::Bytes(_) | Field::Group(_) | Field::ListInternal(_) | Field::MapInternal(_) => {
            return Err(PrepareInvalidDataError::new(format!(
                "Parquet row {source_row} contains an unsupported value type"
            ))
            .into());
        }
    };
    if matches!(&value, Value::String(text) if text.len() > MAX_CELL_BYTES) {
        return Err(PrepareInvalidDataError::new(format!(
            "Parquet row {source_row} cell exceeds {MAX_CELL_BYTES} bytes"
        ))
        .into());
    }
    Ok(value)
}

fn number_or_invalid(value: f64, source_row: usize) -> Result<Value> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| non_finite_parquet(source_row).into())
}

fn decimal_string(decimal: &Decimal, source_row: usize) -> Result<String> {
    let scale = usize::try_from(decimal.scale()).map_err(|_| invalid_logical_value(source_row))?;
    let number = BigInt::from_signed_bytes_be(decimal.data());
    let negative = number.sign() == Sign::Minus;
    let mut digits = number.magnitude().to_str_radix(10);
    if scale > 0 {
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        digits.insert(digits.len() - scale, '.');
    }
    if negative {
        digits.insert(0, '-');
    }
    if digits.len() > MAX_CELL_BYTES {
        return Err(PrepareInvalidDataError::new(format!(
            "Parquet row {source_row} cell exceeds {MAX_CELL_BYTES} bytes"
        ))
        .into());
    }
    Ok(digits)
}

fn format_time(value: i64, units_per_second: i64, precision: usize) -> String {
    let hours = value / (3_600 * units_per_second);
    let minutes = value % (3_600 * units_per_second) / (60 * units_per_second);
    let seconds = value % (60 * units_per_second) / units_per_second;
    let fraction = value % units_per_second;
    format!("{hours:02}:{minutes:02}:{seconds:02}.{fraction:0precision$}")
}

fn invalid_logical_value(source_row: usize) -> PrepareInvalidDataError {
    PrepareInvalidDataError::new(format!(
        "Parquet row {source_row} contains an invalid logical value"
    ))
}

fn non_finite_parquet(source_row: usize) -> PrepareInvalidDataError {
    PrepareInvalidDataError::new(format!(
        "Parquet row {source_row} contains a non-finite floating-point value"
    ))
}
