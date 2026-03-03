use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaRef};

fn map_utf8_utf8() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        )),
        false,
    )
}

pub fn traces_schema() -> SchemaRef {
    Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("kind", DataType::Int32, false),
        Field::new("start_time", DataType::UInt64, false),
        Field::new("end_time", DataType::UInt64, false),
        Field::new("duration_ns", DataType::UInt64, false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("status_message", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("attributes", map_utf8_utf8(), false),
        Field::new("resource", map_utf8_utf8(), false),
    ]))
}

pub fn logs_schema() -> SchemaRef {
    Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("severity", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("attributes", map_utf8_utf8(), false),
        Field::new("resource", map_utf8_utf8(), false),
    ]))
}

pub fn metrics_schema() -> SchemaRef {
    Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_type", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new("count", DataType::UInt64, true),
        Field::new("sum", DataType::Float64, true),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("attributes", map_utf8_utf8(), false),
        Field::new("resource", map_utf8_utf8(), false),
    ]))
}
