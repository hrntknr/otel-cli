use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Float64Builder, Int32Builder, MapBuilder, StringBuilder, UInt64Builder,
};
use datafusion::arrow::record_batch::RecordBatch;

use crate::client::{
    extract_any_value_string, get_resource_attributes, get_service_name, hex_encode,
};
use crate::proto::opentelemetry::proto::common::v1::KeyValue;
use crate::proto::opentelemetry::proto::metrics::v1::{metric, number_data_point};
use crate::store::Store;

use super::arrow_schema;

fn append_kv_map(builder: &mut MapBuilder<StringBuilder, StringBuilder>, kvs: &[KeyValue]) {
    for kv in kvs {
        builder.keys().append_value(&kv.key);
        let val = kv
            .value
            .as_ref()
            .map(extract_any_value_string)
            .unwrap_or_default();
        builder.values().append_value(val);
    }
    builder.append(true).unwrap();
}

pub fn traces_to_batch(store: &Store) -> RecordBatch {
    let schema = arrow_schema::traces_schema();

    let mut trace_id = StringBuilder::new();
    let mut span_id = StringBuilder::new();
    let mut parent_span_id = StringBuilder::new();
    let mut span_name = StringBuilder::new();
    let mut kind = Int32Builder::new();
    let mut start_time = UInt64Builder::new();
    let mut end_time = UInt64Builder::new();
    let mut duration_ns = UInt64Builder::new();
    let mut status_code = Int32Builder::new();
    let mut status_message = StringBuilder::new();
    let mut service_name = StringBuilder::new();
    let mut attributes = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let mut resource = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    for rs in store.all_traces() {
        let svc = get_service_name(&rs.resource);
        let res_attrs = get_resource_attributes(&rs.resource);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                trace_id.append_value(hex_encode(&span.trace_id));
                span_id.append_value(hex_encode(&span.span_id));
                if span.parent_span_id.is_empty() {
                    parent_span_id.append_null();
                } else {
                    parent_span_id.append_value(hex_encode(&span.parent_span_id));
                }
                span_name.append_value(&span.name);
                kind.append_value(span.kind);
                start_time.append_value(span.start_time_unix_nano);
                end_time.append_value(span.end_time_unix_nano);
                duration_ns.append_value(
                    span.end_time_unix_nano
                        .saturating_sub(span.start_time_unix_nano),
                );
                let (sc, sm) = match &span.status {
                    Some(s) => (s.code, s.message.as_str()),
                    None => (0, ""),
                };
                status_code.append_value(sc);
                status_message.append_value(sm);
                service_name.append_value(&svc);
                append_kv_map(&mut attributes, &span.attributes);
                append_kv_map(&mut resource, res_attrs);
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(parent_span_id.finish()),
        Arc::new(span_name.finish()),
        Arc::new(kind.finish()),
        Arc::new(start_time.finish()),
        Arc::new(end_time.finish()),
        Arc::new(duration_ns.finish()),
        Arc::new(status_code.finish()),
        Arc::new(status_message.finish()),
        Arc::new(service_name.finish()),
        Arc::new(attributes.finish()),
        Arc::new(resource.finish()),
    ];

    RecordBatch::try_new(schema, columns).expect("schema mismatch in traces_to_batch")
}

pub fn logs_to_batch(store: &Store) -> RecordBatch {
    let schema = arrow_schema::logs_schema();

    let mut timestamp = UInt64Builder::new();
    let mut severity = StringBuilder::new();
    let mut severity_number = Int32Builder::new();
    let mut body = StringBuilder::new();
    let mut service_name = StringBuilder::new();
    let mut trace_id = StringBuilder::new();
    let mut span_id = StringBuilder::new();
    let mut attributes = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let mut resource = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    for rl in store.all_logs() {
        let svc = get_service_name(&rl.resource);
        let res_attrs = get_resource_attributes(&rl.resource);
        for sl in &rl.scope_logs {
            for lr in &sl.log_records {
                timestamp.append_value(crate::store::log_timestamp(lr));
                severity.append_value(&lr.severity_text);
                severity_number.append_value(lr.severity_number);
                match &lr.body {
                    Some(v) => body.append_value(extract_any_value_string(v)),
                    None => body.append_null(),
                }
                service_name.append_value(&svc);
                trace_id.append_value(hex_encode(&lr.trace_id));
                span_id.append_value(hex_encode(&lr.span_id));
                append_kv_map(&mut attributes, &lr.attributes);
                append_kv_map(&mut resource, res_attrs);
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamp.finish()),
        Arc::new(severity.finish()),
        Arc::new(severity_number.finish()),
        Arc::new(body.finish()),
        Arc::new(service_name.finish()),
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(attributes.finish()),
        Arc::new(resource.finish()),
    ];

    RecordBatch::try_new(schema, columns).expect("schema mismatch in logs_to_batch")
}

struct MetricRowBuilders {
    timestamp: UInt64Builder,
    metric_name: StringBuilder,
    metric_type: StringBuilder,
    value: Float64Builder,
    count: UInt64Builder,
    sum: Float64Builder,
    service_name: StringBuilder,
    attributes: MapBuilder<StringBuilder, StringBuilder>,
    resource: MapBuilder<StringBuilder, StringBuilder>,
}

impl MetricRowBuilders {
    fn new() -> Self {
        Self {
            timestamp: UInt64Builder::new(),
            metric_name: StringBuilder::new(),
            metric_type: StringBuilder::new(),
            value: Float64Builder::new(),
            count: UInt64Builder::new(),
            sum: Float64Builder::new(),
            service_name: StringBuilder::new(),
            attributes: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),
            resource: MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        ts: u64,
        name: &str,
        mtype: &str,
        val: Option<f64>,
        cnt: Option<u64>,
        sm: Option<f64>,
        svc: &str,
        dp_attrs: &[KeyValue],
        res_attrs: &[KeyValue],
    ) {
        self.timestamp.append_value(ts);
        self.metric_name.append_value(name);
        self.metric_type.append_value(mtype);
        match val {
            Some(v) => self.value.append_value(v),
            None => self.value.append_null(),
        }
        match cnt {
            Some(c) => self.count.append_value(c),
            None => self.count.append_null(),
        }
        match sm {
            Some(s) => self.sum.append_value(s),
            None => self.sum.append_null(),
        }
        self.service_name.append_value(svc);
        append_kv_map(&mut self.attributes, dp_attrs);
        append_kv_map(&mut self.resource, res_attrs);
    }

    fn finish(mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.timestamp.finish()),
            Arc::new(self.metric_name.finish()),
            Arc::new(self.metric_type.finish()),
            Arc::new(self.value.finish()),
            Arc::new(self.count.finish()),
            Arc::new(self.sum.finish()),
            Arc::new(self.service_name.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.resource.finish()),
        ]
    }
}

fn number_dp_value(
    dp: &crate::proto::opentelemetry::proto::metrics::v1::NumberDataPoint,
) -> Option<f64> {
    match &dp.value {
        Some(number_data_point::Value::AsDouble(d)) => Some(*d),
        Some(number_data_point::Value::AsInt(i)) => Some(*i as f64),
        None => None,
    }
}

pub fn metrics_to_batch(store: &Store) -> RecordBatch {
    let schema = arrow_schema::metrics_schema();
    let mut b = MetricRowBuilders::new();

    for rm in store.all_metrics() {
        let svc = get_service_name(&rm.resource);
        let res_attrs = get_resource_attributes(&rm.resource);
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                match &m.data {
                    Some(metric::Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            b.append(
                                dp.time_unix_nano,
                                &m.name,
                                "Gauge",
                                number_dp_value(dp),
                                None,
                                None,
                                &svc,
                                &dp.attributes,
                                res_attrs,
                            );
                        }
                    }
                    Some(metric::Data::Sum(s)) => {
                        for dp in &s.data_points {
                            b.append(
                                dp.time_unix_nano,
                                &m.name,
                                "Sum",
                                number_dp_value(dp),
                                None,
                                None,
                                &svc,
                                &dp.attributes,
                                res_attrs,
                            );
                        }
                    }
                    Some(metric::Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            b.append(
                                dp.time_unix_nano,
                                &m.name,
                                "Histogram",
                                None,
                                Some(dp.count),
                                dp.sum,
                                &svc,
                                &dp.attributes,
                                res_attrs,
                            );
                        }
                    }
                    Some(metric::Data::ExponentialHistogram(eh)) => {
                        for dp in &eh.data_points {
                            b.append(
                                dp.time_unix_nano,
                                &m.name,
                                "ExponentialHistogram",
                                None,
                                Some(dp.count),
                                dp.sum,
                                &svc,
                                &dp.attributes,
                                res_attrs,
                            );
                        }
                    }
                    Some(metric::Data::Summary(s)) => {
                        for dp in &s.data_points {
                            b.append(
                                dp.time_unix_nano,
                                &m.name,
                                "Summary",
                                None,
                                Some(dp.count),
                                Some(dp.sum),
                                &svc,
                                &dp.attributes,
                                res_attrs,
                            );
                        }
                    }
                    None => {}
                }
            }
        }
    }

    RecordBatch::try_new(schema, b.finish()).expect("schema mismatch in metrics_to_batch")
}
