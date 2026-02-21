use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use crate::proto::opentelemetry::proto::{
    common::v1::{any_value, KeyValue},
    logs::v1::ResourceLogs,
    metrics::v1::{metric, ResourceMetrics},
    trace::v1::ResourceSpans,
};

#[derive(Debug, Clone)]
pub enum StoreEvent {
    TracesAdded,
    LogsAdded,
    MetricsAdded,
    TracesCleared,
    LogsCleared,
    MetricsCleared,
}

pub struct Store {
    traces: VecDeque<ResourceSpans>,
    logs: VecDeque<ResourceLogs>,
    metrics: VecDeque<ResourceMetrics>,
    max_items: usize,
    event_tx: broadcast::Sender<StoreEvent>,
}

pub type SharedStore = Arc<RwLock<Store>>;

#[derive(Default)]
pub struct TraceFilter {
    pub service_name: Option<String>,
    pub trace_id: Option<String>,
    pub attributes: Vec<(String, String)>,
}

#[derive(Default)]
pub struct LogFilter {
    pub service_name: Option<String>,
    pub severity: Option<String>,
    pub attributes: Vec<(String, String)>,
}

#[derive(Default)]
pub struct MetricFilter {
    pub service_name: Option<String>,
    pub metric_name: Option<String>,
}

fn trace_sort_key(rs: &ResourceSpans) -> u64 {
    rs.scope_spans
        .iter()
        .flat_map(|ss| ss.spans.iter().map(|s| s.start_time_unix_nano))
        .min()
        .unwrap_or(0)
}

fn log_sort_key(rl: &ResourceLogs) -> u64 {
    rl.scope_logs
        .iter()
        .flat_map(|sl| {
            sl.log_records.iter().map(|lr| {
                if lr.time_unix_nano > 0 {
                    lr.time_unix_nano
                } else {
                    lr.observed_time_unix_nano
                }
            })
        })
        .min()
        .unwrap_or(0)
}

fn metric_sort_key(rm: &ResourceMetrics) -> u64 {
    let mut min_ts = u64::MAX;
    for sm in &rm.scope_metrics {
        for m in &sm.metrics {
            match &m.data {
                Some(metric::Data::Gauge(g)) => {
                    for dp in &g.data_points {
                        min_ts = min_ts.min(dp.time_unix_nano);
                    }
                }
                Some(metric::Data::Sum(s)) => {
                    for dp in &s.data_points {
                        min_ts = min_ts.min(dp.time_unix_nano);
                    }
                }
                Some(metric::Data::Histogram(h)) => {
                    for dp in &h.data_points {
                        min_ts = min_ts.min(dp.time_unix_nano);
                    }
                }
                Some(metric::Data::ExponentialHistogram(eh)) => {
                    for dp in &eh.data_points {
                        min_ts = min_ts.min(dp.time_unix_nano);
                    }
                }
                Some(metric::Data::Summary(s)) => {
                    for dp in &s.data_points {
                        min_ts = min_ts.min(dp.time_unix_nano);
                    }
                }
                None => {}
            }
        }
    }
    if min_ts == u64::MAX {
        0
    } else {
        min_ts
    }
}

/// Find the insertion position in a sorted VecDeque using binary search.
/// Returns the index where `target_key` should be inserted to maintain ascending order.
fn sorted_insert_pos<T>(deque: &VecDeque<T>, target_key: u64, key_fn: impl Fn(&T) -> u64) -> usize {
    let mut lo = 0;
    let mut hi = deque.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if key_fn(&deque[mid]) <= target_key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn get_attribute_string(attributes: &[KeyValue], key: &str) -> Option<String> {
    attributes
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_ref())
        .and_then(|v| match &v.value {
            Some(any_value::Value::StringValue(s)) => Some(s.clone()),
            _ => None,
        })
}

impl Store {
    pub fn new(max_items: usize) -> (Self, broadcast::Receiver<StoreEvent>) {
        let (event_tx, event_rx) = broadcast::channel(256);
        let store = Store {
            traces: VecDeque::new(),
            logs: VecDeque::new(),
            metrics: VecDeque::new(),
            max_items,
            event_tx,
        };
        (store, event_rx)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StoreEvent> {
        self.event_tx.subscribe()
    }

    pub fn insert_traces(&mut self, resource_spans: Vec<ResourceSpans>) {
        for rs in resource_spans {
            let ts = trace_sort_key(&rs);
            let pos = sorted_insert_pos(&self.traces, ts, trace_sort_key);
            self.traces.insert(pos, rs);
            if self.traces.len() > self.max_items {
                self.traces.pop_front();
            }
        }
        let _ = self.event_tx.send(StoreEvent::TracesAdded);
    }

    pub fn insert_logs(&mut self, resource_logs: Vec<ResourceLogs>) {
        for rl in resource_logs {
            let ts = log_sort_key(&rl);
            let pos = sorted_insert_pos(&self.logs, ts, log_sort_key);
            self.logs.insert(pos, rl);
            if self.logs.len() > self.max_items {
                self.logs.pop_front();
            }
        }
        let _ = self.event_tx.send(StoreEvent::LogsAdded);
    }

    pub fn insert_metrics(&mut self, resource_metrics: Vec<ResourceMetrics>) {
        for rm in resource_metrics {
            let ts = metric_sort_key(&rm);
            let pos = sorted_insert_pos(&self.metrics, ts, metric_sort_key);
            self.metrics.insert(pos, rm);
            if self.metrics.len() > self.max_items {
                self.metrics.pop_front();
            }
        }
        let _ = self.event_tx.send(StoreEvent::MetricsAdded);
    }

    pub fn clear_traces(&mut self) {
        self.traces.clear();
        let _ = self.event_tx.send(StoreEvent::TracesCleared);
    }

    pub fn clear_logs(&mut self) {
        self.logs.clear();
        let _ = self.event_tx.send(StoreEvent::LogsCleared);
    }

    pub fn clear_metrics(&mut self) {
        self.metrics.clear();
        let _ = self.event_tx.send(StoreEvent::MetricsCleared);
    }

    pub fn trace_count(&self) -> usize {
        self.traces.len()
    }

    pub fn log_count(&self) -> usize {
        self.logs.len()
    }

    pub fn metric_count(&self) -> usize {
        self.metrics.len()
    }

    pub fn query_traces(&self, filter: &TraceFilter, limit: usize) -> Vec<ResourceSpans> {
        let mut result: Vec<_> = self.traces
            .iter()
            .rev()
            .filter(|rs| {
                if let Some(ref service_name) = filter.service_name {
                    let resource_attrs = rs
                        .resource
                        .as_ref()
                        .map(|r| r.attributes.as_slice())
                        .unwrap_or_default();
                    if get_attribute_string(resource_attrs, "service.name").as_deref()
                        != Some(service_name.as_str())
                    {
                        return false;
                    }
                }

                if let Some(ref trace_id_hex) = filter.trace_id {
                    let expected_bytes = hex_decode(trace_id_hex);
                    let has_matching_span = rs.scope_spans.iter().any(|ss| {
                        ss.spans.iter().any(|span| span.trace_id == expected_bytes)
                    });
                    if !has_matching_span {
                        return false;
                    }
                }

                if !filter.attributes.is_empty() {
                    let has_matching_attrs = rs.scope_spans.iter().any(|ss| {
                        ss.spans.iter().any(|span| {
                            filter.attributes.iter().all(|(key, value)| {
                                get_attribute_string(&span.attributes, key).as_deref()
                                    == Some(value.as_str())
                            })
                        })
                    });
                    if !has_matching_attrs {
                        return false;
                    }
                }

                true
            })
            .take(limit)
            .cloned()
            .collect();
        result.reverse();
        result
    }

    pub fn query_logs(&self, filter: &LogFilter, limit: usize) -> Vec<ResourceLogs> {
        let mut result: Vec<_> = self.logs
            .iter()
            .rev()
            .filter(|rl| {
                if let Some(ref service_name) = filter.service_name {
                    let resource_attrs = rl
                        .resource
                        .as_ref()
                        .map(|r| r.attributes.as_slice())
                        .unwrap_or_default();
                    if get_attribute_string(resource_attrs, "service.name").as_deref()
                        != Some(service_name.as_str())
                    {
                        return false;
                    }
                }

                if let Some(ref severity) = filter.severity {
                    let has_matching_severity = rl.scope_logs.iter().any(|sl| {
                        sl.log_records
                            .iter()
                            .any(|lr| lr.severity_text.eq_ignore_ascii_case(severity))
                    });
                    if !has_matching_severity {
                        return false;
                    }
                }

                if !filter.attributes.is_empty() {
                    let has_matching_attrs = rl.scope_logs.iter().any(|sl| {
                        sl.log_records.iter().any(|lr| {
                            filter.attributes.iter().all(|(key, value)| {
                                get_attribute_string(&lr.attributes, key).as_deref()
                                    == Some(value.as_str())
                            })
                        })
                    });
                    if !has_matching_attrs {
                        return false;
                    }
                }

                true
            })
            .take(limit)
            .cloned()
            .collect();
        result.reverse();
        result
    }

    pub fn query_metrics(&self, filter: &MetricFilter, limit: usize) -> Vec<ResourceMetrics> {
        let mut result: Vec<_> = self.metrics
            .iter()
            .rev()
            .filter(|rm| {
                if let Some(ref service_name) = filter.service_name {
                    let resource_attrs = rm
                        .resource
                        .as_ref()
                        .map(|r| r.attributes.as_slice())
                        .unwrap_or_default();
                    if get_attribute_string(resource_attrs, "service.name").as_deref()
                        != Some(service_name.as_str())
                    {
                        return false;
                    }
                }

                if let Some(ref metric_name) = filter.metric_name {
                    let has_matching_name = rm.scope_metrics.iter().any(|sm| {
                        sm.metrics.iter().any(|m| m.name == *metric_name)
                    });
                    if !has_matching_name {
                        return false;
                    }
                }

                true
            })
            .take(limit)
            .cloned()
            .collect();
        result.reverse();
        result
    }
}

pub fn new_shared(max_items: usize) -> (SharedStore, broadcast::Receiver<StoreEvent>) {
    let (store, rx) = Store::new(max_items);
    (Arc::new(RwLock::new(store)), rx)
}

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap_or(0))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::proto::{
        common::v1::{any_value, AnyValue, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{Metric, ResourceMetrics, ScopeMetrics},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };

    fn make_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    fn make_resource(service_name: &str) -> Option<Resource> {
        Some(Resource {
            attributes: vec![make_kv("service.name", service_name)],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        })
    }

    fn make_resource_spans(
        service_name: &str,
        trace_id: &[u8],
        attrs: &[(&str, &str)],
    ) -> ResourceSpans {
        make_resource_spans_full(service_name, trace_id, attrs, 0, 0)
    }

    fn make_resource_spans_full(
        service_name: &str,
        trace_id: &[u8],
        attrs: &[(&str, &str)],
        start_time_unix_nano: u64,
        end_time_unix_nano: u64,
    ) -> ResourceSpans {
        let span_attrs: Vec<KeyValue> = attrs.iter().map(|(k, v)| make_kv(k, v)).collect();
        ResourceSpans {
            resource: make_resource(service_name),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: trace_id.to_vec(),
                    span_id: vec![0, 0, 0, 0, 0, 0, 0, 1],
                    trace_state: String::new(),
                    parent_span_id: vec![],
                    flags: 0,
                    name: "test-span".to_string(),
                    kind: 0,
                    start_time_unix_nano,
                    end_time_unix_nano,
                    attributes: span_attrs,
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: None,
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    fn make_resource_logs(
        service_name: &str,
        severity: &str,
        attrs: &[(&str, &str)],
    ) -> ResourceLogs {
        make_resource_logs_full(service_name, severity, attrs, 0)
    }

    fn make_resource_logs_full(
        service_name: &str,
        severity: &str,
        attrs: &[(&str, &str)],
        time_unix_nano: u64,
    ) -> ResourceLogs {
        let log_attrs: Vec<KeyValue> = attrs.iter().map(|(k, v)| make_kv(k, v)).collect();
        ResourceLogs {
            resource: make_resource(service_name),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano,
                    observed_time_unix_nano: 0,
                    severity_number: 0,
                    severity_text: severity.to_string(),
                    body: None,
                    attributes: log_attrs,
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    fn make_resource_metrics(service_name: &str, metric_name: &str) -> ResourceMetrics {
        ResourceMetrics {
            resource: make_resource(service_name),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    data: None,
                    metadata: vec![],
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    fn get_svc_name(rs: &ResourceSpans) -> String {
        get_attribute_string(
            rs.resource
                .as_ref()
                .map(|r| r.attributes.as_slice())
                .unwrap_or_default(),
            "service.name",
        )
        .unwrap()
    }

    fn get_log_svc_name(rl: &ResourceLogs) -> String {
        get_attribute_string(
            rl.resource
                .as_ref()
                .map(|r| r.attributes.as_slice())
                .unwrap_or_default(),
            "service.name",
        )
        .unwrap()
    }

    #[test]
    fn insert_and_query_traces() {
        let (mut store, _rx) = Store::new(100);
        store.insert_traces(vec![make_resource_spans("svc-a", &[1; 16], &[])]);
        assert_eq!(store.query_traces(&TraceFilter::default(), 100).len(), 1);
    }

    #[test]
    fn insert_and_query_logs() {
        let (mut store, _rx) = Store::new(100);
        store.insert_logs(vec![make_resource_logs("svc-a", "INFO", &[])]);
        assert_eq!(store.query_logs(&LogFilter::default(), 100).len(), 1);
    }

    #[test]
    fn insert_and_query_metrics() {
        let (mut store, _rx) = Store::new(100);
        store.insert_metrics(vec![make_resource_metrics("svc-a", "http.duration")]);
        assert_eq!(store.query_metrics(&MetricFilter::default(), 100).len(), 1);
    }

    #[test]
    fn eviction_traces() {
        let (mut store, _rx) = Store::new(3);
        for i in 0..5u8 {
            store.insert_traces(vec![make_resource_spans(
                &format!("svc-{i}"),
                &[i; 16],
                &[],
            )]);
        }
        let result = store.query_traces(&TraceFilter::default(), 100);
        assert_eq!(result.len(), 3);
        let names: Vec<_> = result.iter().map(|rs| get_svc_name(rs)).collect();
        assert_eq!(names, vec!["svc-2", "svc-3", "svc-4"]);
    }

    #[test]
    fn eviction_logs() {
        let (mut store, _rx) = Store::new(3);
        for i in 0..5 {
            store.insert_logs(vec![make_resource_logs(
                &format!("svc-{i}"),
                "INFO",
                &[],
            )]);
        }
        assert_eq!(store.query_logs(&LogFilter::default(), 100).len(), 3);
    }

    #[test]
    fn eviction_metrics() {
        let (mut store, _rx) = Store::new(3);
        for i in 0..5 {
            store.insert_metrics(vec![make_resource_metrics(&format!("svc-{i}"), "cpu")]);
        }
        assert_eq!(store.query_metrics(&MetricFilter::default(), 100).len(), 3);
    }

    #[test]
    fn filter_traces_by_service_name() {
        let (mut store, _rx) = Store::new(100);
        store.insert_traces(vec![
            make_resource_spans("frontend", &[1; 16], &[]),
            make_resource_spans("backend", &[2; 16], &[]),
            make_resource_spans("frontend", &[3; 16], &[]),
        ]);
        let filter = TraceFilter {
            service_name: Some("frontend".to_string()),
            ..Default::default()
        };
        assert_eq!(store.query_traces(&filter, 100).len(), 2);
    }

    #[test]
    fn filter_traces_by_trace_id() {
        let (mut store, _rx) = Store::new(100);
        let trace_id_a = [
            0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01,
        ];
        let trace_id_b = [
            0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02,
        ];
        store.insert_traces(vec![
            make_resource_spans("svc", &trace_id_a, &[]),
            make_resource_spans("svc", &trace_id_b, &[]),
        ]);
        let filter = TraceFilter {
            trace_id: Some("abcdef01234567890000000000000001".to_string()),
            ..Default::default()
        };
        let result = store.query_traces(&filter, 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].scope_spans[0].spans[0].trace_id, trace_id_a);
    }

    #[test]
    fn filter_traces_by_attributes() {
        let (mut store, _rx) = Store::new(100);
        store.insert_traces(vec![
            make_resource_spans("svc", &[1; 16], &[("env", "prod"), ("region", "us")]),
            make_resource_spans("svc", &[2; 16], &[("env", "staging"), ("region", "eu")]),
            make_resource_spans("svc", &[3; 16], &[("env", "prod"), ("region", "eu")]),
        ]);

        let filter = TraceFilter {
            attributes: vec![("env".to_string(), "prod".to_string())],
            ..Default::default()
        };
        assert_eq!(store.query_traces(&filter, 100).len(), 2);

        let filter = TraceFilter {
            attributes: vec![
                ("env".to_string(), "prod".to_string()),
                ("region".to_string(), "us".to_string()),
            ],
            ..Default::default()
        };
        assert_eq!(store.query_traces(&filter, 100).len(), 1);
    }

    #[test]
    fn filter_logs_by_service_name() {
        let (mut store, _rx) = Store::new(100);
        store.insert_logs(vec![
            make_resource_logs("frontend", "INFO", &[]),
            make_resource_logs("backend", "WARN", &[]),
            make_resource_logs("frontend", "ERROR", &[]),
        ]);
        let filter = LogFilter {
            service_name: Some("frontend".to_string()),
            ..Default::default()
        };
        assert_eq!(store.query_logs(&filter, 100).len(), 2);
    }

    #[test]
    fn filter_logs_by_severity() {
        let (mut store, _rx) = Store::new(100);
        store.insert_logs(vec![
            make_resource_logs("svc", "INFO", &[]),
            make_resource_logs("svc", "ERROR", &[]),
            make_resource_logs("svc", "error", &[]),
            make_resource_logs("svc", "WARN", &[]),
        ]);
        let filter = LogFilter {
            severity: Some("ERROR".to_string()),
            ..Default::default()
        };
        assert_eq!(store.query_logs(&filter, 100).len(), 2);
    }

    #[test]
    fn filter_logs_by_attributes() {
        let (mut store, _rx) = Store::new(100);
        store.insert_logs(vec![
            make_resource_logs("svc", "INFO", &[("env", "prod")]),
            make_resource_logs("svc", "INFO", &[("env", "staging")]),
            make_resource_logs("svc", "INFO", &[("env", "prod"), ("region", "us")]),
        ]);
        let filter = LogFilter {
            attributes: vec![("env".to_string(), "prod".to_string())],
            ..Default::default()
        };
        assert_eq!(store.query_logs(&filter, 100).len(), 2);
    }

    #[test]
    fn filter_metrics_by_service_name() {
        let (mut store, _rx) = Store::new(100);
        store.insert_metrics(vec![
            make_resource_metrics("frontend", "http.duration"),
            make_resource_metrics("backend", "db.latency"),
            make_resource_metrics("frontend", "http.count"),
        ]);
        let filter = MetricFilter {
            service_name: Some("frontend".to_string()),
            ..Default::default()
        };
        assert_eq!(store.query_metrics(&filter, 100).len(), 2);
    }

    #[test]
    fn filter_metrics_by_metric_name() {
        let (mut store, _rx) = Store::new(100);
        store.insert_metrics(vec![
            make_resource_metrics("svc", "http.duration"),
            make_resource_metrics("svc", "db.latency"),
            make_resource_metrics("svc", "http.duration"),
        ]);
        let filter = MetricFilter {
            metric_name: Some("http.duration".to_string()),
            ..Default::default()
        };
        assert_eq!(store.query_metrics(&filter, 100).len(), 2);
    }

    #[test]
    fn query_limit() {
        let (mut store, _rx) = Store::new(100);
        for i in 0..10u8 {
            store.insert_traces(vec![make_resource_spans("svc", &[i; 16], &[])]);
        }
        assert_eq!(store.query_traces(&TraceFilter::default(), 3).len(), 3);
    }

    #[test]
    fn event_notification() {
        let (mut store, mut rx) = Store::new(100);

        store.insert_traces(vec![make_resource_spans("svc", &[1; 16], &[])]);
        assert!(matches!(rx.try_recv().unwrap(), StoreEvent::TracesAdded));

        store.insert_logs(vec![make_resource_logs("svc", "INFO", &[])]);
        assert!(matches!(rx.try_recv().unwrap(), StoreEvent::LogsAdded));

        store.insert_metrics(vec![make_resource_metrics("svc", "cpu")]);
        assert!(matches!(rx.try_recv().unwrap(), StoreEvent::MetricsAdded));
    }

    #[test]
    fn insert_traces_sorted_by_timestamp() {
        let (mut store, _rx) = Store::new(100);
        store.insert_traces(vec![make_resource_spans_full("svc-300", &[0; 16], &[], 300, 400)]);
        store.insert_traces(vec![make_resource_spans_full("svc-100", &[0; 16], &[], 100, 200)]);
        store.insert_traces(vec![make_resource_spans_full("svc-200", &[0; 16], &[], 200, 300)]);

        let result = store.query_traces(&TraceFilter::default(), 100);
        let names: Vec<_> = result.iter().map(|rs| get_svc_name(rs)).collect();
        assert_eq!(names, vec!["svc-100", "svc-200", "svc-300"]);
    }

    #[test]
    fn insert_logs_sorted_by_timestamp() {
        let (mut store, _rx) = Store::new(100);
        store.insert_logs(vec![make_resource_logs_full("svc-300", "INFO", &[], 300)]);
        store.insert_logs(vec![make_resource_logs_full("svc-100", "INFO", &[], 100)]);
        store.insert_logs(vec![make_resource_logs_full("svc-200", "INFO", &[], 200)]);

        let result = store.query_logs(&LogFilter::default(), 100);
        let names: Vec<_> = result.iter().map(|rl| get_log_svc_name(rl)).collect();
        assert_eq!(names, vec!["svc-100", "svc-200", "svc-300"]);
    }
}
