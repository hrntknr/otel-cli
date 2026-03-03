use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::instrument;

use crate::proto::opentelemetry::proto::{
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

pub fn rs_sort_key(rs: &ResourceSpans) -> u64 {
    rs.scope_spans
        .iter()
        .flat_map(|ss| ss.spans.iter().map(|s| s.start_time_unix_nano))
        .min()
        .unwrap_or(0)
}

/// Returns the effective timestamp for a log record,
/// falling back to observed_time_unix_nano when time_unix_nano is not set.
pub fn log_timestamp(lr: &crate::proto::opentelemetry::proto::logs::v1::LogRecord) -> u64 {
    if lr.time_unix_nano > 0 {
        lr.time_unix_nano
    } else {
        lr.observed_time_unix_nano
    }
}

pub fn log_sort_key(rl: &ResourceLogs) -> u64 {
    rl.scope_logs
        .iter()
        .flat_map(|sl| sl.log_records.iter().map(|lr| log_timestamp(lr)))
        .min()
        .unwrap_or(0)
}

pub fn metric_sort_key(rm: &ResourceMetrics) -> u64 {
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

pub fn severity_text_to_number(text: &str) -> Option<i32> {
    match text.to_ascii_uppercase().as_str() {
        "TRACE" => Some(1),
        "DEBUG" => Some(5),
        "INFO" => Some(9),
        "WARN" | "WARNING" => Some(13),
        "ERROR" => Some(17),
        "FATAL" => Some(21),
        _ => text.parse::<i32>().ok(),
    }
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

    pub fn all_traces(&self) -> &VecDeque<ResourceSpans> {
        &self.traces
    }

    pub fn all_logs(&self) -> &VecDeque<ResourceLogs> {
        &self.logs
    }

    pub fn all_metrics(&self) -> &VecDeque<ResourceMetrics> {
        &self.metrics
    }

    #[instrument(name = "store.insert_traces", skip_all, fields(count = resource_spans.len()))]
    pub fn insert_traces(&mut self, resource_spans: Vec<ResourceSpans>) {
        for rs in resource_spans {
            let ts = rs_sort_key(&rs);
            let pos = sorted_insert_pos(&self.traces, ts, rs_sort_key);
            self.traces.insert(pos, rs);
            if self.traces.len() > self.max_items {
                self.traces.pop_front();
                tracing::debug!(max_items = self.max_items, "trace evicted");
            }
        }
        let _ = self.event_tx.send(StoreEvent::TracesAdded);
    }

    #[instrument(name = "store.insert_logs", skip_all, fields(count = resource_logs.len()))]
    pub fn insert_logs(&mut self, resource_logs: Vec<ResourceLogs>) {
        for rl in resource_logs {
            let ts = log_sort_key(&rl);
            let pos = sorted_insert_pos(&self.logs, ts, log_sort_key);
            self.logs.insert(pos, rl);
            if self.logs.len() > self.max_items {
                self.logs.pop_front();
                tracing::debug!(max_items = self.max_items, "log evicted");
            }
        }
        let _ = self.event_tx.send(StoreEvent::LogsAdded);
    }

    #[instrument(name = "store.insert_metrics", skip_all, fields(count = resource_metrics.len()))]
    pub fn insert_metrics(&mut self, resource_metrics: Vec<ResourceMetrics>) {
        for rm in resource_metrics {
            let ts = metric_sort_key(&rm);
            let pos = sorted_insert_pos(&self.metrics, ts, metric_sort_key);
            self.metrics.insert(pos, rm);
            if self.metrics.len() > self.max_items {
                self.metrics.pop_front();
                tracing::debug!(max_items = self.max_items, "metric evicted");
            }
        }
        let _ = self.event_tx.send(StoreEvent::MetricsAdded);
    }

    #[instrument(name = "store.clear_traces", skip_all)]
    pub fn clear_traces(&mut self) {
        self.traces.clear();
        let _ = self.event_tx.send(StoreEvent::TracesCleared);
    }

    #[instrument(name = "store.clear_logs", skip_all)]
    pub fn clear_logs(&mut self) {
        self.logs.clear();
        let _ = self.event_tx.send(StoreEvent::LogsCleared);
    }

    #[instrument(name = "store.clear_metrics", skip_all)]
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

    pub fn query_traces_since(&self, min_ts: u64) -> Vec<ResourceSpans> {
        self.traces
            .iter()
            .filter(|rs| rs_sort_key(rs) >= min_ts)
            .cloned()
            .collect()
    }

    pub fn query_logs_since(&self, min_ts: u64) -> Vec<ResourceLogs> {
        self.logs
            .iter()
            .filter(|rl| log_sort_key(rl) >= min_ts)
            .cloned()
            .collect()
    }

    pub fn query_metrics_since(&self, min_ts: u64) -> Vec<ResourceMetrics> {
        self.metrics
            .iter()
            .filter(|rm| metric_sort_key(rm) >= min_ts)
            .cloned()
            .collect()
    }
}

pub fn new_shared(max_items: usize) -> (SharedStore, broadcast::Receiver<StoreEvent>) {
    let (store, rx) = Store::new(max_items);
    (Arc::new(RwLock::new(store)), rx)
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
        let severity_number = severity_text_to_number(severity).unwrap_or(0);
        let log_attrs: Vec<KeyValue> = attrs.iter().map(|(k, v)| make_kv(k, v)).collect();
        ResourceLogs {
            resource: make_resource(service_name),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano,
                    observed_time_unix_nano: 0,
                    severity_number,
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
        rs.resource
            .as_ref()
            .and_then(|r| {
                r.attributes
                    .iter()
                    .find(|kv| kv.key == "service.name")
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|v| match &v.value {
                        Some(any_value::Value::StringValue(s)) => Some(s.clone()),
                        _ => None,
                    })
            })
            .unwrap()
    }

    #[test]
    fn insert_and_all_traces() {
        let (mut store, _rx) = Store::new(100);
        store.insert_traces(vec![make_resource_spans("svc-a", &[1; 16], &[])]);
        assert_eq!(store.all_traces().len(), 1);
    }

    #[test]
    fn insert_and_all_logs() {
        let (mut store, _rx) = Store::new(100);
        store.insert_logs(vec![make_resource_logs("svc-a", "INFO", &[])]);
        assert_eq!(store.all_logs().len(), 1);
    }

    #[test]
    fn insert_and_all_metrics() {
        let (mut store, _rx) = Store::new(100);
        store.insert_metrics(vec![make_resource_metrics("svc-a", "http.duration")]);
        assert_eq!(store.all_metrics().len(), 1);
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
        assert_eq!(store.all_traces().len(), 3);
        let names: Vec<_> = store
            .all_traces()
            .iter()
            .map(|rs| get_svc_name(rs))
            .collect();
        assert_eq!(names, vec!["svc-2", "svc-3", "svc-4"]);
    }

    #[test]
    fn eviction_traces_flat() {
        let (mut store, _rx) = Store::new(2);
        store.insert_traces(vec![
            make_resource_spans_full("svc-a", &[1; 16], &[], 100, 200),
            make_resource_spans_full("svc-b", &[1; 16], &[], 200, 300),
        ]);
        // After inserting 2 items at max_items=2, inserting more evicts oldest
        store.insert_traces(vec![make_resource_spans_full(
            "svc-c",
            &[2; 16],
            &[],
            300,
            400,
        )]);

        assert_eq!(store.all_traces().len(), 2);
        let names: Vec<_> = store
            .all_traces()
            .iter()
            .map(|rs| get_svc_name(rs))
            .collect();
        assert_eq!(names, vec!["svc-b", "svc-c"]);
    }

    #[test]
    fn eviction_logs() {
        let (mut store, _rx) = Store::new(3);
        for i in 0..5 {
            store.insert_logs(vec![make_resource_logs(&format!("svc-{i}"), "INFO", &[])]);
        }
        assert_eq!(store.all_logs().len(), 3);
    }

    #[test]
    fn eviction_metrics() {
        let (mut store, _rx) = Store::new(3);
        for i in 0..5 {
            store.insert_metrics(vec![make_resource_metrics(&format!("svc-{i}"), "cpu")]);
        }
        assert_eq!(store.all_metrics().len(), 3);
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
        store.insert_traces(vec![make_resource_spans_full(
            "svc-300",
            &[0; 16],
            &[],
            300,
            400,
        )]);
        store.insert_traces(vec![make_resource_spans_full(
            "svc-100",
            &[0; 16],
            &[],
            100,
            200,
        )]);
        store.insert_traces(vec![make_resource_spans_full(
            "svc-200",
            &[0; 16],
            &[],
            200,
            300,
        )]);

        assert_eq!(store.all_traces().len(), 3);
        let names: Vec<_> = store
            .all_traces()
            .iter()
            .map(get_svc_name)
            .collect();
        assert_eq!(names, vec!["svc-100", "svc-200", "svc-300"]);
    }

    #[test]
    fn insert_logs_sorted_by_timestamp() {
        let (mut store, _rx) = Store::new(100);
        store.insert_logs(vec![make_resource_logs_full("svc-300", "INFO", &[], 300)]);
        store.insert_logs(vec![make_resource_logs_full("svc-100", "INFO", &[], 100)]);
        store.insert_logs(vec![make_resource_logs_full("svc-200", "INFO", &[], 200)]);

        assert_eq!(store.all_logs().len(), 3);
    }

    #[test]
    fn query_traces_since() {
        let (mut store, _rx) = Store::new(100);
        store.insert_traces(vec![
            make_resource_spans_full("svc-a", &[1; 16], &[], 100, 200),
            make_resource_spans_full("svc-b", &[1; 16], &[], 200, 300),
            make_resource_spans_full("svc-c", &[2; 16], &[], 300, 400),
        ]);
        assert_eq!(store.query_traces_since(200).len(), 2);
        assert_eq!(store.query_traces_since(301).len(), 0);
    }

    #[test]
    fn query_logs_since() {
        let (mut store, _rx) = Store::new(100);
        store.insert_logs(vec![
            make_resource_logs_full("svc", "INFO", &[], 100),
            make_resource_logs_full("svc", "INFO", &[], 200),
            make_resource_logs_full("svc", "INFO", &[], 300),
        ]);
        assert_eq!(store.query_logs_since(200).len(), 2);
        assert_eq!(store.query_logs_since(301).len(), 0);
    }

    #[test]
    fn query_metrics_since() {
        use crate::proto::opentelemetry::proto::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
        };

        fn make_resource_metrics_with_ts(
            service_name: &str,
            metric_name: &str,
            time_unix_nano: u64,
        ) -> ResourceMetrics {
            ResourceMetrics {
                resource: make_resource(service_name),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: metric_name.to_string(),
                        description: String::new(),
                        unit: String::new(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano,
                                ..Default::default()
                            }],
                        })),
                        metadata: vec![],
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }
        }

        let (mut store, _rx) = Store::new(100);
        store.insert_metrics(vec![
            make_resource_metrics_with_ts("svc", "cpu", 100),
            make_resource_metrics_with_ts("svc", "cpu", 200),
            make_resource_metrics_with_ts("svc", "cpu", 300),
        ]);
        assert_eq!(store.query_metrics_since(200).len(), 2);
        assert_eq!(store.query_metrics_since(301).len(), 0);
    }
}
