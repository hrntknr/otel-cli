pub mod event;
pub mod tabs;
pub mod ui;

use std::io;

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::prelude::*;
use ratatui::widgets::TableState;
use tokio::sync::broadcast;

use std::collections::HashMap;

use crate::client;
use crate::proto::opentelemetry::proto::{
    logs::v1::ResourceLogs,
    metrics::v1::{metric, ResourceMetrics},
    trace::v1::ResourceSpans,
};
use crate::store::{SharedStore, StoreEvent};

pub struct TraceSummary {
    pub trace_id: String,
    pub root_service: String,
    pub root_span_name: String,
    pub span_count: usize,
    pub duration: String,
    pub start_time: String,
}

pub struct TimelineSpan {
    pub span_id: String,
    pub service_name: String,
    pub span_name: String,
    pub start_ns: u64,
    pub end_ns: u64,
    pub depth: usize,
    pub duration: String,
    pub status_code: i32,
}

#[derive(Default)]
pub enum TraceView {
    #[default]
    List,
    Timeline(String),
}

pub struct LogRow {
    pub timestamp: String,
    pub severity: String,
    pub service_name: String,
    pub body: String,
    pub trace_id: String,
    pub span_id: String,
    pub severity_number: i32,
    pub attributes: Vec<(String, String)>,
    pub resource_attributes: Vec<(String, String)>,
}

pub struct MetricRow {
    pub name: String,
    pub metric_type: String,
    pub service_name: String,
}

pub struct App {
    store: SharedStore,
    event_handler: event::EventHandler,
    pub current_tab: tabs::Tab,
    pub table_state: TableState,
    pub logs_data: Vec<LogRow>,
    pub metrics_data: Vec<MetricRow>,
    pub trace_count: usize,
    pub log_count: usize,
    pub metric_count: usize,
    pub follow: bool,
    pub page_size: usize,
    should_quit: bool,
    pending_clear: bool,
    pub trace_view: TraceView,
    pub trace_summaries: Vec<TraceSummary>,
    pub timeline_spans: Vec<TimelineSpan>,
    pub timeline_table_state: TableState,
    raw_traces: Vec<ResourceSpans>,
}

impl App {
    fn new(store: SharedStore, event_rx: broadcast::Receiver<StoreEvent>) -> Self {
        Self {
            store,
            event_handler: event::EventHandler::new(event_rx),
            current_tab: tabs::Tab::Logs,
            table_state: TableState::default(),
            logs_data: Vec::new(),
            metrics_data: Vec::new(),
            trace_count: 0,
            log_count: 0,
            metric_count: 0,
            follow: true,
            page_size: 20,
            should_quit: false,
            pending_clear: false,
            trace_view: TraceView::default(),
            trace_summaries: Vec::new(),
            timeline_spans: Vec::new(),
            timeline_table_state: TableState::default(),
            raw_traces: Vec::new(),
        }
    }

    async fn run<B: Backend<Error: Send + Sync + 'static>>(
        mut self,
        terminal: &mut Terminal<B>,
    ) -> anyhow::Result<()> {
        self.refresh_data().await;

        loop {
            terminal.draw(|frame| ui::draw(frame, &mut self))?;

            match self.event_handler.next().await {
                event::AppEvent::Key(key) => self.handle_key(key),
                event::AppEvent::StoreUpdate => self.refresh_data().await,
                event::AppEvent::Tick => {}
            }

            if self.pending_clear {
                self.pending_clear = false;
                self.clear_current_tab().await;
            }

            if self.should_quit {
                return Ok(());
            }
        }
    }

    fn handle_key(&mut self, key: crossterm::event::KeyEvent) {
        if key.kind != crossterm::event::KeyEventKind::Press {
            return;
        }
        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
            }
            KeyCode::Tab => {
                self.current_tab = self.current_tab.next();
                self.table_state = TableState::default();
                self.trace_view = TraceView::List;
            }
            KeyCode::BackTab => {
                self.current_tab = self.current_tab.prev();
                self.table_state = TableState::default();
                self.trace_view = TraceView::List;
            }
            KeyCode::Char('1') => {
                self.current_tab = tabs::Tab::Logs;
                self.table_state = TableState::default();
                self.trace_view = TraceView::List;
            }
            KeyCode::Char('2') => {
                self.current_tab = tabs::Tab::Traces;
                self.table_state = TableState::default();
                self.trace_view = TraceView::List;
            }
            KeyCode::Char('3') => {
                self.current_tab = tabs::Tab::Metrics;
                self.table_state = TableState::default();
                self.trace_view = TraceView::List;
            }
            KeyCode::Down | KeyCode::Char('j') => self.select_next(),
            KeyCode::Up | KeyCode::Char('k') => self.select_prev(),
            KeyCode::PageDown | KeyCode::Char(' ') => self.select_next_page(),
            KeyCode::PageUp => self.select_prev_page(),
            KeyCode::Char('f') => {
                if self.current_tab == tabs::Tab::Logs {
                    self.follow = !self.follow;
                    if self.follow && !self.logs_data.is_empty() {
                        self.table_state
                            .select(Some(self.logs_data.len() - 1));
                    }
                }
            }
            KeyCode::Enter => {
                if self.current_tab == tabs::Tab::Traces {
                    if let TraceView::List = self.trace_view {
                        if let Some(idx) = self.table_state.selected() {
                            if let Some(summary) = self.trace_summaries.get(idx) {
                                let trace_id = summary.trace_id.clone();
                                self.timeline_spans =
                                    build_timeline_spans(&self.raw_traces, &trace_id);
                                self.timeline_table_state = TableState::default();
                                if !self.timeline_spans.is_empty() {
                                    self.timeline_table_state.select(Some(0));
                                }
                                self.trace_view = TraceView::Timeline(trace_id);
                            }
                        }
                    }
                }
            }
            KeyCode::Char('c') => {
                self.pending_clear = true;
            }
            KeyCode::Esc => {
                if self.current_tab == tabs::Tab::Traces {
                    if let TraceView::Timeline(_) = self.trace_view {
                        self.trace_view = TraceView::List;
                        return;
                    }
                }
                self.table_state.select(None);
            }
            _ => {}
        }
    }

    fn current_list_len(&self) -> usize {
        match self.current_tab {
            tabs::Tab::Traces => match self.trace_view {
                TraceView::List => self.trace_summaries.len(),
                TraceView::Timeline(_) => self.timeline_spans.len(),
            },
            tabs::Tab::Logs => self.logs_data.len(),
            tabs::Tab::Metrics => self.metrics_data.len(),
        }
    }

    fn active_table_state(&mut self) -> &mut TableState {
        if self.current_tab == tabs::Tab::Traces {
            if let TraceView::Timeline(_) = self.trace_view {
                return &mut self.timeline_table_state;
            }
        }
        &mut self.table_state
    }

    fn select_next(&mut self) {
        let len = self.current_list_len();
        if len == 0 {
            return;
        }
        let state = self.active_table_state();
        let i = state
            .selected()
            .map(|i| (i + 1) % len)
            .unwrap_or(0);
        state.select(Some(i));
        if self.current_tab == tabs::Tab::Logs && i == len - 1 {
            self.follow = true;
        }
    }

    fn select_prev(&mut self) {
        let len = self.current_list_len();
        if len == 0 {
            return;
        }
        let state = self.active_table_state();
        let i = state
            .selected()
            .map(|i| if i == 0 { len - 1 } else { i - 1 })
            .unwrap_or(0);
        state.select(Some(i));
        if self.current_tab == tabs::Tab::Logs {
            self.follow = false;
        }
    }

    fn select_next_page(&mut self) {
        let len = self.current_list_len();
        if len == 0 {
            return;
        }
        let page = self.page_size;
        let state = self.active_table_state();
        let i = state
            .selected()
            .map(|i| (i + page).min(len - 1))
            .unwrap_or(0);
        state.select(Some(i));
        if self.current_tab == tabs::Tab::Logs && i == len - 1 {
            self.follow = true;
        }
    }

    fn select_prev_page(&mut self) {
        let len = self.current_list_len();
        if len == 0 {
            return;
        }
        let page = self.page_size;
        let state = self.active_table_state();
        let i = state
            .selected()
            .map(|i| i.saturating_sub(page))
            .unwrap_or(0);
        state.select(Some(i));
        if self.current_tab == tabs::Tab::Logs {
            self.follow = false;
        }
    }

    async fn clear_current_tab(&mut self) {
        let mut store = self.store.write().await;
        match self.current_tab {
            tabs::Tab::Traces => store.clear_traces(),
            tabs::Tab::Logs => store.clear_logs(),
            tabs::Tab::Metrics => store.clear_metrics(),
        }
        drop(store);
        self.table_state = TableState::default();
        if self.current_tab == tabs::Tab::Traces {
            self.trace_view = TraceView::List;
            self.timeline_table_state = TableState::default();
        }
        self.refresh_data().await;
    }

    async fn refresh_data(&mut self) {
        let store = self.store.read().await;
        self.trace_count = store.trace_count();
        self.log_count = store.log_count();
        self.metric_count = store.metric_count();

        let traces = store.query_traces(&Default::default(), 500);
        let logs = store.query_logs(&Default::default(), 500);
        let metrics = store.query_metrics(&Default::default(), 500);
        drop(store);

        self.raw_traces = traces;
        self.trace_summaries = build_trace_summaries(&self.raw_traces);
        if let TraceView::Timeline(ref trace_id) = self.trace_view {
            self.timeline_spans = build_timeline_spans(&self.raw_traces, trace_id);
        }

        self.logs_data = logs
            .into_iter()
            .flat_map(|rl| convert_log_rows(&rl))
            .collect();

        if self.current_tab == tabs::Tab::Logs && self.follow && !self.logs_data.is_empty() {
            self.table_state
                .select(Some(self.logs_data.len() - 1));
        }

        self.metrics_data = metrics
            .into_iter()
            .flat_map(|rm| convert_metric_rows(&rm))
            .collect();
    }
}

pub fn format_duration_ns(duration_ns: u64) -> String {
    if duration_ns >= 1_000_000_000 {
        format!("{:.2}s", duration_ns as f64 / 1_000_000_000.0)
    } else if duration_ns >= 1_000_000 {
        format!("{:.2}ms", duration_ns as f64 / 1_000_000.0)
    } else if duration_ns >= 1_000 {
        format!("{:.2}us", duration_ns as f64 / 1_000.0)
    } else {
        format!("{}ns", duration_ns)
    }
}

struct CollectedSpan {
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    service_name: String,
    span_name: String,
    start_ns: u64,
    end_ns: u64,
    status_code: i32,
}

fn collect_all_spans(resource_spans: &[ResourceSpans]) -> Vec<CollectedSpan> {
    let mut spans = Vec::new();
    for rs in resource_spans {
        let service_name = client::get_service_name(&rs.resource);
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                spans.push(CollectedSpan {
                    trace_id: client::hex_encode(&span.trace_id),
                    span_id: client::hex_encode(&span.span_id),
                    parent_span_id: client::hex_encode(&span.parent_span_id),
                    service_name: service_name.clone(),
                    span_name: span.name.clone(),
                    start_ns: span.start_time_unix_nano,
                    end_ns: span.end_time_unix_nano,
                    status_code: span
                        .status
                        .as_ref()
                        .map(|s| s.code)
                        .unwrap_or(0),
                });
            }
        }
    }
    spans
}

fn build_trace_summaries(resource_spans: &[ResourceSpans]) -> Vec<TraceSummary> {
    let spans = collect_all_spans(resource_spans);

    let mut grouped: HashMap<String, Vec<&CollectedSpan>> = HashMap::new();
    for span in &spans {
        grouped.entry(span.trace_id.clone()).or_default().push(span);
    }

    let mut summaries: Vec<TraceSummary> = grouped
        .into_iter()
        .map(|(trace_id, group)| {
            let root = group
                .iter()
                .find(|s| s.parent_span_id.chars().all(|c| c == '0') || s.parent_span_id.is_empty());
            let (root_service, root_span_name) = match root {
                Some(r) => (r.service_name.clone(), r.span_name.clone()),
                None => (
                    group[0].service_name.clone(),
                    group[0].span_name.clone(),
                ),
            };
            let min_start = group.iter().map(|s| s.start_ns).min().unwrap_or(0);
            let max_end = group.iter().map(|s| s.end_ns).max().unwrap_or(0);
            let duration_ns = max_end.saturating_sub(min_start);
            TraceSummary {
                trace_id,
                root_service,
                root_span_name,
                span_count: group.len(),
                duration: format_duration_ns(duration_ns),
                start_time: client::format_timestamp(min_start),
            }
        })
        .collect();

    summaries.sort_by(|a, b| b.start_time.cmp(&a.start_time));
    summaries
}

fn build_timeline_spans(resource_spans: &[ResourceSpans], trace_id: &str) -> Vec<TimelineSpan> {
    let all_spans = collect_all_spans(resource_spans);
    let spans: Vec<&CollectedSpan> = all_spans.iter().filter(|s| s.trace_id == trace_id).collect();

    if spans.is_empty() {
        return Vec::new();
    }

    let span_ids: std::collections::HashSet<&str> =
        spans.iter().map(|s| s.span_id.as_str()).collect();

    // Build parent -> children map
    let mut children_map: HashMap<&str, Vec<&CollectedSpan>> = HashMap::new();
    let mut roots: Vec<&CollectedSpan> = Vec::new();

    for span in &spans {
        let is_root = span.parent_span_id.chars().all(|c| c == '0')
            || span.parent_span_id.is_empty()
            || !span_ids.contains(span.parent_span_id.as_str());
        if is_root {
            roots.push(span);
        } else {
            children_map
                .entry(span.parent_span_id.as_str())
                .or_default()
                .push(span);
        }
    }

    // Sort roots by start time
    roots.sort_by_key(|s| s.start_ns);

    // DFS to flatten tree
    let mut result = Vec::new();
    let mut stack: Vec<(&CollectedSpan, usize)> = roots.into_iter().rev().map(|s| (s, 0)).collect();

    while let Some((span, depth)) = stack.pop() {
        result.push(TimelineSpan {
            span_id: span.span_id.clone(),
            service_name: span.service_name.clone(),
            span_name: span.span_name.clone(),
            start_ns: span.start_ns,
            end_ns: span.end_ns,
            depth,
            duration: format_duration_ns(span.end_ns.saturating_sub(span.start_ns)),
            status_code: span.status_code,
        });

        if let Some(children) = children_map.get(span.span_id.as_str()) {
            let mut sorted_children: Vec<&CollectedSpan> = children.clone();
            sorted_children.sort_by_key(|s| std::cmp::Reverse(s.start_ns));
            for child in sorted_children {
                stack.push((child, depth + 1));
            }
        }
    }

    result
}

fn extract_kv_pairs(
    attributes: &[crate::proto::opentelemetry::proto::common::v1::KeyValue],
) -> Vec<(String, String)> {
    attributes
        .iter()
        .map(|kv| {
            let val = kv
                .value
                .as_ref()
                .map(client::extract_any_value_string)
                .unwrap_or_default();
            (kv.key.clone(), val)
        })
        .collect()
}

fn convert_log_rows(rl: &ResourceLogs) -> Vec<LogRow> {
    let service_name = client::get_service_name(&rl.resource);
    let resource_attributes = rl
        .resource
        .as_ref()
        .map(|r| extract_kv_pairs(&r.attributes))
        .unwrap_or_default();
    rl.scope_logs
        .iter()
        .flat_map(|sl| {
            sl.log_records
                .iter()
                .map(|lr| {
                    let body = lr
                        .body
                        .as_ref()
                        .map(client::extract_any_value_string)
                        .unwrap_or_default();
                    LogRow {
                        timestamp: client::format_timestamp(lr.time_unix_nano),
                        severity: lr.severity_text.clone(),
                        service_name: service_name.clone(),
                        body,
                        trace_id: client::hex_encode(&lr.trace_id),
                        span_id: client::hex_encode(&lr.span_id),
                        severity_number: lr.severity_number,
                        attributes: extract_kv_pairs(&lr.attributes),
                        resource_attributes: resource_attributes.clone(),
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn convert_metric_rows(rm: &ResourceMetrics) -> Vec<MetricRow> {
    let service_name = client::get_service_name(&rm.resource);
    rm.scope_metrics
        .iter()
        .flat_map(|sm| {
            sm.metrics
                .iter()
                .map(|m| {
                    let metric_type = match &m.data {
                        Some(metric::Data::Gauge(_)) => "gauge",
                        Some(metric::Data::Sum(_)) => "sum",
                        Some(metric::Data::Histogram(_)) => "histogram",
                        Some(metric::Data::ExponentialHistogram(_)) => "exp_histogram",
                        Some(metric::Data::Summary(_)) => "summary",
                        None => "unknown",
                    };
                    MetricRow {
                        name: m.name.clone(),
                        metric_type: metric_type.to_string(),
                        service_name: service_name.clone(),
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

pub async fn run(
    store: SharedStore,
    event_rx: broadcast::Receiver<StoreEvent>,
) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
        original_hook(panic_info);
    }));

    let app = App::new(store, event_rx);
    let result = app.run(&mut terminal).await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}
