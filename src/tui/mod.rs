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

use crate::client;
use crate::proto::opentelemetry::proto::{
    logs::v1::ResourceLogs,
    metrics::v1::{metric, ResourceMetrics},
    trace::v1::ResourceSpans,
};
use crate::store::{SharedStore, StoreEvent};

pub struct TraceRow {
    pub trace_id: String,
    pub service_name: String,
    pub span_name: String,
    pub duration: String,
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
    pub traces_data: Vec<TraceRow>,
    pub logs_data: Vec<LogRow>,
    pub metrics_data: Vec<MetricRow>,
    pub trace_count: usize,
    pub log_count: usize,
    pub metric_count: usize,
    pub follow: bool,
    should_quit: bool,
}

impl App {
    fn new(store: SharedStore, event_rx: broadcast::Receiver<StoreEvent>) -> Self {
        Self {
            store,
            event_handler: event::EventHandler::new(event_rx),
            current_tab: tabs::Tab::Logs,
            table_state: TableState::default(),
            traces_data: Vec::new(),
            logs_data: Vec::new(),
            metrics_data: Vec::new(),
            trace_count: 0,
            log_count: 0,
            metric_count: 0,
            follow: true,
            should_quit: false,
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
            }
            KeyCode::BackTab => {
                self.current_tab = self.current_tab.prev();
                self.table_state = TableState::default();
            }
            KeyCode::Char('1') => {
                self.current_tab = tabs::Tab::Logs;
                self.table_state = TableState::default();
            }
            KeyCode::Char('2') => {
                self.current_tab = tabs::Tab::Traces;
                self.table_state = TableState::default();
            }
            KeyCode::Char('3') => {
                self.current_tab = tabs::Tab::Metrics;
                self.table_state = TableState::default();
            }
            KeyCode::Down | KeyCode::Char('j') => self.select_next(),
            KeyCode::Up | KeyCode::Char('k') => self.select_prev(),
            KeyCode::Char('f') => {
                if self.current_tab == tabs::Tab::Logs {
                    self.follow = !self.follow;
                    if self.follow && !self.logs_data.is_empty() {
                        self.table_state
                            .select(Some(self.logs_data.len() - 1));
                    }
                }
            }
            KeyCode::Esc => {
                self.table_state.select(None);
            }
            _ => {}
        }
    }

    fn current_list_len(&self) -> usize {
        match self.current_tab {
            tabs::Tab::Traces => self.traces_data.len(),
            tabs::Tab::Logs => self.logs_data.len(),
            tabs::Tab::Metrics => self.metrics_data.len(),
        }
    }

    fn select_next(&mut self) {
        let len = self.current_list_len();
        if len == 0 {
            return;
        }
        let i = self
            .table_state
            .selected()
            .map(|i| (i + 1) % len)
            .unwrap_or(0);
        self.table_state.select(Some(i));
        if self.current_tab == tabs::Tab::Logs && i == len - 1 {
            self.follow = true;
        }
    }

    fn select_prev(&mut self) {
        let len = self.current_list_len();
        if len == 0 {
            return;
        }
        let i = self
            .table_state
            .selected()
            .map(|i| if i == 0 { len - 1 } else { i - 1 })
            .unwrap_or(0);
        self.table_state.select(Some(i));
        if self.current_tab == tabs::Tab::Logs {
            self.follow = false;
        }
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

        self.traces_data = traces
            .into_iter()
            .flat_map(|rs| convert_trace_rows(&rs))
            .collect();
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

fn convert_trace_rows(rs: &ResourceSpans) -> Vec<TraceRow> {
    let service_name = client::get_service_name(&rs.resource);
    rs.scope_spans
        .iter()
        .flat_map(|ss| {
            ss.spans
                .iter()
                .map(|span| {
                    let duration_ns =
                        span.end_time_unix_nano.saturating_sub(span.start_time_unix_nano);
                    let duration = if duration_ns >= 1_000_000_000 {
                        format!("{:.2}s", duration_ns as f64 / 1_000_000_000.0)
                    } else if duration_ns >= 1_000_000 {
                        format!("{:.2}ms", duration_ns as f64 / 1_000_000.0)
                    } else if duration_ns >= 1_000 {
                        format!("{:.2}us", duration_ns as f64 / 1_000.0)
                    } else {
                        format!("{}ns", duration_ns)
                    };
                    TraceRow {
                        trace_id: client::hex_encode(&span.trace_id),
                        service_name: service_name.clone(),
                        span_name: span.name.clone(),
                        duration,
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect()
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
