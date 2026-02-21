pub mod event;
pub mod tabs;
pub mod ui;

use std::io;

use crossterm::{
    event::{
        DisableMouseCapture, EnableMouseCapture, KeyCode, KeyModifiers, MouseEventKind,
    },
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
use crate::store::{
    FilterCondition, FilterOperator, LogFilter, SeverityCondition, SharedStore, StoreEvent,
};

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

#[derive(Clone)]
pub enum FilterSection {
    Attribute,
    ResourceAttribute,
}

pub const SEVERITY_LEVELS: &[&str] = &["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"];

pub enum FilterPopupMode {
    /// Main list: 3 sections of filter conditions
    List { selected: usize },
    /// Severity level picker
    SelectSeverity { selected: usize },
    /// Field selection (for Attribute/ResourceAttribute)
    SelectField {
        section: FilterSection,
        candidates: Vec<String>,
        selected: usize,
        input: String,
    },
    /// Operator selection
    SelectOperator {
        section: FilterSection,
        field: String,
        selected: usize,
    },
    /// Value input
    InputValue {
        section: FilterSection,
        field: String,
        operator: FilterOperator,
        value: String,
    },
}

pub struct LogFilterPopup {
    pub mode: FilterPopupMode,
    pub severity: Option<SeverityCondition>,
    pub attribute_conditions: Vec<FilterCondition>,
    pub resource_conditions: Vec<FilterCondition>,
}

impl LogFilterPopup {
    /// Total items in the list view
    pub fn list_item_count(&self) -> usize {
        filter_list_item_count(
            self.attribute_conditions.len(),
            self.resource_conditions.len(),
        )
    }

    /// Determine what kind of item is at a given list index
    pub fn item_at(&self, idx: usize) -> ListItem {
        filter_item_at(
            idx,
            self.attribute_conditions.len(),
            self.resource_conditions.len(),
        )
    }
}

/// Free function to compute list item count (avoids borrow issues)
fn filter_list_item_count(na: usize, nr: usize) -> usize {
    // Severity + attrs + [+]Add Attr + resource attrs + [+]Add Resource + [Apply]
    1 + na + 1 + nr + 1 + 1
}

/// Free function to determine item at given index (avoids borrow issues)
fn filter_item_at(idx: usize, na: usize, nr: usize) -> ListItem {
    if idx == 0 {
        ListItem::Severity
    } else if idx <= na {
        ListItem::AttributeCondition(idx - 1)
    } else if idx == na + 1 {
        ListItem::AddAttribute
    } else if idx <= na + 1 + nr {
        ListItem::ResourceCondition(idx - na - 2)
    } else if idx == na + nr + 2 {
        ListItem::AddResourceAttribute
    } else {
        ListItem::Apply
    }
}

pub enum ListItem {
    Severity,
    AttributeCondition(usize),
    AddAttribute,
    ResourceCondition(usize),
    AddResourceAttribute,
    Apply,
}

pub const ALL_OPERATORS: &[FilterOperator] = &[
    FilterOperator::Eq,
    FilterOperator::Contains,
    FilterOperator::NotEq,
    FilterOperator::NotContains,
];

pub fn operator_symbol(op: &FilterOperator) -> &'static str {
    match op {
        FilterOperator::Eq => "=",
        FilterOperator::Contains => "\u{2283}",
        FilterOperator::NotEq => "\u{2260}",
        FilterOperator::NotContains => "\u{2285}",
        FilterOperator::Ge => ">=",
        FilterOperator::Gt => ">",
        FilterOperator::Le => "<=",
        FilterOperator::Lt => "<",
    }
}

pub fn operator_label(op: &FilterOperator) -> &'static str {
    match op {
        FilterOperator::Eq => "equals",
        FilterOperator::Contains => "contains",
        FilterOperator::NotEq => "not equals",
        FilterOperator::NotContains => "not contains",
        FilterOperator::Ge => "greater or equal",
        FilterOperator::Gt => "greater than",
        FilterOperator::Le => "less or equal",
        FilterOperator::Lt => "less than",
    }
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
    pub log_detail_open: bool,
    should_quit: bool,
    pending_clear: bool,
    pub trace_view: TraceView,
    pub trace_summaries: Vec<TraceSummary>,
    pub timeline_spans: Vec<TimelineSpan>,
    pub timeline_table_state: TableState,
    raw_traces: Vec<ResourceSpans>,
    pub detail_panel_percent: u16,
    pub content_area: ratatui::layout::Rect,
    dragging_split: bool,
    pub log_filter_popup: Option<LogFilterPopup>,
    pub log_filter: LogFilter,
    pub available_log_fields: Vec<String>,
    pub available_resource_fields: Vec<String>,
    pending_refresh: bool,
    pub search_input: Option<String>,
    pub log_search: String,
    pub trace_search: String,
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
            log_detail_open: true,
            should_quit: false,
            pending_clear: false,
            trace_view: TraceView::default(),
            trace_summaries: Vec::new(),
            timeline_spans: Vec::new(),
            timeline_table_state: TableState::default(),
            raw_traces: Vec::new(),
            detail_panel_percent: 40,
            content_area: ratatui::layout::Rect::default(),
            dragging_split: false,
            log_filter_popup: None,
            log_filter: LogFilter {
                severity: Some(SeverityCondition {
                    operator: FilterOperator::Ge,
                    value: "INFO".to_string(),
                }),
                ..LogFilter::default()
            },
            available_log_fields: Vec::new(),
            available_resource_fields: Vec::new(),
            pending_refresh: false,
            search_input: None,
            log_search: String::new(),
            trace_search: String::new(),
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
                event::AppEvent::Mouse(mouse) => self.handle_mouse(mouse),
                event::AppEvent::Resize => {}
                event::AppEvent::StoreUpdate => self.refresh_data().await,
                event::AppEvent::Tick => {}
            }

            if self.pending_clear {
                self.pending_clear = false;
                self.clear_current_tab().await;
            }

            if self.pending_refresh {
                self.pending_refresh = false;
                self.refresh_data().await;
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

        // Route to search input if active
        if let Some(ref mut input) = self.search_input {
            match key.code {
                KeyCode::Char(c) => input.push(c),
                KeyCode::Backspace => {
                    input.pop();
                }
                KeyCode::Enter => {
                    let text = self.search_input.take().unwrap();
                    match self.current_tab {
                        tabs::Tab::Logs => self.log_search = text,
                        tabs::Tab::Traces => self.trace_search = text,
                        _ => {}
                    }
                    self.pending_refresh = true;
                }
                KeyCode::Esc => {
                    self.search_input = None;
                }
                _ => {}
            }
            return;
        }

        // Route to popup if open
        if self.log_filter_popup.is_some() {
            self.handle_filter_popup_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
            }
            KeyCode::Tab => {
                self.switch_tab(self.current_tab.next());
            }
            KeyCode::BackTab => {
                self.switch_tab(self.current_tab.prev());
            }
            KeyCode::Char('1') => {
                self.switch_tab(tabs::Tab::Logs);
            }
            KeyCode::Char('2') => {
                self.switch_tab(tabs::Tab::Traces);
            }
            KeyCode::Char('3') => {
                self.switch_tab(tabs::Tab::Metrics);
            }
            KeyCode::Down | KeyCode::Char('j') => self.select_next(),
            KeyCode::Up | KeyCode::Char('k') => self.select_prev(),
            KeyCode::PageDown | KeyCode::Char(' ') => self.select_next_page(),
            KeyCode::PageUp => self.select_prev_page(),
            KeyCode::Char('f') => {
                if matches!(self.current_tab, tabs::Tab::Logs | tabs::Tab::Traces) {
                    self.follow = !self.follow;
                    if self.follow {
                        self.follow_to_latest();
                    }
                }
            }
            KeyCode::Enter => {
                if self.current_tab == tabs::Tab::Logs {
                    self.log_detail_open = true;
                } else if self.current_tab == tabs::Tab::Traces {
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
            KeyCode::Char('/') => {
                if matches!(self.current_tab, tabs::Tab::Logs | tabs::Tab::Traces) {
                    self.search_input = Some(String::new());
                }
            }
            KeyCode::F(4) => {
                if self.current_tab == tabs::Tab::Logs {
                    self.open_filter_popup();
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
                if self.current_tab == tabs::Tab::Logs {
                    self.log_detail_open = false;
                }
            }
            _ => {}
        }
    }

    fn open_filter_popup(&mut self) {
        self.log_filter_popup = Some(LogFilterPopup {
            mode: FilterPopupMode::List { selected: 0 },
            severity: self.log_filter.severity.clone(),
            attribute_conditions: self.log_filter.attribute_conditions.clone(),
            resource_conditions: self.log_filter.resource_conditions.clone(),
        });
    }

    fn apply_filter_popup(&mut self) {
        if let Some(popup) = self.log_filter_popup.take() {
            self.log_filter = LogFilter {
                severity: popup.severity,
                attribute_conditions: popup.attribute_conditions,
                resource_conditions: popup.resource_conditions,
            };
            self.pending_refresh = true;
        }
    }

    pub fn log_filter_condition_count(&self) -> usize {
        let mut count = 0;
        if !self.log_search.is_empty() {
            count += 1;
        }
        if self.log_filter.severity.is_some() {
            count += 1;
        }
        count += self.log_filter.attribute_conditions.len();
        count += self.log_filter.resource_conditions.len();
        count
    }

    fn handle_filter_popup_key(&mut self, key: crossterm::event::KeyEvent) {
        let popup = self.log_filter_popup.as_mut().unwrap();
        match &mut popup.mode {
            FilterPopupMode::List { selected } => {
                let na = popup.attribute_conditions.len();
                let nr = popup.resource_conditions.len();
                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        if *selected > 0 {
                            *selected -= 1;
                        }
                    }
                    KeyCode::Down | KeyCode::Char('j') | KeyCode::Tab => {
                        let max = filter_list_item_count(na, nr).saturating_sub(1);
                        if *selected < max {
                            *selected += 1;
                        }
                    }
                    KeyCode::BackTab => {
                        if *selected > 0 {
                            *selected -= 1;
                        }
                    }
                    KeyCode::Enter => {
                        let sel = *selected;
                        match filter_item_at(sel, na, nr) {
                            ListItem::Severity => {
                                let current_idx = popup
                                    .severity
                                    .as_ref()
                                    .and_then(|sev| {
                                        SEVERITY_LEVELS
                                            .iter()
                                            .position(|l| l.eq_ignore_ascii_case(&sev.value))
                                    })
                                    .unwrap_or(0);
                                popup.mode = FilterPopupMode::SelectSeverity {
                                    selected: current_idx,
                                };
                            }
                            ListItem::AttributeCondition(i) => {
                                let cond = popup.attribute_conditions[i].clone();
                                popup.attribute_conditions.remove(i);
                                let candidates = self.available_log_fields.clone();
                                popup.mode = FilterPopupMode::SelectField {
                                    section: FilterSection::Attribute,
                                    candidates,
                                    selected: 0,
                                    input: cond.field,
                                };
                            }
                            ListItem::AddAttribute => {
                                let candidates = self.available_log_fields.clone();
                                popup.mode = FilterPopupMode::SelectField {
                                    section: FilterSection::Attribute,
                                    candidates,
                                    selected: 0,
                                    input: String::new(),
                                };
                            }
                            ListItem::ResourceCondition(i) => {
                                let cond = popup.resource_conditions[i].clone();
                                popup.resource_conditions.remove(i);
                                let candidates = self.available_resource_fields.clone();
                                popup.mode = FilterPopupMode::SelectField {
                                    section: FilterSection::ResourceAttribute,
                                    candidates,
                                    selected: 0,
                                    input: cond.field,
                                };
                            }
                            ListItem::AddResourceAttribute => {
                                let candidates = self.available_resource_fields.clone();
                                popup.mode = FilterPopupMode::SelectField {
                                    section: FilterSection::ResourceAttribute,
                                    candidates,
                                    selected: 0,
                                    input: String::new(),
                                };
                            }
                            ListItem::Apply => {
                                self.apply_filter_popup();
                            }
                        }
                    }
                    KeyCode::Char('d') | KeyCode::Delete => {
                        let sel = *selected;
                        match filter_item_at(sel, na, nr) {
                            ListItem::Severity => {
                                popup.severity = None;
                            }
                            ListItem::AttributeCondition(i) => {
                                popup.attribute_conditions.remove(i);
                                let max = filter_list_item_count(na - 1, nr)
                                    .saturating_sub(1);
                                if *selected > max {
                                    *selected = max;
                                }
                            }
                            ListItem::ResourceCondition(i) => {
                                popup.resource_conditions.remove(i);
                                let max = filter_list_item_count(na, nr - 1)
                                    .saturating_sub(1);
                                if *selected > max {
                                    *selected = max;
                                }
                            }
                            _ => {}
                        }
                    }
                    KeyCode::Esc => {
                        self.log_filter_popup = None;
                    }
                    _ => {}
                }
            }
            FilterPopupMode::SelectSeverity { selected } => match key.code {
                KeyCode::Up | KeyCode::Char('k') => {
                    if *selected > 0 {
                        *selected -= 1;
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if *selected < SEVERITY_LEVELS.len() - 1 {
                        *selected += 1;
                    }
                }
                KeyCode::Enter => {
                    let level = SEVERITY_LEVELS[*selected].to_string();
                    let popup = self.log_filter_popup.as_mut().unwrap();
                    popup.severity = Some(SeverityCondition {
                        operator: FilterOperator::Ge,
                        value: level,
                    });
                    popup.mode = FilterPopupMode::List { selected: 0 };
                }
                KeyCode::Esc => {
                    let popup = self.log_filter_popup.as_mut().unwrap();
                    popup.mode = FilterPopupMode::List { selected: 0 };
                }
                _ => {}
            },
            FilterPopupMode::SelectField {
                section,
                candidates,
                selected,
                input,
            } => match key.code {
                KeyCode::Char(c) => {
                    input.push(c);
                    *selected = 0;
                }
                KeyCode::Backspace => {
                    input.pop();
                    *selected = 0;
                }
                KeyCode::Up => {
                    if *selected > 0 {
                        *selected -= 1;
                    }
                }
                KeyCode::Down => {
                    let filtered: Vec<_> = candidates
                        .iter()
                        .filter(|c| {
                            c.to_ascii_lowercase()
                                .contains(&input.to_ascii_lowercase())
                        })
                        .collect();
                    let max = filtered.len().saturating_sub(1);
                    if *selected < max {
                        *selected += 1;
                    }
                }
                KeyCode::Enter => {
                    let filtered: Vec<_> = candidates
                        .iter()
                        .filter(|c| {
                            c.to_ascii_lowercase()
                                .contains(&input.to_ascii_lowercase())
                        })
                        .cloned()
                        .collect();
                    let field = if let Some(f) = filtered.get(*selected) {
                        f.clone()
                    } else if !input.is_empty() {
                        input.clone()
                    } else {
                        return;
                    };
                    let sec = section.clone();
                    let popup = self.log_filter_popup.as_mut().unwrap();
                    popup.mode = FilterPopupMode::SelectOperator {
                        section: sec,
                        field,
                        selected: 0,
                    };
                }
                KeyCode::Esc => {
                    let popup = self.log_filter_popup.as_mut().unwrap();
                    popup.mode = FilterPopupMode::List { selected: 0 };
                }
                _ => {}
            },
            FilterPopupMode::SelectOperator {
                section,
                field,
                selected,
            } => match key.code {
                KeyCode::Up => {
                    if *selected > 0 {
                        *selected -= 1;
                    }
                }
                KeyCode::Down => {
                    let max = ALL_OPERATORS.len().saturating_sub(1);
                    if *selected < max {
                        *selected += 1;
                    }
                }
                KeyCode::Enter => {
                    let op = ALL_OPERATORS[*selected].clone();
                    let sec = section.clone();
                    let f = field.clone();
                    let popup = self.log_filter_popup.as_mut().unwrap();
                    popup.mode = FilterPopupMode::InputValue {
                        section: sec,
                        field: f,
                        operator: op,
                        value: String::new(),
                    };
                }
                KeyCode::Esc => {
                    let sec = section.clone();
                    let f = field.clone();
                    let candidates = match sec {
                        FilterSection::Attribute => self.available_log_fields.clone(),
                        FilterSection::ResourceAttribute => {
                            self.available_resource_fields.clone()
                        }
                    };
                    let popup = self.log_filter_popup.as_mut().unwrap();
                    popup.mode = FilterPopupMode::SelectField {
                        section: sec,
                        candidates,
                        selected: 0,
                        input: f,
                    };
                }
                _ => {}
            },
            FilterPopupMode::InputValue {
                section,
                field,
                operator,
                value,
            } => match key.code {
                KeyCode::Char(c) => {
                    value.push(c);
                }
                KeyCode::Backspace => {
                    value.pop();
                }
                KeyCode::Enter => {
                    if value.is_empty() {
                        return;
                    }
                    let cond = FilterCondition {
                        field: field.clone(),
                        operator: operator.clone(),
                        value: value.clone(),
                    };
                    let sec = section.clone();
                    let popup = self.log_filter_popup.as_mut().unwrap();
                    match sec {
                        FilterSection::Attribute => {
                            popup.attribute_conditions.push(cond);
                        }
                        FilterSection::ResourceAttribute => {
                            popup.resource_conditions.push(cond);
                        }
                    }
                    popup.mode = FilterPopupMode::List { selected: 0 };
                }
                KeyCode::Esc => {
                    let sec = section.clone();
                    let f = field.clone();
                    let popup = self.log_filter_popup.as_mut().unwrap();
                    popup.mode = FilterPopupMode::SelectOperator {
                        section: sec,
                        field: f,
                        selected: 0,
                    };
                }
                _ => {}
            },
        }
    }

    fn handle_mouse(&mut self, mouse: crossterm::event::MouseEvent) {
        let area = self.content_area;
        match mouse.kind {
            MouseEventKind::ScrollDown => {
                if mouse.row >= area.y && mouse.row < area.y + area.height {
                    self.select_next();
                }
            }
            MouseEventKind::ScrollUp => {
                if mouse.row >= area.y && mouse.row < area.y + area.height {
                    self.select_prev();
                }
            }
            MouseEventKind::Down(crossterm::event::MouseButton::Left) => {
                // Check if click is near the split border
                if self.current_tab == tabs::Tab::Logs && self.table_state.selected().is_some() {
                    let split_x =
                        area.x + (area.width * (100 - self.detail_panel_percent) / 100);
                    if mouse.column >= split_x.saturating_sub(1)
                        && mouse.column <= split_x + 1
                        && mouse.row >= area.y
                        && mouse.row < area.y + area.height
                    {
                        self.dragging_split = true;
                    }
                }
            }
            MouseEventKind::Drag(crossterm::event::MouseButton::Left) => {
                if self.dragging_split && area.width > 0 {
                    let relative_x = mouse.column.saturating_sub(area.x);
                    let left_percent = (relative_x as u16 * 100) / area.width;
                    self.detail_panel_percent = left_percent.clamp(20, 80);
                    // detail_panel_percent is the right side, so invert
                    self.detail_panel_percent = 100 - left_percent.clamp(20, 80);
                }
            }
            MouseEventKind::Up(crossterm::event::MouseButton::Left) => {
                self.dragging_split = false;
            }
            _ => {}
        }
    }

    fn switch_tab(&mut self, tab: tabs::Tab) {
        self.current_tab = tab;
        self.table_state = TableState::default();
        self.trace_view = TraceView::List;
        if matches!(tab, tabs::Tab::Logs | tabs::Tab::Traces) {
            self.follow = true;
            self.follow_to_latest();
        }
    }

    fn follow_to_latest(&mut self) {
        let len = self.current_list_len();
        if len > 0 {
            self.active_table_state().select(Some(len - 1));
        }
    }

    fn is_followable_tab(&self) -> bool {
        matches!(self.current_tab, tabs::Tab::Logs | tabs::Tab::Traces)
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
        self.update_follow_on_navigate(i, len);
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
        self.update_follow_on_navigate(i, len);
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
        self.update_follow_on_navigate(i, len);
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
        self.update_follow_on_navigate(i, len);
    }

    fn update_follow_on_navigate(&mut self, i: usize, len: usize) {
        if !self.is_followable_tab() {
            return;
        }
        self.follow = i == len - 1;
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
        self.log_count = store.log_count();
        self.metric_count = store.metric_count();

        let traces = store.query_traces(&Default::default(), 500);

        // Fetch all logs for field collection, then apply filter
        let all_logs = store.query_logs(&LogFilter::default(), 500);
        let filtered_logs = if self.log_filter_condition_count() > 0 {
            store.query_logs(&self.log_filter, 500)
        } else {
            all_logs.clone()
        };

        let metrics = store.query_metrics(&Default::default(), 500);
        drop(store);

        self.raw_traces = traces;
        self.trace_summaries = build_trace_summaries(&self.raw_traces);

        if !self.trace_search.is_empty() {
            let needle = self.trace_search.to_ascii_lowercase();
            self.trace_summaries.retain(|t| {
                let line = format!(
                    "{} {} {} {}",
                    t.trace_id, t.root_service, t.root_span_name, t.duration
                );
                line.to_ascii_lowercase().contains(&needle)
            });
        }

        self.trace_count = self.trace_summaries.len();
        if let TraceView::Timeline(ref trace_id) = self.trace_view {
            self.timeline_spans = build_timeline_spans(&self.raw_traces, trace_id);
        }

        if self.current_tab == tabs::Tab::Traces && self.follow {
            if let TraceView::List = self.trace_view {
                if !self.trace_summaries.is_empty() {
                    self.table_state
                        .select(Some(self.trace_summaries.len() - 1));
                }
            }
        }

        // Collect available field names from all logs
        self.update_available_fields(&all_logs);

        let mut logs_data: Vec<LogRow> = filtered_logs
            .into_iter()
            .flat_map(|rl| convert_log_rows(&rl))
            .collect();

        if !self.log_search.is_empty() {
            let needle = self.log_search.to_ascii_lowercase();
            logs_data.retain(|row| {
                let line = format!("{} {} {}", row.timestamp, row.severity, row.body);
                line.to_ascii_lowercase().contains(&needle)
            });
        }

        self.logs_data = logs_data;

        if self.current_tab == tabs::Tab::Logs && self.follow && !self.logs_data.is_empty() {
            self.table_state
                .select(Some(self.logs_data.len() - 1));
        }

        self.metrics_data = metrics
            .into_iter()
            .flat_map(|rm| convert_metric_rows(&rm))
            .collect();
    }

    fn update_available_fields(&mut self, logs: &[ResourceLogs]) {
        let mut log_fields = std::collections::BTreeSet::new();
        let mut resource_fields = std::collections::BTreeSet::new();

        for rl in logs {
            if let Some(ref resource) = rl.resource {
                for kv in &resource.attributes {
                    resource_fields.insert(kv.key.clone());
                }
            }
            for sl in &rl.scope_logs {
                for lr in &sl.log_records {
                    for kv in &lr.attributes {
                        log_fields.insert(kv.key.clone());
                    }
                }
            }
        }

        self.available_log_fields = log_fields.into_iter().collect();
        self.available_resource_fields = resource_fields.into_iter().collect();
    }
}

fn format_timestamp_time_only(nanos: u64) -> String {
    if nanos == 0 {
        return "N/A".to_string();
    }
    let secs = (nanos / 1_000_000_000) as i64;
    let nsec = (nanos % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nsec) {
        Some(dt) => {
            let local = dt.with_timezone(&chrono::Local);
            local.format("%H:%M:%S%.3f").to_string()
        }
        None => "N/A".to_string(),
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

    summaries.sort_by(|a, b| a.start_time.cmp(&b.start_time));
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
                        timestamp: format_timestamp_time_only(lr.time_unix_nano),
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
