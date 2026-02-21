use ratatui::prelude::*;
use ratatui::widgets::*;

use super::tabs::Tab;
use super::{App, LogRow, TraceView};

pub fn draw(frame: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Tab bar
            Constraint::Min(0),   // Content area
            Constraint::Length(1), // Status bar
        ])
        .split(frame.area());

    draw_tabs(frame, chunks[0], app);

    app.content_area = chunks[1];
    // borders(2) + header(1) + header margin(1) = 4
    app.page_size = chunks[1].height.saturating_sub(4) as usize;

    match app.current_tab {
        Tab::Traces => match app.trace_view {
            TraceView::List => draw_traces_list(frame, chunks[1], app),
            TraceView::Timeline(_) => draw_traces_timeline(frame, chunks[1], app),
        },
        Tab::Logs => draw_logs_split(frame, chunks[1], app),
        Tab::Metrics => draw_metrics_table(frame, chunks[1], app),
    }

    draw_status_bar(frame, chunks[2], app);
}

fn draw_tabs(frame: &mut Frame, area: Rect, app: &App) {
    let titles: Vec<String> = Tab::all()
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let count = match t {
                Tab::Logs => app.log_count,
                Tab::Traces => app.trace_count,
                Tab::Metrics => app.metric_count,
            };
            format!("{}:{}({})", i + 1, t.title(), count)
        })
        .collect();
    let selected = Tab::all()
        .iter()
        .position(|t| *t == app.current_tab)
        .unwrap_or(0);

    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title("OTLP Viewer"))
        .select(selected)
        .highlight_style(Style::default().bold().fg(Color::Yellow));

    frame.render_widget(tabs, area);
}

fn severity_color(severity: &str) -> Color {
    match severity.to_ascii_uppercase().as_str() {
        "TRACE" | "TRACE2" | "TRACE3" | "TRACE4" => Color::DarkGray,
        "DEBUG" | "DEBUG2" | "DEBUG3" | "DEBUG4" => Color::Cyan,
        "INFO" | "INFO2" | "INFO3" | "INFO4" => Color::Green,
        "WARN" | "WARNING" => Color::Yellow,
        "ERROR" | "ERROR2" | "ERROR3" | "ERROR4" => Color::Red,
        "FATAL" => Color::Magenta,
        _ => Color::White,
    }
}

fn draw_traces_list(frame: &mut Frame, area: Rect, app: &mut App) {
    let header = Row::new(vec!["Trace ID", "Service", "Root Span", "Spans", "Duration"])
        .style(Style::default().bold())
        .bottom_margin(1);

    let rows: Vec<Row> = app
        .trace_summaries
        .iter()
        .map(|t| {
            let id_display = if t.trace_id.len() > 16 {
                &t.trace_id[..16]
            } else {
                &t.trace_id
            };
            Row::new(vec![
                id_display.to_string(),
                t.root_service.clone(),
                t.root_span_name.clone(),
                t.span_count.to_string(),
                t.duration.clone(),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(16),
        Constraint::Percentage(20),
        Constraint::Percentage(35),
        Constraint::Length(6),
        Constraint::Length(12),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Traces"))
        .row_highlight_style(Style::default().bg(Color::DarkGray))
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(table, area, &mut app.table_state);
}

const SERVICE_COLORS: [Color; 6] = [
    Color::Cyan,
    Color::Green,
    Color::Yellow,
    Color::Blue,
    Color::Magenta,
    Color::LightCyan,
];

fn draw_traces_timeline(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.timeline_spans.is_empty() {
        let block = Block::default()
            .borders(Borders::ALL)
            .title("Timeline (no spans)");
        frame.render_widget(block, area);
        return;
    }

    let trace_min = app.timeline_spans.iter().map(|s| s.start_ns).min().unwrap_or(0);
    let trace_max = app.timeline_spans.iter().map(|s| s.end_ns).max().unwrap_or(0);
    let trace_range = trace_max.saturating_sub(trace_min).max(1) as f64;

    // Assign colors to services
    let mut service_colors: std::collections::HashMap<&str, Color> = std::collections::HashMap::new();
    let mut color_idx = 0;
    for span in &app.timeline_spans {
        if !service_colors.contains_key(span.service_name.as_str()) {
            service_colors.insert(&span.service_name, SERVICE_COLORS[color_idx % SERVICE_COLORS.len()]);
            color_idx += 1;
        }
    }

    // Calculate waterfall bar width (area minus borders, other columns, and spacing)
    let waterfall_width = area.width.saturating_sub(2 + 40 + 15 + 12 + 6) as usize;

    let header = Row::new(vec!["Span", "Service", "Duration", "Waterfall"])
        .style(Style::default().bold())
        .bottom_margin(1);

    let rows: Vec<Row> = app
        .timeline_spans
        .iter()
        .map(|s| {
            let indent = "  ".repeat(s.depth);
            let prefix = if s.depth > 0 { "|- " } else { "" };
            let span_label = format!("{}{}{}", indent, prefix, s.span_name);

            let color = if s.status_code == 2 {
                Color::Red
            } else {
                service_colors.get(s.service_name.as_str()).copied().unwrap_or(Color::White)
            };

            // Build waterfall bar
            let bar = if waterfall_width > 0 {
                let start_offset =
                    ((s.start_ns.saturating_sub(trace_min)) as f64 / trace_range * waterfall_width as f64) as usize;
                let bar_len = ((s.end_ns.saturating_sub(s.start_ns)) as f64 / trace_range
                    * waterfall_width as f64)
                    .ceil() as usize;
                let bar_len = bar_len.max(1).min(waterfall_width.saturating_sub(start_offset));
                let start_offset = start_offset.min(waterfall_width.saturating_sub(1));
                format!(
                    "{}{}",
                    " ".repeat(start_offset),
                    "\u{2588}".repeat(bar_len)
                )
            } else {
                String::new()
            };

            Row::new(vec![
                Cell::from(Span::styled(span_label, Style::default().fg(color))),
                Cell::from(Span::styled(
                    s.service_name.clone(),
                    Style::default().fg(color),
                )),
                Cell::from(s.duration.clone()),
                Cell::from(Span::styled(bar, Style::default().fg(color))),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(40),
        Constraint::Length(15),
        Constraint::Length(12),
        Constraint::Min(0),
    ];

    let title = match &app.trace_view {
        TraceView::Timeline(id) => {
            let short_id = if id.len() > 16 { &id[..16] } else { id };
            format!("Timeline [{}...]", short_id)
        }
        _ => "Timeline".to_string(),
    };

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .row_highlight_style(Style::default().bg(Color::DarkGray))
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(table, area, &mut app.timeline_table_state);
}

fn log_row_cells(l: &LogRow) -> Vec<Cell<'static>> {
    vec![
        Cell::from(l.timestamp.clone()),
        Cell::from(Span::styled(
            l.severity.clone(),
            Style::default().fg(severity_color(&l.severity)),
        )),
        Cell::from(l.body.clone()),
    ]
}

fn draw_logs_table_basic(frame: &mut Frame, area: Rect, app: &mut App) {
    let header = Row::new(vec!["Timestamp", "Severity", "Body"])
        .style(Style::default().bold())
        .bottom_margin(1);

    let rows: Vec<Row> = app
        .logs_data
        .iter()
        .map(|l| Row::new(log_row_cells(l)))
        .collect();

    let widths = [
        Constraint::Length(30),
        Constraint::Length(10),
        Constraint::Min(0),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Logs"))
        .row_highlight_style(Style::default().bg(Color::DarkGray))
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(table, area, &mut app.table_state);
}

fn draw_logs_split(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.table_state.selected().is_some() {
        let left = 100 - app.detail_panel_percent;
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(left),
                Constraint::Percentage(app.detail_panel_percent),
            ])
            .split(area);

        draw_logs_table_basic(frame, chunks[0], app);

        if let Some(idx) = app.table_state.selected() {
            if let Some(log) = app.logs_data.get(idx) {
                draw_detail_panel(frame, chunks[1], log);
            }
        }
    } else {
        draw_logs_table_basic(frame, area, app);
    }
}

fn draw_detail_panel(frame: &mut Frame, area: Rect, log: &LogRow) {
    let lines = build_detail_lines(log);
    let text = Text::from(lines);

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Detail")
        .border_style(Style::default().fg(Color::Cyan));

    let paragraph = Paragraph::new(text).block(block).wrap(Wrap { trim: false });

    frame.render_widget(paragraph, area);
}

fn build_detail_lines(log: &LogRow) -> Vec<Line<'static>> {
    let section_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let key_style = Style::default().fg(Color::DarkGray);

    let mut lines: Vec<Line<'static>> = Vec::new();

    // Metadata
    lines.push(Line::from(Span::styled("Metadata", section_style)));
    lines.push(Line::from(vec![
        Span::styled("  Scope:      ", key_style),
        Span::raw(log.service_name.clone()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Observed:   ", key_style),
        Span::raw(log.timestamp.clone()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  SevNumber:  ", key_style),
        Span::raw(log.severity_number.to_string()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Body:       ", key_style),
        Span::raw(log.body.clone()),
    ]));
    if !log.trace_id.is_empty() && log.trace_id != "00000000000000000000000000000000" {
        lines.push(Line::from(vec![
            Span::styled("  Trace ID:   ", key_style),
            Span::raw(log.trace_id.clone()),
        ]));
    }
    if !log.span_id.is_empty() && log.span_id != "0000000000000000" {
        lines.push(Line::from(vec![
            Span::styled("  Span ID:    ", key_style),
            Span::raw(log.span_id.clone()),
        ]));
    }

    // Attributes
    if !log.attributes.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("Attributes", section_style)));
        for (k, v) in &log.attributes {
            lines.push(Line::from(vec![
                Span::styled(format!("  {}: ", k), key_style),
                Span::raw(v.clone()),
            ]));
        }
    }

    // Resource Attributes
    if !log.resource_attributes.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Resource Attributes",
            section_style,
        )));
        for (k, v) in &log.resource_attributes {
            lines.push(Line::from(vec![
                Span::styled(format!("  {}: ", k), key_style),
                Span::raw(v.clone()),
            ]));
        }
    }

    lines
}

fn draw_metrics_table(frame: &mut Frame, area: Rect, app: &mut App) {
    let header = Row::new(vec!["Name", "Type", "Service"])
        .style(Style::default().bold())
        .bottom_margin(1);

    let rows: Vec<Row> = app
        .metrics_data
        .iter()
        .map(|m| {
            Row::new(vec![
                m.name.clone(),
                m.metric_type.clone(),
                m.service_name.clone(),
            ])
        })
        .collect();

    let widths = [
        Constraint::Percentage(40),
        Constraint::Length(15),
        Constraint::Percentage(30),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Metrics"))
        .row_highlight_style(Style::default().bg(Color::DarkGray))
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(table, area, &mut app.table_state);
}

fn draw_status_bar(frame: &mut Frame, area: Rect, app: &App) {
    let status = match app.current_tab {
        Tab::Logs => {
            let follow_str = if app.follow { "ON" } else { "OFF" };
            format!("[f]ollow:{} | c:Clear | q:Quit", follow_str)
        }
        Tab::Traces => {
            let follow_str = if app.follow { "ON" } else { "OFF" };
            match app.trace_view {
                TraceView::List => {
                    format!("Enter:Open | [f]ollow:{} | c:Clear | q:Quit", follow_str)
                }
                TraceView::Timeline(_) => {
                    format!("Esc:Back | [f]ollow:{} | c:Clear | q:Quit", follow_str)
                }
            }
        }
        _ => "c:Clear | q:Quit".to_string(),
    };
    let paragraph =
        Paragraph::new(status).style(Style::default().fg(Color::Black).bg(Color::White));
    frame.render_widget(paragraph, area);
}
