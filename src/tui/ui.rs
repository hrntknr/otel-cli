use ratatui::prelude::*;
use ratatui::widgets::*;

use super::tabs::Tab;
use super::{App, LogRow};

pub fn draw(frame: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Tab bar
            Constraint::Min(0),   // Content area
            Constraint::Length(1), // Status bar
        ])
        .split(frame.area());

    draw_tabs(frame, chunks[0], app.current_tab);

    match app.current_tab {
        Tab::Traces => draw_traces_table(frame, chunks[1], app),
        Tab::Logs => draw_logs_split(frame, chunks[1], app),
        Tab::Metrics => draw_metrics_table(frame, chunks[1], app),
    }

    draw_status_bar(frame, chunks[2], app);
}

fn draw_tabs(frame: &mut Frame, area: Rect, current_tab: Tab) {
    let titles: Vec<&str> = Tab::all().iter().map(|t| t.title()).collect();
    let selected = Tab::all()
        .iter()
        .position(|t| *t == current_tab)
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

fn draw_traces_table(frame: &mut Frame, area: Rect, app: &mut App) {
    let header = Row::new(vec!["Trace ID", "Service", "Span Name", "Duration"])
        .style(Style::default().bold())
        .bottom_margin(1);

    let rows: Vec<Row> = app
        .traces_data
        .iter()
        .map(|t| {
            let id_display = if t.trace_id.len() > 16 {
                &t.trace_id[..16]
            } else {
                &t.trace_id
            };
            Row::new(vec![
                id_display.to_string(),
                t.service_name.clone(),
                t.span_name.clone(),
                t.duration.clone(),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(16),
        Constraint::Percentage(25),
        Constraint::Percentage(40),
        Constraint::Length(12),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Traces"))
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(table, area, &mut app.table_state);
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
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(table, area, &mut app.table_state);
}

fn draw_logs_split(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.table_state.selected().is_some() {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
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
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    frame.render_stateful_widget(table, area, &mut app.table_state);
}

fn draw_status_bar(frame: &mut Frame, area: Rect, app: &App) {
    let status = if app.current_tab == Tab::Logs {
        let follow_str = if app.follow { "ON" } else { "OFF" };
        format!(
            "Traces: {} | Logs: {} | Metrics: {} | [f]ollow:{} | q:Quit Tab:Switch j/k:Navigate",
            app.trace_count,
            app.log_count,
            app.metric_count,
            follow_str,
        )
    } else {
        format!(
            "Traces: {} | Logs: {} | Metrics: {} | q:Quit Tab:Switch j/k:Navigate",
            app.trace_count, app.log_count, app.metric_count
        )
    };
    let paragraph =
        Paragraph::new(status).style(Style::default().fg(Color::Black).bg(Color::White));
    frame.render_widget(paragraph, area);
}
