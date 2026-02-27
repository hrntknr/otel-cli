use ratatui::prelude::*;
use ratatui::widgets::*;

use super::tabs::Tab;
use ratatui::symbols;

use super::{
    operator_label, operator_symbol, App, FilterPopupMode, LogFilterPopup, LogRow, MetricGroup,
    MetricView, TraceView, ALL_OPERATORS, SEVERITY_LEVELS,
};

pub fn draw(frame: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Tab bar
            Constraint::Min(0),    // Content area
            Constraint::Length(1), // Status bar
        ])
        .split(frame.area());

    draw_tabs(frame, chunks[0], app);

    app.content_area = chunks[1];
    let old_page_size = app.page_size;
    // borders(2) + header(1) = 3
    app.page_size = chunks[1].height.saturating_sub(3) as usize;

    // Keep bottom-anchored when window grows
    if app.page_size > old_page_size {
        let growth = app.page_size - old_page_size;
        let state = app.active_table_state();
        let off = state.offset();
        *state.offset_mut() = off.saturating_sub(growth);
    }

    match app.current_tab {
        Tab::Traces => match app.trace_view {
            TraceView::List => draw_traces_list(frame, chunks[1], app),
            TraceView::Timeline(_) => draw_traces_timeline(frame, chunks[1], app),
        },
        Tab::Logs => draw_logs_split(frame, chunks[1], app),
        Tab::Metrics => match app.metric_view {
            MetricView::List => draw_metrics_split(frame, chunks[1], app),
            MetricView::Chart(_) => draw_metrics_chart(frame, chunks[1], app),
        },
    }

    draw_status_bar(frame, chunks[2], app);

    if app.log_filter_popup.is_some() {
        draw_filter_popup(frame, frame.area(), app);
    }
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
    let header = Row::new(vec![
        "Trace ID",
        "Service",
        "Root Span",
        "Spans",
        "Duration",
    ])
    .style(Style::default().bold());

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

    let trace_min = app
        .timeline_spans
        .iter()
        .map(|s| s.start_ns)
        .min()
        .unwrap_or(0);
    let trace_max = app
        .timeline_spans
        .iter()
        .map(|s| s.end_ns)
        .max()
        .unwrap_or(0);
    let trace_range = trace_max.saturating_sub(trace_min).max(1) as f64;

    // Assign colors to services
    let mut service_colors: std::collections::HashMap<&str, Color> =
        std::collections::HashMap::new();
    let mut color_idx = 0;
    for span in &app.timeline_spans {
        if !service_colors.contains_key(span.service_name.as_str()) {
            service_colors.insert(
                &span.service_name,
                SERVICE_COLORS[color_idx % SERVICE_COLORS.len()],
            );
            color_idx += 1;
        }
    }

    // Calculate waterfall bar width (area minus borders, other columns, and spacing)
    let waterfall_width = area.width.saturating_sub(2 + 40 + 15 + 12 + 6) as usize;

    let header =
        Row::new(vec!["Span", "Service", "Duration", "Waterfall"]).style(Style::default().bold());

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
                service_colors
                    .get(s.service_name.as_str())
                    .copied()
                    .unwrap_or(Color::White)
            };

            // Build waterfall bar
            let bar = if waterfall_width > 0 {
                let start_offset = ((s.start_ns.saturating_sub(trace_min)) as f64 / trace_range
                    * waterfall_width as f64) as usize;
                let bar_len = ((s.end_ns.saturating_sub(s.start_ns)) as f64 / trace_range
                    * waterfall_width as f64)
                    .ceil() as usize;
                let bar_len = bar_len
                    .max(1)
                    .min(waterfall_width.saturating_sub(start_offset));
                let start_offset = start_offset.min(waterfall_width.saturating_sub(1));
                format!("{}{}", " ".repeat(start_offset), "\u{2588}".repeat(bar_len))
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
    let header = Row::new(vec!["Timestamp", "Severity", "Body"]).style(Style::default().bold());

    let rows: Vec<Row> = app
        .logs_data
        .iter()
        .map(|l| Row::new(log_row_cells(l)))
        .collect();

    let widths = [
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Min(0),
    ];

    let n = app.log_filter_condition_count();
    let title = if n > 0 {
        format!("Logs [filtered: {} conditions]", n)
    } else {
        "Logs".to_string()
    };

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .row_highlight_style(Style::default().bg(Color::DarkGray))
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(table, area, &mut app.table_state);
}

fn draw_logs_split(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.log_detail_open && app.table_state.selected().is_some() {
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

    let mut lines: Vec<Line<'static>> = vec![
        Line::from(Span::styled("Metadata", section_style)),
        Line::from(vec![
            Span::styled("  Scope:      ", key_style),
            Span::raw(log.service_name.clone()),
        ]),
        Line::from(vec![
            Span::styled("  Observed:   ", key_style),
            Span::raw(log.timestamp.clone()),
        ]),
        Line::from(vec![
            Span::styled("  SevNumber:  ", key_style),
            Span::raw(log.severity_number.to_string()),
        ]),
        Line::from(vec![
            Span::styled("  Body:       ", key_style),
            Span::raw(log.body.clone()),
        ]),
    ];
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
    let header = Row::new(vec!["Name", "Type", "Points", "Service"]).style(Style::default().bold());

    let rows: Vec<Row> = app
        .metrics_data
        .iter()
        .map(|m| {
            Row::new(vec![
                m.name.clone(),
                m.metric_type.clone(),
                m.data_points.len().to_string(),
                m.service_names.join(", "),
            ])
        })
        .collect();

    let widths = [
        Constraint::Min(0),
        Constraint::Length(14),
        Constraint::Length(6),
        Constraint::Length(20),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Metrics"))
        .row_highlight_style(Style::default().bg(Color::DarkGray))
        .highlight_symbol("▶ ");

    frame.render_stateful_widget(table, area, &mut app.table_state);
}

fn draw_metrics_split(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.metric_detail_open && app.table_state.selected().is_some() {
        let left = 100 - app.detail_panel_percent;
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(left),
                Constraint::Percentage(app.detail_panel_percent),
            ])
            .split(area);

        draw_metrics_table(frame, chunks[0], app);

        if let Some(idx) = app.table_state.selected() {
            if let Some(group) = app.metrics_data.get(idx) {
                draw_metric_detail_panel(frame, chunks[1], group);
            }
        }
    } else {
        draw_metrics_table(frame, area, app);
    }
}

fn draw_metrics_chart(frame: &mut Frame, area: Rect, app: &App) {
    let title = match &app.metric_view {
        MetricView::Chart(name) => format!("Chart: {}", name),
        _ => "Chart".to_string(),
    };

    if app.chart_series.is_empty() || app.chart_series.iter().all(|s| s.data.is_empty()) {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!("{} (no data)", title));
        frame.render_widget(block, area);
        return;
    }

    let x_min = app
        .chart_series
        .iter()
        .flat_map(|s| s.data.iter().map(|(x, _)| *x))
        .fold(f64::INFINITY, f64::min);
    let x_max = app
        .chart_series
        .iter()
        .flat_map(|s| s.data.iter().map(|(x, _)| *x))
        .fold(f64::NEG_INFINITY, f64::max);
    let y_min = app
        .chart_series
        .iter()
        .flat_map(|s| s.data.iter().map(|(_, y)| *y))
        .fold(f64::INFINITY, f64::min);
    let y_max = app
        .chart_series
        .iter()
        .flat_map(|s| s.data.iter().map(|(_, y)| *y))
        .fold(f64::NEG_INFINITY, f64::max);

    let y_range = y_max - y_min;
    let y_pad = if y_range == 0.0 { 1.0 } else { y_range * 0.1 };
    let y_lo = y_min - y_pad;
    let y_hi = y_max + y_pad;

    // Ensure x_max > x_min for chart rendering
    let x_max = if x_max <= x_min { x_min + 1.0 } else { x_max };

    let datasets: Vec<Dataset> = app
        .chart_series
        .iter()
        .enumerate()
        .map(|(i, series)| {
            Dataset::default()
                .name(series.label.clone())
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(SERVICE_COLORS[i % SERVICE_COLORS.len()]))
                .data(&series.data)
        })
        .collect();

    let x_labels = vec![
        Span::raw(format!("{:.0}s", x_min)),
        Span::raw(format!("{:.0}s", (x_min + x_max) / 2.0)),
        Span::raw(format!("{:.0}s", x_max)),
    ];

    let y_labels = vec![
        Span::raw(format_chart_value(y_lo)),
        Span::raw(format_chart_value((y_lo + y_hi) / 2.0)),
        Span::raw(format_chart_value(y_hi)),
    ];

    let chart = Chart::new(datasets)
        .block(Block::default().borders(Borders::ALL).title(title))
        .x_axis(
            Axis::default()
                .title("Time")
                .bounds([x_min, x_max])
                .labels(x_labels),
        )
        .y_axis(
            Axis::default()
                .title("Value")
                .bounds([y_lo, y_hi])
                .labels(y_labels),
        );

    frame.render_widget(chart, area);
}

fn format_chart_value(v: f64) -> String {
    if v.abs() >= 1_000_000.0 {
        format!("{:.1}M", v / 1_000_000.0)
    } else if v.abs() >= 1_000.0 {
        format!("{:.1}K", v / 1_000.0)
    } else if v.fract() == 0.0 {
        format!("{:.0}", v)
    } else {
        format!("{:.2}", v)
    }
}

fn draw_metric_detail_panel(frame: &mut Frame, area: Rect, group: &MetricGroup) {
    let section_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let key_style = Style::default().fg(Color::DarkGray);

    let mut lines: Vec<Line<'static>> = Vec::new();

    // Metadata
    lines.push(Line::from(Span::styled("Metadata", section_style)));
    lines.push(Line::from(vec![
        Span::styled("  Name:     ", key_style),
        Span::raw(group.name.clone()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("  Type:     ", key_style),
        Span::raw(group.metric_type.clone()),
    ]));
    if !group.description.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("  Desc:     ", key_style),
            Span::raw(group.description.clone()),
        ]));
    }
    if !group.unit.is_empty() {
        lines.push(Line::from(vec![
            Span::styled("  Unit:     ", key_style),
            Span::raw(group.unit.clone()),
        ]));
    }
    lines.push(Line::from(vec![
        Span::styled("  Services: ", key_style),
        Span::raw(group.service_names.join(", ")),
    ]));

    // Data Points
    if !group.data_points.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            format!("Data Points ({})", group.data_points.len()),
            section_style,
        )));
        for dp in group.data_points.iter().rev().take(20) {
            lines.push(Line::from(vec![
                Span::styled("  ", key_style),
                Span::styled(format!("{} ", dp.timestamp), key_style),
                Span::raw(dp.value.clone()),
            ]));
            if !dp.attributes.is_empty() {
                let attrs: Vec<String> = dp
                    .attributes
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect();
                lines.push(Line::from(Span::styled(
                    format!("    {}", attrs.join(" ")),
                    Style::default().fg(Color::DarkGray),
                )));
            }
        }
        if group.data_points.len() > 20 {
            lines.push(Line::from(Span::styled(
                format!("  ... and {} more", group.data_points.len() - 20),
                Style::default().fg(Color::DarkGray),
            )));
        }
    }

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Detail")
        .border_style(Style::default().fg(Color::Cyan));

    let text = Text::from(lines);
    let paragraph = Paragraph::new(text).block(block).wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

fn draw_status_bar(frame: &mut Frame, area: Rect, app: &App) {
    if let Some(ref input) = app.search_input {
        let line = Line::from(vec![
            Span::styled("/", Style::default().fg(Color::Yellow).bg(Color::White)),
            Span::styled(
                format!("{}|", input),
                Style::default().fg(Color::Black).bg(Color::White),
            ),
        ]);
        let paragraph = Paragraph::new(line).style(Style::default().bg(Color::White));
        frame.render_widget(paragraph, area);
        return;
    }

    let status = match app.current_tab {
        Tab::Logs => {
            let follow_str = if app.follow { "ON" } else { "OFF" };
            let search_str = if app.log_search.is_empty() {
                "/:Search".to_string()
            } else {
                format!("/:Search(\"{}\")", app.log_search)
            };
            let filter_n = app.log_filter.severity.as_ref().map_or(0, |_| 1)
                + app.log_filter.attribute_conditions.len()
                + app.log_filter.resource_conditions.len();
            let filter_str = if filter_n > 0 {
                format!("F4:Filter({})", filter_n)
            } else {
                "F4:Filter".to_string()
            };
            let detail_str = if app.log_detail_open {
                "Esc:Close"
            } else {
                "Enter:Open"
            };
            format!(
                "{} | {} | {} | [f]ollow:{} | c:Clear | q:Quit",
                search_str, filter_str, detail_str, follow_str
            )
        }
        Tab::Traces => {
            let follow_str = if app.follow { "ON" } else { "OFF" };
            let search_str = if app.trace_search.is_empty() {
                "/:Search".to_string()
            } else {
                format!("/:Search(\"{}\")", app.trace_search)
            };
            match app.trace_view {
                TraceView::List => {
                    format!(
                        "{} | Enter:Open | [f]ollow:{} | c:Clear | q:Quit",
                        search_str, follow_str
                    )
                }
                TraceView::Timeline(_) => {
                    format!(
                        "{} | Esc:Back | [f]ollow:{} | c:Clear | q:Quit",
                        search_str, follow_str
                    )
                }
            }
        }
        Tab::Metrics => match app.metric_view {
            MetricView::List => {
                format!("Enter:Chart | c:Clear | q:Quit")
            }
            MetricView::Chart(_) => {
                format!("Esc:Back | c:Clear | q:Quit")
            }
        },
    };
    let paragraph =
        Paragraph::new(status).style(Style::default().fg(Color::Black).bg(Color::White));
    frame.render_widget(paragraph, area);
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

fn draw_filter_popup(frame: &mut Frame, area: Rect, app: &App) {
    let popup = app.log_filter_popup.as_ref().unwrap();
    match &popup.mode {
        FilterPopupMode::List { selected } => {
            draw_filter_list(frame, area, popup, *selected);
        }
        FilterPopupMode::SelectSeverity { selected } => {
            draw_select_severity(frame, area, *selected);
        }
        FilterPopupMode::SelectField {
            section: _,
            candidates,
            selected,
            input,
        } => {
            draw_select_field(frame, area, candidates, *selected, input);
        }
        FilterPopupMode::SelectOperator {
            section: _,
            field,
            selected,
        } => {
            draw_select_operator(frame, area, field, *selected);
        }
        FilterPopupMode::InputValue {
            section: _,
            field,
            operator,
            value,
        } => {
            draw_input_value(frame, area, field, operator, value);
        }
    }
}

fn draw_filter_list(frame: &mut Frame, area: Rect, popup: &LogFilterPopup, selected: usize) {
    let highlight = Style::default().bg(Color::DarkGray).fg(Color::White);
    let section_style = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let normal = Style::default();

    let mut lines: Vec<Line> = Vec::new();

    // Severity
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(" Severity", section_style)));
    let sev_text = if let Some(ref sev) = popup.severity {
        format!("   >= {}", sev.value)
    } else {
        "   (not set)".to_string()
    };
    let sev_style = if selected == 0 { highlight } else { normal };
    lines.push(Line::from(Span::styled(sev_text, sev_style)));
    lines.push(Line::from(""));

    // Attributes section
    lines.push(Line::from(Span::styled(" Attributes", section_style)));
    let na = popup.attribute_conditions.len();
    for (i, cond) in popup.attribute_conditions.iter().enumerate() {
        let text = format!(
            "   {} {} {}",
            cond.field,
            operator_symbol(&cond.operator),
            cond.value
        );
        let idx = 1 + i;
        let style = if selected == idx { highlight } else { normal };
        lines.push(Line::from(Span::styled(text, style)));
    }
    let add_attr_idx = 1 + na;
    let add_attr_style = if selected == add_attr_idx {
        highlight
    } else {
        Style::default().fg(Color::Green)
    };
    lines.push(Line::from(Span::styled("   [+] Add", add_attr_style)));
    lines.push(Line::from(""));

    // Resource Attributes section
    lines.push(Line::from(Span::styled(
        " Resource Attributes",
        section_style,
    )));
    let nr = popup.resource_conditions.len();
    for (i, cond) in popup.resource_conditions.iter().enumerate() {
        let text = format!(
            "   {} {} {}",
            cond.field,
            operator_symbol(&cond.operator),
            cond.value
        );
        let idx = 2 + na + i;
        let style = if selected == idx { highlight } else { normal };
        lines.push(Line::from(Span::styled(text, style)));
    }
    let add_res_idx = 2 + na + nr;
    let add_res_style = if selected == add_res_idx {
        highlight
    } else {
        Style::default().fg(Color::Green)
    };
    lines.push(Line::from(Span::styled("   [+] Add", add_res_style)));
    lines.push(Line::from(""));

    // Apply button
    let apply_idx = 3 + na + nr;
    let apply_style = if selected == apply_idx {
        Style::default()
            .bg(Color::Yellow)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Yellow)
    };
    lines.push(Line::from(Span::styled("   [Apply]", apply_style)));
    lines.push(Line::from(""));

    // Help
    lines.push(Line::from(Span::styled(
        " \u{2191}\u{2193}:Navigate  Enter:Edit/Add  d:Del",
        Style::default().fg(Color::DarkGray),
    )));
    lines.push(Line::from(Span::styled(
        " Esc:Cancel",
        Style::default().fg(Color::DarkGray),
    )));

    let height = (lines.len() as u16) + 2; // +2 for borders
    let popup_area = centered_rect(50, height, area);

    frame.render_widget(Clear, popup_area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Log Filter ")
        .border_style(Style::default().fg(Color::Yellow));
    let text = Text::from(lines);
    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, popup_area);
}

fn draw_select_severity(frame: &mut Frame, area: Rect, selected: usize) {
    let highlight = Style::default().bg(Color::DarkGray).fg(Color::White);

    let mut lines: Vec<Line> = Vec::new();
    for (i, level) in SEVERITY_LEVELS.iter().enumerate() {
        let style = if i == selected {
            highlight
        } else {
            Style::default()
        };
        let prefix = if i == selected {
            "   \u{25B6} "
        } else {
            "     "
        };
        lines.push(Line::from(Span::styled(
            format!("{}{}", prefix, level),
            style,
        )));
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  Enter:Select  Esc:Back",
        Style::default().fg(Color::DarkGray),
    )));

    let height = (lines.len() as u16) + 2;
    let popup_area = centered_rect(40, height, area);

    frame.render_widget(Clear, popup_area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Select Severity (>=) ")
        .border_style(Style::default().fg(Color::Yellow));
    let text = Text::from(lines);
    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, popup_area);
}

fn draw_select_field(
    frame: &mut Frame,
    area: Rect,
    candidates: &[String],
    selected: usize,
    input: &str,
) {
    let filtered: Vec<&String> = candidates
        .iter()
        .filter(|c| c.to_ascii_lowercase().contains(&input.to_ascii_lowercase()))
        .collect();

    let highlight = Style::default().bg(Color::DarkGray).fg(Color::White);

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("  > ", Style::default().fg(Color::Green)),
        Span::styled(format!("{}|", input), Style::default().fg(Color::Yellow)),
    ]));
    lines.push(Line::from(""));

    for (i, candidate) in filtered.iter().enumerate() {
        let style = if i == selected {
            highlight
        } else {
            Style::default()
        };
        let prefix = if i == selected {
            "   \u{25B6} "
        } else {
            "     "
        };
        lines.push(Line::from(Span::styled(
            format!("{}{}", prefix, candidate),
            style,
        )));
    }

    if filtered.is_empty() {
        lines.push(Line::from(Span::styled(
            "     (no matches - input will be used)",
            Style::default().fg(Color::DarkGray),
        )));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  \u{2191}\u{2193}:Navigate  Enter:Select  Esc:Back",
        Style::default().fg(Color::DarkGray),
    )));

    let height = (lines.len() as u16 + 2).min(area.height);
    let popup_area = centered_rect(50, height, area);

    frame.render_widget(Clear, popup_area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Select Field ")
        .border_style(Style::default().fg(Color::Yellow));
    let text = Text::from(lines);
    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, popup_area);
}

fn draw_select_operator(frame: &mut Frame, area: Rect, field: &str, selected: usize) {
    let highlight = Style::default().bg(Color::DarkGray).fg(Color::White);

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("  Field: ", Style::default().fg(Color::DarkGray)),
        Span::raw(field.to_string()),
    ]));
    lines.push(Line::from(""));

    for (i, op) in ALL_OPERATORS.iter().enumerate() {
        let style = if i == selected {
            highlight
        } else {
            Style::default()
        };
        let prefix = if i == selected {
            "   \u{25B6} "
        } else {
            "     "
        };
        let text = format!(
            "{}{:<4} ({})",
            prefix,
            operator_symbol(op),
            operator_label(op)
        );
        lines.push(Line::from(Span::styled(text, style)));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "  Enter:Select  Esc:Back",
        Style::default().fg(Color::DarkGray),
    )));

    let height = (lines.len() as u16) + 2;
    let popup_area = centered_rect(50, height, area);

    frame.render_widget(Clear, popup_area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Select Operator ")
        .border_style(Style::default().fg(Color::Yellow));
    let text = Text::from(lines);
    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, popup_area);
}

fn draw_input_value(
    frame: &mut Frame,
    area: Rect,
    field: &str,
    operator: &super::FilterOperator,
    value: &str,
) {
    let popup_area = centered_rect(50, 9, area);
    frame.render_widget(Clear, popup_area);

    let lines = vec![
        Line::from(vec![
            Span::styled("  Field: ", Style::default().fg(Color::DarkGray)),
            Span::raw(field.to_string()),
        ]),
        Line::from(vec![
            Span::styled("  Operator: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{} ({})",
                operator_symbol(operator),
                operator_label(operator)
            )),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Value: ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{}|", value), Style::default().fg(Color::Yellow)),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            "  Enter:Add  Esc:Back",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Enter Value ")
        .border_style(Style::default().fg(Color::Yellow));
    let text = Text::from(lines);
    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, popup_area);
}
