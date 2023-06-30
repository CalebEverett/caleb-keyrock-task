use chrono::{prelude::DateTime, Utc};
use orderbook_agg_old::booksummary::ExchangeType;
use std::time::{Duration, UNIX_EPOCH};
use symbols::line;
use tui::backend::Backend;
use tui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{
    Axis, Block, BorderType, Borders, Cell, Chart, Dataset, GraphType, LineGauge, Paragraph, Row,
    Table,
};
use tui::{symbols, Frame};
use tui_logger::TuiLoggerWidget;

use super::actions::Actions;
use super::state::AppState;
use crate::app::App;

pub fn draw<B>(rect: &mut Frame<B>, app: &App, symbol: &String, decimals: u32)
where
    B: Backend,
{
    let size = rect.size();
    check_size(&size);

    // Vertical layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(12),
            ]
            .as_ref(),
        )
        .split(size);

    // Title
    let title = draw_title(symbol.clone());
    rect.render_widget(title, chunks[0]);

    // Body & Help
    let body_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            [
                Constraint::Length(48),
                Constraint::Min(20),
                Constraint::Length(32),
                Constraint::Length(32),
            ]
            .as_ref(),
        )
        .split(chunks[1]);

    let summary = draw_summary(app.state(), decimals);
    rect.render_widget(summary, body_chunks[0]);

    if let Some(datapoints) = app.state.get_datapoints() {
        if datapoints.into_iter().all(|d| d.len() > 0) {
            let chart = draw_chart(datapoints, decimals);
            rect.render_widget(chart, body_chunks[1]);
        }
    }

    let body = draw_body(app.is_loading(), app.state());
    rect.render_widget(body, body_chunks[2]);

    let help = draw_help(app.actions());
    rect.render_widget(help, body_chunks[3]);

    // Duration LineGauge
    // if let Some(duration) = app.state().duration() {
    //     let duration_block = draw_duration(duration);
    //     rect.render_widget(duration_block, chunks[2]);
    // }

    // Logs
    let logs = draw_logs();
    rect.render_widget(logs, chunks[2]);
}

fn draw_title<'a>(symbol: String) -> Paragraph<'a> {
    Paragraph::new(format!("Orderbook Summary: {}", symbol))
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::White))
                .border_type(BorderType::Plain),
        )
}

fn check_size(rect: &Rect) {
    if rect.width < 52 {
        panic!("Require width >= 52, (got {})", rect.width);
    }
    if rect.height < 28 {
        panic!("Require height >= 28, (got {})", rect.height);
    }
}

fn draw_body<'a>(loading: bool, state: &AppState) -> Paragraph<'a> {
    let initialized_text = if state.is_initialized() {
        "Initialized"
    } else {
        "Not Initialized !"
    };
    let loading_text = if loading { "Loading..." } else { "" };
    #[allow(unused_variables)]
    let sleep_text = if let Some(sleeps) = state.count_sleep() {
        format!("Sleep count: {}", sleeps)
    } else {
        String::default()
    };
    let tick_text = if let Some(ticks) = state.count_tick() {
        format!("Tick count: {}", ticks)
    } else {
        String::default()
    };

    Paragraph::new(vec![
        Spans::from(Span::raw(initialized_text)),
        Spans::from(Span::raw(loading_text)),
        // Spans::from(Span::raw(sleep_text)),
        Spans::from(Span::raw(tick_text)),
    ])
    .style(Style::default().fg(Color::LightCyan))
    .alignment(Alignment::Left)
    .block(
        Block::default()
            // .title("Body")
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::White))
            .border_type(BorderType::Plain),
    )
}

fn get_date_str_format(timestamp: f64) -> String {
    let d = UNIX_EPOCH + Duration::from_millis(timestamp as u64);
    let datetime = DateTime::<Utc>::from(d);
    datetime.format("%H:%M:%S").to_string()
}

fn draw_chart<'a>(datapoints: [&'a Vec<(f64, f64)>; 3], decimals: u32) -> Chart<'a> {
    let max_min_decimals = 8;
    let x_min = datapoints[0]
        .into_iter()
        .min_by(|x, y| (x.0 as u32).cmp(&(y.0 as u32)))
        .unwrap()
        .0;
    let x_max = datapoints[0]
        .into_iter()
        .max_by(|x, y| (x.0 as u32).cmp(&(y.0 as u32)))
        .unwrap()
        .0;
    let y_min = datapoints[2]
        .into_iter()
        .min_by(|x, y| {
            ((x.1 * 10u32.pow(max_min_decimals) as f64) as i32)
                .cmp(&((y.1 * 10u32.pow(max_min_decimals) as f64) as i32))
        })
        .unwrap()
        .1;
    let y_max = datapoints[2]
        .into_iter()
        .max_by(|x, y| {
            ((x.1 * 10u32.pow(max_min_decimals) as f64) as i32)
                .cmp(&((y.1 * 10u32.pow(max_min_decimals) as f64) as i32))
        })
        .unwrap()
        .1;

    let y_bounds = if y_max > y_min {
        [y_min, y_max]
    } else {
        [y_max, y_min]
    };

    let datasets: Vec<Dataset<'a>> = vec![
        // Dataset::<'a>::default()
        //     .name("bid")
        //     .marker(symbols::Marker::Dot)
        //     .graph_type(GraphType::Scatter)
        //     .style(Style::default().fg(Color::LightGreen))
        //     .data(datapoints[0]),
        // Dataset::<'a>::default()
        //     .name("ask")
        //     .marker(symbols::Marker::Dot)
        //     .graph_type(GraphType::Scatter)
        //     .style(Style::default().fg(Color::LightRed))
        //     .data(datapoints[1]),
        Dataset::<'a>::default()
            .name("spread")
            .marker(symbols::Marker::Dot)
            .graph_type(GraphType::Scatter)
            .style(Style::default().fg(Color::LightYellow))
            .data(datapoints[2]),
    ];
    Chart::new(datasets)
        .block(
            Block::default()
                .title(Span::styled(
                    "Spread History",
                    Style::default().fg(Color::White),
                ))
                .borders(Borders::ALL),
        )
        .x_axis(
            Axis::default()
                .title(Span::styled("UTC", Style::default().fg(Color::White)))
                .style(Style::default().fg(Color::White))
                .bounds([x_min, x_max])
                .labels(
                    [get_date_str_format(x_min), get_date_str_format(x_max)]
                        .iter()
                        .cloned()
                        .map(Span::from)
                        .collect(),
                ),
        )
        .y_axis(
            Axis::default()
                .style(Style::default().fg(Color::White))
                .bounds(y_bounds)
                .labels(
                    [
                        format!("{:.1$}", y_bounds[0], decimals as usize),
                        format!("{:.1$}", y_bounds[1], decimals as usize),
                    ]
                    .iter()
                    .cloned()
                    .map(Span::from)
                    .collect(),
                ),
        )
}

#[allow(dead_code)]
fn draw_duration(duration: &Duration) -> LineGauge {
    let sec = duration.as_secs();
    let label = format!("{}s", sec);
    let ratio = sec as f64 / 10.0;
    LineGauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Sleep duration"),
        )
        .gauge_style(
            Style::default()
                .fg(Color::Cyan)
                .bg(Color::Black)
                .add_modifier(Modifier::BOLD),
        )
        .line_set(line::THICK)
        .label(label)
        .ratio(ratio)
}

fn exchange_string(exchange: i32) -> String {
    match exchange {
        0 => ExchangeType::Binance.as_str_name().to_string(),
        1 => ExchangeType::Bitstamp.as_str_name().to_string(),
        _ => String::from("Unknown"),
    }
}

fn draw_summary(state: &AppState, decimals: u32) -> Table {
    let help_style = Style::default().fg(Color::Gray);

    let mut rows = vec![];
    rows.push(Row::new(vec![
        Cell::from(Span::styled("Ask/Bid".to_string(), help_style)),
        Cell::from(Span::styled(format!("{:>10}", "Quantity"), help_style)),
        Cell::from(Span::styled("Exchange".to_string(), help_style)),
    ]));
    if let Some(summary) = state.get_summary() {
        for level in summary.asks.iter().rev() {
            let row = Row::new(vec![
                Cell::from(Span::styled(
                    format!("{:>8.1$}", level.price, decimals as usize),
                    Style::default().fg(Color::LightRed),
                )),
                Cell::from(Span::styled(
                    format!("{:>10.5}", level.quantity),
                    help_style,
                )),
                Cell::from(Span::styled(exchange_string(level.exchange), help_style)),
            ]);
            rows.push(row);
        }
        rows.push(Row::new(vec![Span::styled("".to_string(), help_style); 3]));
        rows.push(Row::new(vec![
            Cell::from(Span::styled(
                format!("{:>8.1$}", summary.spread, decimals as usize),
                Style::default().fg(Color::LightYellow),
            )),
            Cell::from(Span::styled("".to_string(), help_style)),
            Cell::from(Span::styled("".to_string(), help_style)),
        ]));
        rows.push(Row::new(vec![Span::styled("".to_string(), help_style); 3]));
        for level in summary.bids.iter() {
            let row = Row::new(vec![
                Cell::from(Span::styled(
                    format!("{:>8.1$}", level.price, decimals as usize),
                    Style::default().fg(Color::LightGreen),
                )),
                Cell::from(Span::styled(
                    format!("{:>10.5}", level.quantity),
                    help_style,
                )),
                Cell::from(Span::styled(exchange_string(level.exchange), help_style)),
            ]);
            rows.push(row);
        }
    };

    Table::new(rows)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Plain)
                .title("Orderbook Summary"),
        )
        .widths(&[
            Constraint::Ratio(1, 3),
            Constraint::Ratio(1, 3),
            Constraint::Ratio(1, 3),
        ])
        .column_spacing(1)
}

fn draw_help(actions: &Actions) -> Table {
    let key_style = Style::default().fg(Color::LightCyan);
    let help_style = Style::default().fg(Color::Gray);

    let mut rows = vec![];
    let mut counter = 0;
    for action in actions.actions().iter() {
        let mut first = true;
        for key in action.keys() {
            let help = if first {
                first = false;
                action.to_string()
            } else {
                String::from("")
            };
            let row = Row::new(vec![
                Cell::from(Span::styled(key.to_string(), key_style)),
                Cell::from(Span::styled(help, help_style)),
            ]);
            if counter < 2 {
                rows.push(row);
            }
            counter += 1;
        }
    }

    Table::new(rows)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Plain)
                .title("Help"),
        )
        .widths(&[Constraint::Length(11), Constraint::Min(20)])
        .column_spacing(1)
}

fn draw_logs<'a>() -> TuiLoggerWidget<'a> {
    TuiLoggerWidget::default()
        .style_error(Style::default().fg(Color::Red))
        .style_debug(Style::default().fg(Color::Green))
        .style_warn(Style::default().fg(Color::Yellow))
        .style_trace(Style::default().fg(Color::Gray))
        .style_info(Style::default().fg(Color::Blue))
        .block(
            Block::default()
                .title("Logs")
                .border_style(Style::default().fg(Color::White).bg(Color::Black))
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::White).bg(Color::Black))
}
