use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table, Wrap},
    Frame, Terminal,
};

use crate::{fmt_duration, fmt_num, DashboardState};

/// Deploy info displayed in the header area.
pub struct DeployInfo {
    pub binary: String,
    pub results_dir: String,
    pub log_dir: String,
    pub cc_addrs: Vec<String>,
    pub num_nodes: usize,
    pub num_cc: usize,
    pub num_lc: usize,
    pub nodes_file: String,
}

/// Thread-safe log buffer that captures messages for the TUI.
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<Mutex<LogBufferInner>>,
}

struct LogBufferInner {
    lines: Vec<String>,
    max_lines: usize,
}

impl LogBuffer {
    pub fn new(max_lines: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LogBufferInner {
                lines: Vec::new(),
                max_lines,
            })),
        }
    }

    pub fn push(&self, msg: String) {
        let mut inner = self.inner.lock().unwrap();
        inner.lines.push(msg);
        if inner.lines.len() > inner.max_lines {
            inner.lines.remove(0);
        }
    }

    pub fn get_lines(&self) -> Vec<String> {
        self.inner.lock().unwrap().lines.clone()
    }
}

/// A macro-friendly logging function that appends to the buffer and also prints
/// to stderr so messages are visible on the terminal before the TUI starts.
#[macro_export]
macro_rules! log_msg {
    ($buf:expr, $($arg:tt)*) => {{
        let msg = format!($($arg)*);
        eprintln!("{}", &msg);
        $buf.push(msg);
    }};
}

/// Run the interactive TUI dashboard. Blocks until the user presses 'q' or Ctrl+C.
/// `state` and `log_buf` are polled each tick for updates.
pub fn run_tui(
    state: Arc<Mutex<DashboardState>>,
    deploy_info: DeployInfo,
    log_buf: LogBuffer,
) -> io::Result<()> {
    enable_raw_mode()?;
    io::stdout().execute(EnterAlternateScreen)?;
    let backend = ratatui::backend::CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let result = tui_loop(&mut terminal, state, &deploy_info, &log_buf);

    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;

    result
}

fn tui_loop(
    terminal: &mut Terminal<ratatui::backend::CrosstermBackend<io::Stdout>>,
    state: Arc<Mutex<DashboardState>>,
    deploy_info: &DeployInfo,
    log_buf: &LogBuffer,
) -> io::Result<()> {
    loop {
        terminal.draw(|f| {
            let state = state.lock().unwrap();
            draw_ui(f, &state, deploy_info, log_buf);
        })?;

        // Poll for events with a 200ms timeout so the UI refreshes ~5x/sec.
        // The CC is still only polled every 10s by the watchdog thread.
        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') => return Ok(()),
                        KeyCode::Char('c')
                            if key
                                .modifiers
                                .contains(crossterm::event::KeyModifiers::CONTROL) =>
                        {
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

fn draw_ui(
    f: &mut Frame,
    state: &DashboardState,
    deploy_info: &DeployInfo,
    log_buf: &LogBuffer,
) {
    let area = f.area();

    // Main layout: header, progress, details columns, logs
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),  // Header / deploy info
            Constraint::Length(7),  // Progress gauges + stats
            Constraint::Min(10),   // Middle section (two columns)
            Constraint::Length(12), // Logs
            Constraint::Length(1),  // Footer
        ])
        .split(area);

    draw_header(f, main_chunks[0], deploy_info, state);
    draw_progress(f, main_chunks[1], state);
    draw_middle(f, main_chunks[2], state);
    draw_logs(f, main_chunks[3], log_buf);
    draw_footer(f, main_chunks[4]);
}

fn draw_header(f: &mut Frame, area: Rect, info: &DeployInfo, state: &DashboardState) {
    // If completed, show frozen completion time; otherwise show live elapsed.
    let elapsed = if let Some(completed_at) = state.completed_at {
        completed_at.duration_since(state.start_time).as_secs()
    } else {
        state.start_time.elapsed().as_secs()
    };

    let timer_spans = if state.completed_at.is_some() {
        vec![
            Span::styled(
                format!("✅ Completed in {}", fmt_duration(elapsed)),
                Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
            ),
        ]
    } else {
        vec![
            Span::styled(
                format!("⏱ {}", fmt_duration(elapsed)),
                Style::default().fg(Color::Yellow),
            ),
        ]
    };

    let mut title_spans = vec![
        Span::styled(
            " Áika Cluster Dashboard ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
    ];
    title_spans.extend(timer_spans);

    let lines = vec![
        Line::from(title_spans),
        Line::from(vec![
            Span::styled(" CCs: ", Style::default().fg(Color::DarkGray)),
            Span::raw(info.cc_addrs.join("  ")),
        ]),
        Line::from(vec![
            Span::styled(" Nodes: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(
                "{} total  ({} CC, {} LC)",
                info.num_nodes, info.num_cc, info.num_lc
            )),
            Span::raw("   "),
            Span::styled("Results: ", Style::default().fg(Color::DarkGray)),
            Span::raw(&info.results_dir),
            Span::raw("   "),
            Span::styled("Logs: ", Style::default().fg(Color::DarkGray)),
            Span::raw(&info.log_dir),
        ]),
        Line::from(vec![
            Span::styled(" Binary: ", Style::default().fg(Color::DarkGray)),
            Span::raw(&info.binary),
            Span::raw("   "),
            Span::styled("Nodes file: ", Style::default().fg(Color::DarkGray)),
            Span::raw(&info.nodes_file),
        ]),
    ];

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(" Deploy Info ");
    let para = Paragraph::new(lines).block(block);
    f.render_widget(para, area);
}

fn draw_progress(f: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green))
        .title(" Progress ");

    let inner = block.inner(area);
    f.render_widget(block, area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Batch gauge
            Constraint::Length(1), // Image gauge
            Constraint::Length(1), // Throughput + ETA
            Constraint::Length(1), // Extra stats
            Constraint::Min(0),
        ])
        .split(inner);

    if let Some(ref status) = state.last_status {
        let t = &status.telemetry;

        // Batch gauge
        let batch_pct = if status.total_tasks > 0 {
            (status.completed_tasks as f64 / status.total_tasks as f64 * 100.0).min(100.0)
        } else {
            0.0
        };
        let batch_label = format!(
            "Batches: {} / {}",
            fmt_num(status.completed_tasks),
            fmt_num(status.total_tasks)
        );
        let batch_gauge = Gauge::default()
            .gauge_style(Style::default().fg(Color::Blue).bg(Color::DarkGray))
            .label(batch_label)
            .percent(batch_pct as u16);
        f.render_widget(batch_gauge, chunks[0]);

        // Image gauge
        let img_pct = if t.total_images > 0 {
            (t.completed_images as f64 / t.total_images as f64 * 100.0).min(100.0)
        } else {
            0.0
        };
        let img_label = format!(
            "Images:  {} / {}",
            fmt_num(t.completed_images),
            fmt_num(t.total_images)
        );
        let img_gauge = Gauge::default()
            .gauge_style(Style::default().fg(Color::Magenta).bg(Color::DarkGray))
            .label(img_label)
            .percent(img_pct as u16);
        f.render_widget(img_gauge, chunks[1]);

        // Throughput line
        let (throughput_avg, throughput_recent) = calc_throughput(state, t.batch_size);
        let eta = if throughput_avg > 0.0 && t.total_images > t.completed_images {
            let remaining = t.total_images - t.completed_images;
            let eta_secs = (remaining as f64 / throughput_avg) as u64;
            format!("ETA: {}", fmt_duration(eta_secs))
        } else if status.completed_tasks == status.total_tasks && status.total_tasks > 0 {
            "✅ DONE".to_string()
        } else {
            "ETA: calculating…".to_string()
        };

        let throughput_line = Line::from(vec![
            Span::styled(
                format!(" Avg: {:.1} img/s", throughput_avg),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw("  "),
            Span::styled(
                format!("Recent: {:.1} img/s", throughput_recent),
                Style::default().fg(Color::Green),
            ),
            Span::raw("  "),
            Span::styled(
                format!("Pending: {}", fmt_num(status.pending_tasks)),
                Style::default().fg(Color::Yellow),
            ),
            Span::raw("  "),
            Span::styled(
                format!("Assigned: {}", fmt_num(status.assigned_tasks)),
                Style::default().fg(Color::Blue),
            ),
            Span::raw("  "),
            Span::styled(eta, Style::default().fg(Color::White)),
        ]);
        f.render_widget(Paragraph::new(throughput_line), chunks[2]);

        // Extra stats line: efficiency, batch size, avg batch/s
        let efficiency = if t.total_assignments > 0 {
            format!(
                "{:.1}%",
                t.total_completions as f64 / t.total_assignments as f64 * 100.0
            )
        } else {
            "-".to_string()
        };
        let (throughput_batch_avg, _) = calc_throughput_batches(state);
        let extra_line = Line::from(vec![
            Span::styled(
                format!(" Efficiency: {}", efficiency),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                " (completions/assignments)",
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw("  "),
            Span::styled(
                format!("Batch size: {}", t.batch_size),
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw("  "),
            Span::styled(
                format!("Avg: {:.2} batch/s", throughput_batch_avg),
                Style::default().fg(Color::DarkGray),
            ),
        ]);
        f.render_widget(Paragraph::new(extra_line), chunks[3]);
    } else {
        let waiting = Paragraph::new(Line::from(Span::styled(
            " Waiting for cluster status… (CCs may still be electing a leader)",
            Style::default().fg(Color::Yellow),
        )));
        f.render_widget(waiting, chunks[0]);
    }
}

fn draw_middle(f: &mut Frame, area: Rect, state: &DashboardState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    draw_fault_tolerance(f, chunks[0], state);
    draw_per_node(f, chunks[1], state);
}

fn draw_fault_tolerance(f: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Red))
        .title(" Fault Tolerance & Nodes ");

    let mut lines = Vec::new();

    if let Some(ref status) = state.last_status {
        let t = &status.telemetry;

        lines.push(Line::from(vec![
            Span::styled(
                " TTL Expirations: ",
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(
                format!("{}", t.ttl_expirations),
                Style::default().fg(if t.ttl_expirations > 0 {
                    Color::Yellow
                } else {
                    Color::Green
                }),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled(" Assignments: ", Style::default().fg(Color::DarkGray)),
            Span::raw(fmt_num(t.total_assignments)),
            Span::raw("   "),
            Span::styled("Completions: ", Style::default().fg(Color::DarkGray)),
            Span::raw(fmt_num(t.total_completions)),
        ]));

        if t.max_images > 0 {
            lines.push(Line::from(vec![
                Span::styled(
                    " Max images: ",
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    format!("{} (limited)", fmt_num(t.max_images)),
                    Style::default().fg(Color::Yellow),
                ),
            ]));
        }

        lines.push(Line::from(""));

        lines.push(Line::from(vec![
            Span::styled(" LC  ", Style::default().fg(Color::DarkGray)),
            Span::styled("crashes: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", state.lc_crashes),
                Style::default().fg(if state.lc_crashes > 0 {
                    Color::Red
                } else {
                    Color::Green
                }),
            ),
            Span::raw("  "),
            Span::styled("restarts: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", state.lc_restarts)),
            Span::raw("  "),
            Span::styled("replacements: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", state.lc_replacements)),
        ]));
        lines.push(Line::from(vec![
            Span::styled(" CC  ", Style::default().fg(Color::DarkGray)),
            Span::styled("crashes: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", state.cc_crashes),
                Style::default().fg(if state.cc_crashes > 0 {
                    Color::Red
                } else {
                    Color::Green
                }),
            ),
            Span::raw("  "),
            Span::styled("restarts: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{}", state.cc_restarts)),
        ]));

        lines.push(Line::from(""));

        // Registered nodes info
        let total_agents: usize = status.registered_nodes.iter().map(|n| n.agent_count).sum();
        let active_lcs = status
            .registered_nodes
            .iter()
            .filter(|n| n.agent_count > 0)
            .count();
        let replica_lcs = status
            .registered_nodes
            .iter()
            .filter(|n| n.agent_count == 0)
            .count();
        lines.push(Line::from(vec![
            Span::styled(
                format!(" {} active LC", active_lcs),
                Style::default().fg(Color::Green),
            ),
            Span::raw("  "),
            Span::styled(
                format!("{} replica", replica_lcs),
                Style::default().fg(Color::Yellow),
            ),
            Span::raw("  "),
            Span::styled(
                format!("{} agents", total_agents),
                Style::default().fg(Color::Cyan),
            ),
        ]));

        if !status.stale_nodes.is_empty() {
            lines.push(Line::from(vec![
                Span::styled(
                    " ⚠ Stale: ",
                    Style::default()
                        .fg(Color::Red)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(status.stale_nodes.join(", ")),
            ]));
        }
    } else {
        lines.push(Line::from(Span::styled(
            " No data yet",
            Style::default().fg(Color::DarkGray),
        )));
    }

    let para = Paragraph::new(lines).block(block).wrap(Wrap { trim: true });
    f.render_widget(para, area);
}

fn draw_per_node(f: &mut Frame, area: Rect, state: &DashboardState) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue))
        .title(" Per-Node Throughput ");

    if let Some(ref status) = state.last_status {
        let t = &status.telemetry;

        if t.per_node_images.is_empty() {
            let para = Paragraph::new(Line::from(Span::styled(
                " (no completions yet)",
                Style::default().fg(Color::DarkGray),
            )))
            .block(block);
            f.render_widget(para, area);
            return;
        }

        let max_img = t
            .per_node_images
            .iter()
            .map(|(_, c)| *c)
            .max()
            .unwrap_or(1);

        let header = Row::new(vec![
            Cell::from("Node").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Images").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Batches").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Bar").style(Style::default().add_modifier(Modifier::BOLD)),
        ]);

        let rows: Vec<Row> = t
            .per_node_images
            .iter()
            .map(|(node_id, img_count)| {
                let batch_count = t
                    .per_node_completions
                    .iter()
                    .find(|(n, _)| n == node_id)
                    .map(|(_, c)| *c)
                    .unwrap_or(0);

                let bar_width: usize = 15;
                let filled = if max_img > 0 {
                    ((*img_count as f64 / max_img as f64) * bar_width as f64) as usize
                } else {
                    0
                };
                let bar = format!(
                    "{}{}",
                    "█".repeat(filled),
                    "░".repeat(bar_width.saturating_sub(filled))
                );

                let display_id = if node_id.len() > 16 {
                    &node_id[..16]
                } else {
                    node_id
                };

                Row::new(vec![
                    Cell::from(display_id.to_string()),
                    Cell::from(fmt_num(*img_count)),
                    Cell::from(fmt_num(batch_count)),
                    Cell::from(bar),
                ])
            })
            .collect();

        let widths = [
            Constraint::Length(17),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Min(10),
        ];

        let table = Table::new(rows, widths)
            .header(header.style(Style::default().fg(Color::Cyan)))
            .block(block);
        f.render_widget(table, area);
    } else {
        let para = Paragraph::new(Line::from(Span::styled(
            " No data yet",
            Style::default().fg(Color::DarkGray),
        )))
        .block(block);
        f.render_widget(para, area);
    }
}

fn draw_logs(f: &mut Frame, area: Rect, log_buf: &LogBuffer) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow))
        .title(" Logs ");

    let inner_height = area.height.saturating_sub(2) as usize; // minus borders
    let all_lines = log_buf.get_lines();
    let start = all_lines.len().saturating_sub(inner_height);
    let visible: Vec<Line> = all_lines[start..]
        .iter()
        .map(|l| {
            let style = if l.contains("FAILED") || l.contains("failed") || l.contains("ERROR") {
                Style::default().fg(Color::Red)
            } else if l.contains("warning") || l.contains("Warning") || l.contains("⚠") {
                Style::default().fg(Color::Yellow)
            } else if l.contains("ok") || l.contains("started") || l.contains("ready") {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::Gray)
            };
            Line::from(Span::styled(l.as_str(), style))
        })
        .collect();

    let para = Paragraph::new(visible).block(block);
    f.render_widget(para, area);
}

fn draw_footer(f: &mut Frame, area: Rect) {
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(
            " q",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" quit  "),
        Span::styled(
            "Ctrl+C",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" stop watchdog"),
    ]));
    f.render_widget(footer, area);
}

fn calc_throughput(state: &DashboardState, batch_size: usize) -> (f64, f64) {
    let avg = if state.throughput_history.len() >= 2 {
        let (t1, c1) = state.throughput_history.first().unwrap();
        let (t2, c2) = state.throughput_history.last().unwrap();
        let dt = t2.saturating_sub(*t1);
        if dt > 0 {
            (c2 - c1) as f64 / dt as f64 * batch_size as f64
        } else {
            0.0
        }
    } else {
        0.0
    };

    let recent = if state.throughput_history.len() >= 2 {
        let now_entry = state.throughput_history.last().unwrap();
        let cutoff = now_entry.0.saturating_sub(60);
        let old_entry = state
            .throughput_history
            .iter()
            .rev()
            .find(|(ts, _)| *ts <= cutoff)
            .unwrap_or(state.throughput_history.first().unwrap());
        let dt = now_entry.0.saturating_sub(old_entry.0);
        if dt > 0 {
            (now_entry.1 - old_entry.1) as f64 / dt as f64 * batch_size as f64
        } else {
            0.0
        }
    } else {
        0.0
    };

    (avg, recent)
}

/// Returns (avg_batch_per_sec, recent_batch_per_sec) without multiplying by batch_size.
fn calc_throughput_batches(state: &DashboardState) -> (f64, f64) {
    let avg = if state.throughput_history.len() >= 2 {
        let (t1, c1) = state.throughput_history.first().unwrap();
        let (t2, c2) = state.throughput_history.last().unwrap();
        let dt = t2.saturating_sub(*t1);
        if dt > 0 {
            (c2 - c1) as f64 / dt as f64
        } else {
            0.0
        }
    } else {
        0.0
    };

    let recent = if state.throughput_history.len() >= 2 {
        let now_entry = state.throughput_history.last().unwrap();
        let cutoff = now_entry.0.saturating_sub(60);
        let old_entry = state
            .throughput_history
            .iter()
            .rev()
            .find(|(ts, _)| *ts <= cutoff)
            .unwrap_or(state.throughput_history.first().unwrap());
        let dt = now_entry.0.saturating_sub(old_entry.0);
        if dt > 0 {
            (now_entry.1 - old_entry.1) as f64 / dt as f64
        } else {
            0.0
        }
    } else {
        0.0
    };

    (avg, recent)
}
