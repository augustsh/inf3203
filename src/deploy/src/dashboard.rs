use std::collections::HashMap;
use std::io;
use std::process::Stdio;
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
    widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table},
    Frame, Terminal,
};

use crate::{fmt_duration, fmt_num, DashboardState};

// ── Startup config TUI ──────────────────────────────────────────────────────

/// User-configurable cluster parameters chosen in the startup TUI.
pub struct StartupConfig {
    pub num_nodes: usize,
    pub num_cc: usize,
    pub num_replicas: usize,
    pub agents_per_lc: usize,
    pub max_images: u64,
    pub task_ttl_secs: u64,
    pub batch_size: usize,
}

impl StartupConfig {
    pub fn new(available_nodes: usize) -> Self {
        Self {
            num_nodes: available_nodes.min(10),
            num_cc: 3,
            num_replicas: 1,
            agents_per_lc: 2,
            max_images: 0,
            task_ttl_secs: 60,
            batch_size: 50,
        }
    }
}

/// Field currently selected in the startup form.
#[derive(Clone, Copy, PartialEq)]
enum StartupField {
    NumNodes,
    NumCC,
    NumReplicas,
    AgentsPerLC,
    MaxImages,
    TaskTTL,
    BatchSize,
}

const STARTUP_FIELDS: [StartupField; 7] = [
    StartupField::NumNodes,
    StartupField::NumCC,
    StartupField::NumReplicas,
    StartupField::AgentsPerLC,
    StartupField::MaxImages,
    StartupField::TaskTTL,
    StartupField::BatchSize,
];

/// Show the startup configuration TUI. Returns the config when the user presses Enter.
/// Returns `None` if the user presses 'q' to quit.
pub fn run_startup_tui(available_nodes: usize) -> io::Result<Option<StartupConfig>> {
    enable_raw_mode()?;
    io::stdout().execute(EnterAlternateScreen)?;
    let backend = ratatui::backend::CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let result = startup_loop(&mut terminal, available_nodes);

    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;

    result
}

fn startup_loop(
    terminal: &mut Terminal<ratatui::backend::CrosstermBackend<io::Stdout>>,
    available_nodes: usize,
) -> io::Result<Option<StartupConfig>> {
    let mut config = StartupConfig::new(available_nodes);
    let mut selected = 0usize; // index into STARTUP_FIELDS

    loop {
        let field = STARTUP_FIELDS[selected];
        terminal.draw(|f| draw_startup(f, &config, field, available_nodes))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') => return Ok(None),
                        KeyCode::Char('c')
                            if key
                                .modifiers
                                .contains(crossterm::event::KeyModifiers::CONTROL) =>
                        {
                            return Ok(None);
                        }
                        KeyCode::Up => {
                            selected = if selected == 0 {
                                STARTUP_FIELDS.len() - 1
                            } else {
                                selected - 1
                            };
                        }
                        KeyCode::Down => {
                            selected = (selected + 1) % STARTUP_FIELDS.len();
                        }
                        KeyCode::Left => adjust_field(&mut config, field, false, available_nodes),
                        KeyCode::Right => adjust_field(&mut config, field, true, available_nodes),
                        KeyCode::Enter => {
                            // Validate before accepting.
                            if config.num_nodes >= config.num_cc
                                && config.num_cc >= 3
                                && config.num_nodes <= available_nodes
                            {
                                return Ok(Some(config));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

fn adjust_field(config: &mut StartupConfig, field: StartupField, up: bool, max_nodes: usize) {
    match field {
        StartupField::NumNodes => {
            if up {
                config.num_nodes = (config.num_nodes + 1).min(max_nodes);
            } else {
                config.num_nodes = config.num_nodes.saturating_sub(1).max(config.num_cc);
            }
        }
        StartupField::NumCC => {
            if up {
                config.num_cc = (config.num_cc + 2).min(config.num_nodes).min(7); // odd, max 7
            } else {
                config.num_cc = config.num_cc.saturating_sub(2).max(3); // odd, min 3
            }
        }
        StartupField::NumReplicas => {
            let max_replicas = config.num_nodes.saturating_sub(config.num_cc);
            if up {
                config.num_replicas = (config.num_replicas + 1).min(max_replicas);
            } else {
                config.num_replicas = config.num_replicas.saturating_sub(1);
            }
        }
        StartupField::AgentsPerLC => {
            if up {
                config.agents_per_lc = (config.agents_per_lc + 1).min(8);
            } else {
                config.agents_per_lc = config.agents_per_lc.saturating_sub(1).max(1);
            }
        }
        StartupField::MaxImages => {
            if up {
                config.max_images = config.max_images.saturating_add(10000);
            } else {
                config.max_images = config.max_images.saturating_sub(10000);
            }
        }
        StartupField::TaskTTL => {
            if up {
                config.task_ttl_secs += 30;
            } else {
                config.task_ttl_secs = config.task_ttl_secs.saturating_sub(30).max(30);
            }
        }
        StartupField::BatchSize => {
            if up {
                config.batch_size = (config.batch_size + 10).min(500);
            } else {
                config.batch_size = config.batch_size.saturating_sub(10).max(10);
            }
        }
    }
}

fn draw_startup(f: &mut Frame, config: &StartupConfig, selected: StartupField, available: usize) {
    let area = f.area();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Title
            Constraint::Min(14),   // Form
            Constraint::Length(5), // Summary
            Constraint::Length(1),  // Footer
        ])
        .split(area);

    // Title
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            " Áika Cluster — Configuration ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    ]))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(title, chunks[0]);

    // Form
    let form_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue))
        .title(" Settings [↑↓ select, ←→ adjust, Enter start, q quit] ");
    let inner = form_block.inner(chunks[1]);
    f.render_widget(form_block, chunks[1]);

    let num_lc = config.num_nodes.saturating_sub(config.num_cc);
    let num_active_lc = num_lc.saturating_sub(config.num_replicas);
    let total_agents = num_active_lc * config.agents_per_lc;
    let img_display = if config.max_images == 0 {
        "all (~1.2M)".to_string()
    } else {
        fmt_num(config.max_images)
    };

    let available_hint = format!("available: {}", available);
    let agents_hint = format!("→ {} total agents", total_agents);

    let fields: Vec<(&str, String, StartupField, &str)> = vec![
        (
            "Total Nodes",
            format!("{}", config.num_nodes),
            StartupField::NumNodes,
            &available_hint,
        ),
        (
            "Cluster Controllers",
            format!("{}", config.num_cc),
            StartupField::NumCC,
            "Raft quorum (odd, 3-7)",
        ),
        (
            "LC Replicas",
            format!("{}", config.num_replicas),
            StartupField::NumReplicas,
            "standby LC nodes (--agents 0)",
        ),
        (
            "Agents per LC",
            format!("{}", config.agents_per_lc),
            StartupField::AgentsPerLC,
            &agents_hint,
        ),
        (
            "Max Images",
            img_display,
            StartupField::MaxImages,
            "0 = unlimited (←→ steps of 10k)",
        ),
        (
            "Task TTL",
            format!("{}s", config.task_ttl_secs),
            StartupField::TaskTTL,
            "seconds before expired tasks reassigned",
        ),
        (
            "Batch Size",
            format!("{}", config.batch_size),
            StartupField::BatchSize,
            "images per task batch",
        ),
    ];

    let row_constraints: Vec<Constraint> = fields.iter().map(|_| Constraint::Length(2)).collect();
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(row_constraints)
        .split(inner);

    for (i, (label, value, field, hint)) in fields.iter().enumerate() {
        let is_selected = *field == selected;
        let marker = if is_selected { "▸ " } else { "  " };
        let style = if is_selected {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        };
        let val_style = if is_selected {
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Cyan)
        };

        let arrows = if is_selected { " ◂ ▸" } else { "" };

        let line = Line::from(vec![
            Span::styled(marker, style),
            Span::styled(format!("{:<22}", label), style),
            Span::styled(format!("{:>10}", value), val_style),
            Span::styled(arrows, Style::default().fg(Color::DarkGray)),
            Span::styled(format!("  {}", hint), Style::default().fg(Color::DarkGray)),
        ]);
        f.render_widget(Paragraph::new(line), rows[i]);
    }

    // Summary
    let summary_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Green))
        .title(" Cluster Summary ");
    let summary_inner = summary_block.inner(chunks[2]);
    f.render_widget(summary_block, chunks[2]);

    let valid = config.num_nodes >= config.num_cc
        && config.num_cc >= 3
        && config.num_nodes <= available;

    let summary_lines = vec![
        Line::from(vec![
            Span::styled(
                format!(
                    " {} CC nodes  +  {} LC ({} active, {} replica)  =  {} total",
                    config.num_cc, num_lc, num_active_lc, config.num_replicas, config.num_nodes
                ),
                Style::default().fg(Color::White),
            ),
        ]),
        Line::from(vec![
            Span::styled(
                format!(
                    " {} agents × {} img/batch  |  TTL {}s  |  Images: {}",
                    total_agents,
                    config.batch_size,
                    config.task_ttl_secs,
                    if config.max_images == 0 {
                        "all".to_string()
                    } else {
                        fmt_num(config.max_images)
                    },
                ),
                Style::default().fg(Color::White),
            ),
        ]),
        if valid {
            Line::from(Span::styled(
                " ✓ Ready — press Enter to deploy",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ))
        } else {
            Line::from(Span::styled(
                " ✗ Invalid config — need ≥3 CC, nodes ≥ CC, nodes ≤ available",
                Style::default().fg(Color::Red),
            ))
        },
    ];
    f.render_widget(Paragraph::new(summary_lines), summary_inner);

    // Footer
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(
            " q",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" quit  "),
        Span::styled(
            "↑↓",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" select  "),
        Span::styled(
            "←→",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" adjust  "),
        Span::styled(
            "Enter",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" deploy"),
    ]));
    f.render_widget(footer, chunks[3]);
}

// ── Monitoring TUI ──────────────────────────────────────────────────────────

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
    /// CC hostnames (for the selectable node list).
    pub cc_nodes: Vec<String>,
    /// LC hostnames in order: replica first, then active.
    pub lc_nodes: Vec<String>,
}

/// TUI-local interactive state (not shared with background threads).
struct TuiState {
    selected_node: usize,
    show_results: bool,
    label_counts: Option<Vec<(String, u64)>>,
    cluster_started: bool,
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
/// Run the interactive TUI dashboard. Blocks until the user presses 'q' or Ctrl+C.
/// `state` and `log_buf` are polled each tick for updates.
/// `deploy_info` starts as `None` while deploy is in progress and becomes `Some` once ready.
pub fn run_tui(
    state: Arc<Mutex<DashboardState>>,
    deploy_info: Arc<Mutex<Option<DeployInfo>>>,
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
    deploy_info: &Arc<Mutex<Option<DeployInfo>>>,
    log_buf: &LogBuffer,
) -> io::Result<()> {
    let mut tui = TuiState {
        selected_node: 0,
        show_results: false,
        label_counts: None,
        cluster_started: false,
    };

    loop {
        // Lock deploy_info once per frame.
        let info_guard = deploy_info.lock().unwrap();
        let total_nodes = info_guard
            .as_ref()
            .map_or(0, |di| di.cc_nodes.len() + di.lc_nodes.len());

        terminal.draw(|f| {
            let state = state.lock().unwrap();
            draw_ui(f, &state, info_guard.as_ref(), log_buf, &tui);
        })?;
        drop(info_guard);

        // Poll for events with a 200ms timeout so the UI refreshes ~5x/sec.
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
                        KeyCode::Up => {
                            if total_nodes > 0 {
                                tui.selected_node = if tui.selected_node == 0 {
                                    total_nodes - 1
                                } else {
                                    tui.selected_node - 1
                                };
                            }
                        }
                        KeyCode::Down => {
                            if total_nodes > 0 {
                                tui.selected_node = (tui.selected_node + 1) % total_nodes;
                            }
                        }
                        KeyCode::Char('k') | KeyCode::Char('K') => {
                            let info_guard = deploy_info.lock().unwrap();
                            if let Some(di) = info_guard.as_ref() {
                                if total_nodes > 0 {
                                    let hostname = node_hostname(di, tui.selected_node);
                                    drop(info_guard);
                                    kill_node(&hostname, log_buf);
                                }
                            }
                        }
                        KeyCode::Char('a') | KeyCode::Char('A') => {
                            let info_guard = deploy_info.lock().unwrap();
                            if let Some(di) = info_guard.as_ref() {
                                if total_nodes > 0
                                    && tui.selected_node >= di.cc_nodes.len()
                                {
                                    let hostname =
                                        node_hostname(di, tui.selected_node);
                                    drop(info_guard);
                                    kill_one_agent(&hostname, log_buf);
                                }
                            }
                        }
                        KeyCode::Char('r') | KeyCode::Char('R') => {
                            if tui.show_results {
                                // Toggle off.
                                tui.show_results = false;
                            } else {
                                let info_guard = deploy_info.lock().unwrap();
                                if let Some(di) = info_guard.as_ref() {
                                    tui.show_results = true;
                                    tui.label_counts = Some(load_results(&di.results_dir));
                                }
                            }
                        }
                        KeyCode::Char('s') | KeyCode::Char('S') => {
                            if !tui.cluster_started {
                                let info_guard = deploy_info.lock().unwrap();
                                if let Some(di) = info_guard.as_ref() {
                                    send_start_signal(&di.cc_addrs, log_buf);
                                    tui.cluster_started = true;
                                    // Start the timer now.
                                    state.lock().unwrap().start_time = Some(std::time::Instant::now());
                                }
                            }
                        }
                        KeyCode::Esc => {
                            tui.show_results = false;
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
    deploy_info: Option<&DeployInfo>,
    log_buf: &LogBuffer,
    tui: &TuiState,
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
    draw_middle(f, main_chunks[2], state, deploy_info, tui);
    draw_logs(f, main_chunks[3], log_buf);
    draw_footer(f, main_chunks[4], tui);
}

fn draw_header(f: &mut Frame, area: Rect, info: Option<&DeployInfo>, state: &DashboardState) {
    // If completed, show frozen completion time; if started, show live elapsed; else show waiting.
    let timer_spans = if let Some(completed_at) = state.completed_at {
        let elapsed = state.start_time
            .map(|st| completed_at.duration_since(st).as_secs())
            .unwrap_or(0);
        vec![
            Span::styled(
                format!("✅ Completed in {}", fmt_duration(elapsed)),
                Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
            ),
        ]
    } else if let Some(start) = state.start_time {
        let elapsed = start.elapsed().as_secs();
        vec![
            Span::styled(
                format!("⏱ {}", fmt_duration(elapsed)),
                Style::default().fg(Color::Yellow),
            ),
        ]
    } else {
        vec![
            Span::styled(
                "⏱ waiting for start".to_string(),
                Style::default().fg(Color::DarkGray),
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

    let (cc_addrs_str, nodes_str, results_str, log_str, binary_str, nodes_file_str) =
        if let Some(info) = info {
            (
                info.cc_addrs.join("  "),
                format!("{} total  ({} CC, {} LC)", info.num_nodes, info.num_cc, info.num_lc),
                info.results_dir.as_str(),
                info.log_dir.as_str(),
                info.binary.as_str(),
                info.nodes_file.as_str(),
            )
        } else {
            (
                "deploying…".into(),
                "deploying…".into(),
                "—",
                "—",
                "—",
                "—",
            )
        };

    let lines = vec![
        Line::from(title_spans),
        Line::from(vec![
            Span::styled(" CCs: ", Style::default().fg(Color::DarkGray)),
            Span::raw(cc_addrs_str),
        ]),
        Line::from(vec![
            Span::styled(" Nodes: ", Style::default().fg(Color::DarkGray)),
            Span::raw(nodes_str),
            Span::raw("   "),
            Span::styled("Results: ", Style::default().fg(Color::DarkGray)),
            Span::raw(results_str),
            Span::raw("   "),
            Span::styled("Logs: ", Style::default().fg(Color::DarkGray)),
            Span::raw(log_str),
        ]),
        Line::from(vec![
            Span::styled(" Binary: ", Style::default().fg(Color::DarkGray)),
            Span::raw(binary_str),
            Span::raw("   "),
            Span::styled("Nodes file: ", Style::default().fg(Color::DarkGray)),
            Span::raw(nodes_file_str),
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

        // Fault tolerance stats line
        let (throughput_batch_avg, _) = calc_throughput_batches(state);
        let extra_line = Line::from(vec![
            Span::styled(
                format!(" TTL: {}", t.ttl_expirations),
                Style::default().fg(if t.ttl_expirations > 0 {
                    Color::Yellow
                } else {
                    Color::DarkGray
                }),
            ),
            Span::raw("  "),
            Span::styled(
                format!(
                    "LC ×{} ↻{}",
                    state.lc_crashes, state.lc_restarts
                ),
                Style::default().fg(if state.lc_crashes > 0 {
                    Color::Red
                } else {
                    Color::DarkGray
                }),
            ),
            Span::raw("  "),
            Span::styled(
                format!(
                    "CC ×{} ↻{}",
                    state.cc_crashes, state.cc_restarts
                ),
                Style::default().fg(if state.cc_crashes > 0 {
                    Color::Red
                } else {
                    Color::DarkGray
                }),
            ),
            Span::raw("  "),
            Span::styled(
                format!("Batch: {}×{}", t.batch_size, fmt_num(status.total_tasks)),
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw("  "),
            Span::styled(
                format!("{:.2} batch/s", throughput_batch_avg),
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

fn draw_middle(f: &mut Frame, area: Rect, state: &DashboardState, deploy_info: Option<&DeployInfo>, tui: &TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    draw_nodes(f, chunks[0], state, deploy_info, tui.selected_node);
    if tui.show_results {
        draw_results(f, chunks[1], &tui.label_counts);
    } else {
        draw_per_node(f, chunks[1], state);
    }
}

fn draw_nodes(
    f: &mut Frame,
    area: Rect,
    state: &DashboardState,
    deploy_info: Option<&DeployInfo>,
    selected: usize,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Red))
        .title(" Nodes [↑↓ select, k kill] ");

    let inner = block.inner(area);
    f.render_widget(block, area);

    let Some(deploy_info) = deploy_info else {
        let msg = Paragraph::new(" Deploying…")
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(msg, inner);
        return;
    };
    let cc_count = deploy_info.cc_nodes.len();
    let mut rows: Vec<Row> = Vec::new();

    for (i, hostname) in deploy_info.cc_nodes.iter().enumerate() {
        let cc_status = state
            .cc_statuses
            .get(i)
            .filter(|s| s.addr.starts_with(hostname));

        let (status_str, status_color) = match cc_status {
            Some(s) if s.is_leader => ("★ leader".to_string(), Color::Green),
            Some(s) if s.alive => ("follower".to_string(), Color::Cyan),
            Some(_) => ("✗ dead".to_string(), Color::Red),
            None => ("—".to_string(), Color::DarkGray),
        };

        let style = if i == selected {
            Style::default()
                .bg(Color::DarkGray)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        rows.push(
            Row::new(vec![
                Cell::from(format!("CC{}", i + 1)).style(Style::default().fg(Color::Cyan)),
                Cell::from(hostname.as_str()),
                Cell::from(status_str).style(Style::default().fg(status_color)),
            ])
            .style(style),
        );
    }

    for (i, hostname) in deploy_info.lc_nodes.iter().enumerate() {
        let global_idx = cc_count + i;
        let lc_id = format!("lc-{}", hostname);

        let agent_info = state
            .last_status
            .as_ref()
            .and_then(|s| s.registered_nodes.iter().find(|n| n.node_id == lc_id));
        let stale = state
            .last_status
            .as_ref()
            .map_or(false, |s| s.stale_nodes.contains(&lc_id));

        let status_str = if stale {
            "⚠ stale".to_string()
        } else if let Some(node) = agent_info {
            if node.agent_count > 0 {
                format!("{} agents", node.agent_count)
            } else {
                "replica".to_string()
            }
        } else {
            "—".to_string()
        };

        let status_color = if stale {
            Color::Red
        } else if agent_info.is_some_and(|n| n.agent_count > 0) {
            Color::Green
        } else if agent_info.is_some() {
            Color::Yellow
        } else {
            Color::DarkGray
        };

        let style = if global_idx == selected {
            Style::default()
                .bg(Color::DarkGray)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };

        rows.push(
            Row::new(vec![
                Cell::from("LC").style(Style::default().fg(Color::Blue)),
                Cell::from(hostname.as_str()),
                Cell::from(status_str).style(Style::default().fg(status_color)),
            ])
            .style(style),
        );
    }

    let widths = [
        Constraint::Length(4),
        Constraint::Min(10),
        Constraint::Length(12),
    ];

    let table = Table::new(rows, widths).column_spacing(1);
    f.render_widget(table, inner);
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
            Cell::from("img/s").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Batches").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("TTLs").style(Style::default().add_modifier(Modifier::BOLD)),
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

                let ttl_count = t
                    .per_node_ttl_expirations
                    .iter()
                    .find(|(n, _)| n == node_id)
                    .map(|(_, c)| *c)
                    .unwrap_or(0);

                // Calculate per-node img/s from history (last 60s window).
                let rate = calc_per_node_rate(state, node_id);

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

                let ttl_cell = if ttl_count > 0 {
                    Cell::from(format!("{}", ttl_count)).style(Style::default().fg(Color::Red))
                } else {
                    Cell::from("0")
                };

                Row::new(vec![
                    Cell::from(display_id.to_string()),
                    Cell::from(fmt_num(*img_count)),
                    Cell::from(format!("{:.1}", rate)),
                    Cell::from(fmt_num(batch_count)),
                    ttl_cell,
                    Cell::from(bar),
                ])
            })
            .collect();

        let widths = [
            Constraint::Length(17),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(8),
            Constraint::Length(5),
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

fn draw_results(f: &mut Frame, area: Rect, label_counts: &Option<Vec<(String, u64)>>) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Magenta))
        .title(" Results — Images per Label ");

    let inner = block.inner(area);
    f.render_widget(block, area);

    let Some(counts) = label_counts else {
        let para = Paragraph::new(Line::from(Span::styled(
            " Loading…",
            Style::default().fg(Color::DarkGray),
        )));
        f.render_widget(para, inner);
        return;
    };

    if counts.is_empty() {
        let para = Paragraph::new(Line::from(Span::styled(
            " No results found yet (press r to refresh)",
            Style::default().fg(Color::Yellow),
        )));
        f.render_widget(para, inner);
        return;
    }

    let max_count = counts.first().map(|(_, c)| *c).unwrap_or(1);
    let total: u64 = counts.iter().map(|(_, c)| *c).sum();

    let header = Row::new(vec![
        Cell::from("Label").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Count").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Bar").style(Style::default().add_modifier(Modifier::BOLD)),
    ]);

    let rows: Vec<Row> = counts
        .iter()
        .map(|(label, count)| {
            let bar_width: usize = 20;
            let filled = if max_count > 0 {
                ((*count as f64 / max_count as f64) * bar_width as f64) as usize
            } else {
                0
            };
            let bar = format!(
                "{}{}",
                "█".repeat(filled),
                "░".repeat(bar_width.saturating_sub(filled))
            );

            Row::new(vec![
                Cell::from(label.as_str()),
                Cell::from(fmt_num(*count)),
                Cell::from(bar),
            ])
        })
        .collect();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(inner);

    let summary = format!(" {} labels, {} images total", counts.len(), fmt_num(total));
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            summary,
            Style::default().fg(Color::Cyan),
        ))),
        chunks[0],
    );

    let widths = [
        Constraint::Min(15),
        Constraint::Length(10),
        Constraint::Length(22),
    ];

    let table = Table::new(rows, widths)
        .header(header.style(Style::default().fg(Color::Magenta)))
        .column_spacing(1);
    f.render_widget(table, chunks[1]);
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

fn draw_footer(f: &mut Frame, area: Rect, tui: &TuiState) {
    let mut spans = vec![
        Span::styled(
            " q",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" quit  "),
    ];
    if !tui.cluster_started {
        spans.push(Span::styled(
            "s",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" START  "));
    }
    spans.extend([
        Span::styled(
            "↑↓",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" select  "),
        Span::styled(
            "k",
            Style::default()
                .fg(Color::Red)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" kill node  "),
        Span::styled(
            "a",
            Style::default()
                .fg(Color::Red)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" kill agent  "),
        Span::styled(
            "r",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    if tui.show_results {
        spans.push(Span::raw(" back  "));
        spans.push(Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" back"));
    } else {
        spans.push(Span::raw(" results"));
    }
    let footer = Paragraph::new(Line::from(spans));
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

/// Calculate per-node img/s rate from the last 60s of per_node_history.
fn calc_per_node_rate(state: &DashboardState, node_id: &str) -> f64 {
    let Some(history) = state.per_node_history.get(node_id) else {
        return 0.0;
    };
    if history.len() < 2 {
        return 0.0;
    }
    let now_entry = history.last().unwrap();
    let cutoff = now_entry.0.saturating_sub(60);
    let old_entry = history
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= cutoff)
        .unwrap_or(history.first().unwrap());
    let dt = now_entry.0.saturating_sub(old_entry.0);
    if dt > 0 {
        (now_entry.1 - old_entry.1) as f64 / dt as f64
    } else {
        0.0
    }
}

/// Get the hostname of the node at the given index in the combined CC+LC list.
fn node_hostname(info: &DeployInfo, index: usize) -> String {
    if index < info.cc_nodes.len() {
        info.cc_nodes[index].clone()
    } else {
        info.lc_nodes[index - info.cc_nodes.len()].clone()
    }
}

/// Send POST /start to all CC addresses to unhold the cluster.
fn send_start_signal(cc_addrs: &[String], log_buf: &LogBuffer) {
    let addrs: Vec<String> = cc_addrs.to_vec();
    let log_buf = log_buf.clone();
    std::thread::spawn(move || {
        log_buf.push(format!("🚀 Sending start signal to {} CCs…", addrs.len()));
        for addr in &addrs {
            let url = format!("http://{}/start", addr);
            let result = std::process::Command::new("curl")
                .args(["-s", "-o", "/dev/null", "-w", "%{http_code}", "-X", "POST", &url])
                .output();
            match result {
                Ok(out) => {
                    let code = String::from_utf8_lossy(&out.stdout);
                    if code.starts_with('2') {
                        log_buf.push(format!("  ✓ {} started (HTTP {})", addr, code.trim()));
                    } else {
                        log_buf.push(format!("  ✗ {} — HTTP {}", addr, code.trim()));
                    }
                }
                Err(e) => {
                    log_buf.push(format!("  ✗ {} — {}", addr, e));
                }
            }
        }
        log_buf.push(format!("Start signal sent."));
    });
}

/// Kill all aika processes on a node via SSH (runs in background thread).
fn kill_node(hostname: &str, log_buf: &LogBuffer) {
    let hostname = hostname.to_string();
    let log_buf = log_buf.clone();
    std::thread::spawn(move || {
        log_buf.push(format!("⚡ Killing aika processes on {}…", hostname));
        let result = std::process::Command::new("ssh")
            .args([
                "-n",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "StrictHostKeyChecking=no",
                &hostname,
            ])
            .arg("pkill -f '[i]nf3203_aika'; pkill -f '[-]-agent-id'; true")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        match result {
            Ok(s) if s.success() => {
                log_buf.push(format!("⚡ Killed processes on {}", hostname));
            }
            Ok(_) => {
                log_buf.push(format!(
                    "⚡ No aika processes on {} (already dead?)",
                    hostname
                ));
            }
            Err(e) => {
                log_buf.push(format!("⚡ SSH to {} failed: {}", hostname, e));
            }
        }
    });
}

/// Kill one agent process on the given LC hostname (oldest agent first).
fn kill_one_agent(hostname: &str, log_buf: &LogBuffer) {
    let hostname = hostname.to_string();
    let log_buf = log_buf.clone();
    std::thread::spawn(move || {
        log_buf.push(format!("⚡ Killing one agent on {}…", hostname));

        // Count agents before kill.
        let before = count_agents_on(&hostname);

        // Find the oldest agent process (by PID, lowest = oldest) and kill it.
        // Agents are spawned via current_exe() which resolves the symlink to
        // the real binary name (aika-node), so match on --agent-id flag instead.
        let result = std::process::Command::new("ssh")
            .args([
                "-n",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                "-o",
                "StrictHostKeyChecking=no",
                &hostname,
            ])
            .arg("pgrep -f '[-]-agent-id' | head -1 | xargs -r kill")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        match result {
            Ok(s) if s.success() => {
                // Brief wait to let the kill take effect.
                std::thread::sleep(std::time::Duration::from_millis(500));
                let after = count_agents_on(&hostname);
                log_buf.push(format!(
                    "⚡ Killed agent on {} (agents: {} → {})",
                    hostname, before, after
                ));
                // Wait for LC to respawn, then report.
                std::thread::sleep(std::time::Duration::from_secs(5));
                let recovered = count_agents_on(&hostname);
                if recovered > after {
                    log_buf.push(format!(
                        "↻ LC on {} respawned agent (agents: {} → {})",
                        hostname, after, recovered
                    ));
                } else if recovered == after {
                    log_buf.push(format!(
                        "⚠ Agent on {} not respawned yet (still {})",
                        hostname, recovered
                    ));
                }
            }
            Ok(_) => {
                log_buf.push(format!(
                    "⚡ No agent processes on {} (already dead?)",
                    hostname
                ));
            }
            Err(e) => {
                log_buf.push(format!("⚡ SSH to {} failed: {}", hostname, e));
            }
        }
    });
}

/// Count the number of agent processes running on a remote host.
fn count_agents_on(hostname: &str) -> u32 {
    let output = std::process::Command::new("ssh")
        .args([
            "-n",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=5",
            "-o", "StrictHostKeyChecking=no",
            hostname,
        ])
        .arg("pgrep -fc '[-]-agent-id' || true")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output();
    match output {
        Ok(o) => String::from_utf8_lossy(&o.stdout)
            .trim()
            .parse()
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Read results NDJSON files, deduplicate by batch_id, and count images per label.
fn load_results(results_dir: &str) -> Vec<(String, u64)> {
    let mut seen_batches: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut label_counts: HashMap<String, u64> = HashMap::new();

    let entries = match std::fs::read_dir(results_dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !name.starts_with("results_") || !name.ends_with(".ndjson") {
            continue;
        }
        if let Ok(content) = std::fs::read_to_string(&path) {
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                    // Deduplicate by batch_id — a batch can appear in multiple
                    // result files when leadership changed mid-run.
                    let batch_id = v["batch_id"].as_u64().unwrap_or(0);
                    if !seen_batches.insert(batch_id) {
                        continue; // already counted this batch
                    }
                    if let Some(labels) = v["labels"].as_array() {
                        for pair in labels {
                            if let Some(label) = pair.get(1).and_then(|l| l.as_str()) {
                                *label_counts.entry(label.to_string()).or_insert(0) += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    let mut result: Vec<(String, u64)> = label_counts.into_iter().collect();
    result.sort_by(|a, b| b.1.cmp(&a.1));
    result
}
