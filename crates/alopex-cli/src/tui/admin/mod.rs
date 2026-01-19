//! Admin TUI entry point.

pub mod actions;

use std::collections::HashSet;
use std::io::{self, Stdout};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Terminal;
use tokio::runtime::Runtime;

use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::{create_formatter, Formatter};
use crate::ui::mode::UiMode;
use crate::{batch::BatchMode, cli::OutputFormat, client::http::HttpClient};

use self::actions::{
    all_actions, execute_local_action, execute_remote_action, AdminAction, AdminCommand,
    AdminRequest,
};
use super::is_tty;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthScope {
    Full,
    Restricted,
}

#[derive(Debug, Clone)]
pub struct AuthCapabilities {
    scope: AuthScope,
    allowed_actions: HashSet<AdminAction>,
}

impl AuthCapabilities {
    pub fn full() -> Self {
        Self {
            scope: AuthScope::Full,
            allowed_actions: HashSet::new(),
        }
    }

    #[allow(dead_code)]
    pub fn restricted(allowed_actions: HashSet<AdminAction>) -> Self {
        Self {
            scope: AuthScope::Restricted,
            allowed_actions,
        }
    }

    pub fn restricted_all() -> Self {
        Self {
            scope: AuthScope::Restricted,
            allowed_actions: all_actions(),
        }
    }

    fn allows(&self, action: AdminAction) -> bool {
        match self.scope {
            AuthScope::Full => true,
            AuthScope::Restricted => self.allowed_actions.contains(&action),
        }
    }
}

#[derive(Debug, Clone)]
struct AdminItem {
    action: AdminAction,
    title: &'static str,
    description: &'static str,
    enabled: bool,
}

struct AdminApp<'a> {
    items: Vec<AdminItem>,
    selected: usize,
    show_help: bool,
    connection_label: String,
    backend: AdminBackend<'a>,
    last_result: Option<AdminResult>,
}

impl<'a> AdminApp<'a> {
    fn new(
        connection_label: impl Into<String>,
        auth: AuthCapabilities,
        backend: AdminBackend<'a>,
    ) -> Self {
        let mut items = default_items();
        for item in &mut items {
            item.enabled = auth.allows(item.action);
        }
        Self {
            items,
            selected: 0,
            show_help: false,
            connection_label: connection_label.into(),
            backend,
            last_result: None,
        }
    }

    fn run(mut self) -> Result<()> {
        if !is_tty() {
            return Err(CliError::InvalidArgument(
                "TUI requires a TTY. Run without --tui in batch mode.".to_string(),
            ));
        }
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;

        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;
        terminal.clear()?;

        let tick_rate = Duration::from_millis(16);
        let mut last_tick = Instant::now();

        loop {
            terminal.draw(|frame| self.draw(frame))?;

            let timeout = tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or_else(|| Duration::from_secs(0));

            if event::poll(timeout)? {
                if let Event::Key(key) = event::read()? {
                    if self.handle_key(key)? {
                        break;
                    }
                }
            }

            if last_tick.elapsed() >= tick_rate {
                last_tick = Instant::now();
            }
        }

        cleanup_terminal(terminal)
    }

    fn draw(&mut self, frame: &mut ratatui::Frame<'_>) {
        let area = frame.size();
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(5), Constraint::Length(3)])
            .split(area);

        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(26), Constraint::Min(10)])
            .split(chunks[0]);

        self.render_nav(frame, main_chunks[0]);
        self.render_detail(frame, main_chunks[1]);
        self.render_status(frame, chunks[1]);

        if self.show_help {
            render_help(frame, area);
        }
    }

    fn render_nav(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let items = self
            .items
            .iter()
            .map(|item| {
                let label = if item.enabled {
                    item.title.to_string()
                } else {
                    format!("{} (locked)", item.title)
                };
                ListItem::new(Line::from(Span::raw(label)))
            })
            .collect::<Vec<_>>();

        let mut state = ListState::default();
        state.select(Some(self.selected));

        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title("Lifecycle"))
            .highlight_style(
                Style::default()
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        frame.render_stateful_widget(list, area, &mut state);
    }

    fn render_detail(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let selected = self.items.get(self.selected);
        let mut lines = Vec::new();
        if let Some(item) = selected {
            lines.push(Line::from(vec![Span::styled(
                item.title,
                Style::default().add_modifier(Modifier::BOLD),
            )]));
            lines.push(Line::from(""));
            lines.push(Line::from(item.description));
            lines.push(Line::from(""));
            if !item.enabled {
                lines.push(Line::from(Span::styled(
                    "Disabled: your current authorization does not allow this action.",
                    Style::default().fg(Color::Red),
                )));
            } else if is_not_implemented(item.action) {
                lines.push(Line::from(Span::styled(
                    "Status: Not implemented yet.",
                    Style::default().fg(Color::Yellow),
                )));
            } else {
                lines.push(Line::from(Span::styled(
                    "Status: Ready.",
                    Style::default().fg(Color::Green),
                )));
            }
        }
        if let Some(result) = &self.last_result {
            append_result_lines(&mut lines, result);
        }

        let paragraph = Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title("Action"))
            .wrap(Wrap { trim: true });
        frame.render_widget(paragraph, area);
    }

    fn render_status(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let action = self
            .items
            .get(self.selected)
            .map(|item| item.title)
            .unwrap_or("-");
        let status_text = if self.show_help {
            format!(
                "Connection: {} | Action: {} | Help: press ? to close",
                self.connection_label, action
            )
        } else {
            format!(
                "Connection: {} | Action: {} | Up/Down: navigate | Enter: select | ?: help | q: quit",
                self.connection_label, action
            )
        };

        let paragraph = Paragraph::new(status_text)
            .block(Block::default().borders(Borders::ALL).title("Status"))
            .style(Style::default().fg(Color::Gray))
            .wrap(Wrap { trim: true });
        frame.render_widget(paragraph, area);
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(true),
            KeyCode::Char('?') => {
                self.show_help = !self.show_help;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.selected > 0 {
                    self.selected -= 1;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.selected + 1 < self.items.len() {
                    self.selected += 1;
                }
            }
            KeyCode::Enter => {
                self.execute_selected_action()?;
            }
            _ => {}
        }
        Ok(false)
    }

    fn execute_selected_action(&mut self) -> Result<()> {
        let Some(item) = self.items.get(self.selected) else {
            return Ok(());
        };
        if !item.enabled {
            self.last_result = Some(AdminResult::status(format!(
                "Action '{}' is not permitted.",
                item.title
            )));
            return Ok(());
        }

        let command = default_command_for(item.action);
        let request = AdminRequest {
            action: item.action,
            command,
            limit: self.backend.limit(),
            quiet: self.backend.quiet(),
            ui_mode: UiMode::Batch,
            connection_label: self.connection_label.clone(),
            output: self.backend.output_format(),
        };

        let (formatter, state) = CaptureFormatter::new();
        let mut sink = io::sink();
        let result = match &self.backend {
            AdminBackend::Local { db, batch_mode, .. } => {
                execute_local_action(db, batch_mode, request, &mut sink, Box::new(formatter))
            }
            AdminBackend::Remote {
                client, batch_mode, ..
            } => {
                let runtime = Runtime::new().map_err(|err| {
                    CliError::InvalidArgument(format!("Failed to start async runtime: {err}"))
                })?;
                runtime.block_on(execute_remote_action(
                    client,
                    batch_mode,
                    request,
                    &mut sink,
                    Box::new(formatter),
                ))
            }
        };

        let capture = state.lock().expect("admin capture lock");
        let mut result_state = AdminResult {
            columns: capture.columns.clone(),
            rows: capture.rows.clone(),
            status_message: None,
        };

        if let Err(err) = result {
            result_state.status_message = Some(err.to_string());
        } else if result_state.columns.is_empty() && result_state.rows.is_empty() {
            result_state.status_message = Some("OK".to_string());
        }

        self.last_result = Some(result_state);
        Ok(())
    }
}

pub enum AdminBackend<'a> {
    Local {
        db: &'a alopex_embedded::Database,
        batch_mode: &'a BatchMode,
        output_format: OutputFormat,
        limit: Option<usize>,
        quiet: bool,
    },
    Remote {
        client: &'a HttpClient,
        batch_mode: &'a BatchMode,
        output_format: OutputFormat,
        limit: Option<usize>,
        quiet: bool,
    },
}

impl AdminBackend<'_> {
    fn output_format(&self) -> OutputFormat {
        match self {
            AdminBackend::Local { output_format, .. } => *output_format,
            AdminBackend::Remote { output_format, .. } => *output_format,
        }
    }

    fn limit(&self) -> Option<usize> {
        match self {
            AdminBackend::Local { limit, .. } => *limit,
            AdminBackend::Remote { limit, .. } => *limit,
        }
    }

    fn quiet(&self) -> bool {
        match self {
            AdminBackend::Local { quiet, .. } => *quiet,
            AdminBackend::Remote { quiet, .. } => *quiet,
        }
    }
}

pub struct AdminContext<'a> {
    pub connection_label: String,
    pub auth: AuthCapabilities,
    pub backend: AdminBackend<'a>,
}

pub fn run_admin_ui(context: AdminContext<'_>) -> Result<()> {
    if !is_tty() {
        let mut formatter = create_formatter(OutputFormat::Table);
        let mut writer = io::stdout().lock();
        let columns = vec![
            Column::new("Status", DataType::Text),
            Column::new("Message", DataType::Text),
        ];
        let rows = vec![Row::new(vec![
            Value::Text("Error".to_string()),
            Value::Text("Admin UI is unavailable without a TTY.".to_string()),
        ])];
        formatter.write_header(&mut writer, &columns)?;
        for row in &rows {
            formatter.write_row(&mut writer, row)?;
        }
        formatter.write_footer(&mut writer)?;
        return Ok(());
    }

    let app = AdminApp::new(context.connection_label, context.auth, context.backend);
    app.run()
}

fn default_items() -> Vec<AdminItem> {
    vec![
        AdminItem {
            action: AdminAction::Read,
            title: "Read / List",
            description: "Browse or query data across databases, tables, and indexes.",
            enabled: true,
        },
        AdminItem {
            action: AdminAction::Create,
            title: "Create",
            description: "Create new databases, tables, indexes, or data objects.",
            enabled: true,
        },
        AdminItem {
            action: AdminAction::Update,
            title: "Update",
            description: "Modify existing records, schemas, or index settings.",
            enabled: true,
        },
        AdminItem {
            action: AdminAction::Delete,
            title: "Delete",
            description: "Remove records, tables, indexes, or data sets.",
            enabled: true,
        },
        AdminItem {
            action: AdminAction::Archive,
            title: "Archive",
            description: "Move data into an archived state for long-term retention.",
            enabled: true,
        },
        AdminItem {
            action: AdminAction::Restore,
            title: "Restore",
            description: "Restore archived data into an active state.",
            enabled: true,
        },
        AdminItem {
            action: AdminAction::Backup,
            title: "Backup",
            description: "Create snapshots or backups of data and metadata.",
            enabled: true,
        },
        AdminItem {
            action: AdminAction::Export,
            title: "Export",
            description: "Export data for external systems or offline analysis.",
            enabled: true,
        },
    ]
}

fn is_not_implemented(action: AdminAction) -> bool {
    matches!(
        action,
        AdminAction::Archive | AdminAction::Restore | AdminAction::Backup | AdminAction::Export
    )
}

fn default_command_for(action: AdminAction) -> AdminCommand {
    let sql = if matches!(action, AdminAction::Read) {
        Some("SELECT 1".to_string())
    } else {
        None
    };
    AdminCommand::Sql(crate::cli::SqlCommand {
        query: sql,
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    })
}

fn render_help(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let help_width = area.width.saturating_sub(4).min(60);
    let help_height = area.height.saturating_sub(4).min(12);
    let rect = Rect::new(
        area.x + (area.width.saturating_sub(help_width)) / 2,
        area.y + (area.height.saturating_sub(help_height)) / 2,
        help_width,
        help_height,
    );

    let lines = [
        "Up/Down or k/j: navigate",
        "Enter: execute action",
        "?: toggle help",
        "q/Esc: quit",
    ]
    .join("\n");

    let help = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Help"))
        .wrap(Wrap { trim: true });
    frame.render_widget(help, rect);
}

fn cleanup_terminal(mut terminal: Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

#[derive(Debug, Default, Clone)]
struct AdminResult {
    columns: Vec<Column>,
    rows: Vec<Row>,
    status_message: Option<String>,
}

impl AdminResult {
    fn status(message: String) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            status_message: Some(message),
        }
    }
}

#[derive(Default)]
struct CaptureState {
    columns: Vec<Column>,
    rows: Vec<Row>,
}

struct CaptureFormatter {
    state: Arc<Mutex<CaptureState>>,
}

impl CaptureFormatter {
    fn new() -> (Self, Arc<Mutex<CaptureState>>) {
        let state = Arc::new(Mutex::new(CaptureState::default()));
        (
            Self {
                state: Arc::clone(&state),
            },
            state,
        )
    }
}

impl Formatter for CaptureFormatter {
    fn write_header(&mut self, _writer: &mut dyn std::io::Write, columns: &[Column]) -> Result<()> {
        let mut state = self.state.lock().expect("admin capture lock");
        state.columns = columns.to_vec();
        Ok(())
    }

    fn write_row(&mut self, _writer: &mut dyn std::io::Write, row: &Row) -> Result<()> {
        self.state
            .lock()
            .expect("admin capture lock")
            .rows
            .push(row.clone());
        Ok(())
    }

    fn write_footer(&mut self, _writer: &mut dyn std::io::Write) -> Result<()> {
        Ok(())
    }

    fn supports_streaming(&self) -> bool {
        true
    }
}

fn append_result_lines(lines: &mut Vec<Line<'static>>, result: &AdminResult) {
    if result.columns.is_empty() && result.rows.is_empty() && result.status_message.is_none() {
        return;
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Last Result",
        Style::default().add_modifier(Modifier::BOLD),
    )));
    if let Some(message) = &result.status_message {
        lines.push(Line::from(message.clone()));
    }
    if !result.columns.is_empty() {
        let header = result
            .columns
            .iter()
            .map(|col| col.name.clone())
            .collect::<Vec<_>>()
            .join(" | ");
        lines.push(Line::from(header));
        for row in &result.rows {
            let row_text = row
                .columns
                .iter()
                .map(value_to_string)
                .collect::<Vec<_>>()
                .join(" | ");
            lines.push(Line::from(row_text));
        }
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{f:.6}"),
        Value::Text(text) => text.clone(),
        Value::Bytes(bytes) => format!("{:02x?}", bytes),
        Value::Vector(values) => format!(
            "[{}]",
            values
                .iter()
                .take(4)
                .map(|value| format!("{value:.4}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}
