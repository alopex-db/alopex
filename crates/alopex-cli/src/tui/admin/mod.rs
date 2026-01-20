//! Admin TUI entry point.

pub mod actions;

use std::collections::HashSet;
use std::io::{self, Stdout, Write};
use std::path::{Path, PathBuf};
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
use crate::{
    batch::BatchMode,
    cli::{
        ColumnarCommand, DistanceMetric, HnswCommand, IndexCommand, KvCommand, LifecycleCommand,
        OutputFormat, SqlCommand, VectorCommand,
    },
    client::http::HttpClient,
};

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
    target: AdminTarget,
    params: String,
    editing_params: bool,
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
            target: AdminTarget::Sql,
            params: String::new(),
            editing_params: false,
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
            lines.push(Line::from(format!(
                "Target: {} (press t to change)",
                self.target.label()
            )));
            if self.editing_params {
                lines.push(Line::from("Params: editing (Enter to finish)"));
            } else if self.params.is_empty() {
                lines.push(Line::from(
                    "Params: <empty> (press e to edit, key=value ...)",
                ));
            } else {
                lines.push(Line::from(format!("Params: {}", self.params)));
            }
            if let Some(example) = self.target.example_for(item.action) {
                lines.push(Line::from(format!("Example: {example}")));
            }
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
        } else if self.editing_params {
            format!(
                "Connection: {} | Action: {} | Editing params | Enter: done | Esc: cancel",
                self.connection_label, action
            )
        } else {
            format!(
                "Connection: {} | Action: {} | Up/Down: navigate | Enter: execute | t: target | e: edit params | ?: help | q: quit",
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
        if self.editing_params {
            match key.code {
                KeyCode::Esc => {
                    self.editing_params = false;
                }
                KeyCode::Enter => {
                    self.editing_params = false;
                }
                KeyCode::Backspace => {
                    self.params.pop();
                }
                KeyCode::Char(ch) => {
                    self.params.push(ch);
                }
                _ => {}
            }
            return Ok(false);
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(true),
            KeyCode::Char('?') => {
                self.show_help = !self.show_help;
            }
            KeyCode::Char('t') => {
                self.target = self.target.next();
            }
            KeyCode::Char('e') => {
                self.editing_params = true;
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

        let params = parse_params(&self.params);
        let command = match build_command_for(item.action, self.target, &params) {
            Ok(Some(command)) => command,
            Ok(None) => {
                self.last_result = Some(AdminResult::status(
                    "Select target/params to execute".to_string(),
                ));
                return Ok(());
            }
            Err(err) => {
                self.last_result = Some(AdminResult::status(err.to_string()));
                return Ok(());
            }
        };
        let request = AdminRequest {
            action: item.action,
            command,
            limit: self.backend.limit(),
            quiet: self.backend.quiet(),
            ui_mode: UiMode::Batch,
            connection_label: self.connection_label.clone(),
            output: self.backend.output_format(),
            data_dir: self.backend.data_dir().map(PathBuf::from),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminTarget {
    Sql,
    Kv,
    Vector,
    Hnsw,
    Columnar,
}

impl AdminTarget {
    const ORDER: [AdminTarget; 5] = [
        AdminTarget::Sql,
        AdminTarget::Kv,
        AdminTarget::Vector,
        AdminTarget::Hnsw,
        AdminTarget::Columnar,
    ];

    fn next(self) -> Self {
        let index = Self::ORDER
            .iter()
            .position(|target| *target == self)
            .unwrap_or(0);
        let next = (index + 1) % Self::ORDER.len();
        Self::ORDER[next]
    }

    fn label(self) -> &'static str {
        match self {
            AdminTarget::Sql => "SQL",
            AdminTarget::Kv => "KV",
            AdminTarget::Vector => "Vector",
            AdminTarget::Hnsw => "HNSW",
            AdminTarget::Columnar => "Columnar",
        }
    }

    fn example_for(self, action: AdminAction) -> Option<&'static str> {
        match (self, action) {
            (AdminTarget::Sql, _) => Some("query=\"SELECT * FROM table\""),
            (AdminTarget::Kv, AdminAction::Read) => Some("key=mykey OR prefix=app/"),
            (AdminTarget::Kv, AdminAction::Create | AdminAction::Update) => {
                Some("key=mykey value=hello")
            }
            (AdminTarget::Kv, AdminAction::Delete) => Some("key=mykey"),
            (AdminTarget::Vector, AdminAction::Read) => {
                Some("index=myindex query=\"[0.1, 0.2]\" k=10")
            }
            (AdminTarget::Vector, AdminAction::Create | AdminAction::Update) => {
                Some("index=myindex key=item1 vector=\"[0.1, 0.2]\"")
            }
            (AdminTarget::Vector, AdminAction::Delete) => Some("index=myindex key=item1"),
            (AdminTarget::Hnsw, AdminAction::Read) => Some("name=myindex"),
            (AdminTarget::Hnsw, AdminAction::Create) => Some("name=myindex dim=128 metric=cosine"),
            (AdminTarget::Hnsw, AdminAction::Delete) => Some("name=myindex"),
            (AdminTarget::Columnar, AdminAction::Read) => Some("mode=list"),
            (AdminTarget::Columnar, AdminAction::Create) => Some("file=data.csv table=mytable"),
            (AdminTarget::Columnar, AdminAction::Delete) => Some("segment=seg1 column=col1"),
            _ => None,
        }
    }
}

pub enum AdminBackend<'a> {
    Local {
        db: &'a alopex_embedded::Database,
        batch_mode: &'a BatchMode,
        output_format: OutputFormat,
        limit: Option<usize>,
        quiet: bool,
        data_dir: Option<PathBuf>,
    },
    Remote {
        client: &'a HttpClient,
        batch_mode: &'a BatchMode,
        output_format: OutputFormat,
        limit: Option<usize>,
        quiet: bool,
        data_dir: Option<PathBuf>,
    },
}

impl AdminBackend<'_> {
    fn output_format(&self) -> OutputFormat {
        match self {
            AdminBackend::Local { output_format, .. } => *output_format,
            AdminBackend::Remote { output_format, .. } => *output_format,
        }
    }

    fn data_dir(&self) -> Option<&Path> {
        match self {
            AdminBackend::Local { data_dir, .. } => data_dir.as_deref(),
            AdminBackend::Remote { data_dir, .. } => data_dir.as_deref(),
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
        let mut writer = io::stdout().lock();
        return write_non_tty_fallback(&mut writer, context.backend.output_format());
    }

    let app = AdminApp::new(context.connection_label, context.auth, context.backend);
    app.run()
}

pub fn write_non_tty_fallback<W: Write>(writer: &mut W, output_format: OutputFormat) -> Result<()> {
    let mut formatter = create_formatter(output_format);
    let columns = vec![
        Column::new("Status", DataType::Text),
        Column::new("Message", DataType::Text),
    ];
    let rows = vec![Row::new(vec![
        Value::Text("Error".to_string()),
        Value::Text("Admin UI is unavailable without a TTY.".to_string()),
    ])];
    formatter.write_header(writer, &columns)?;
    for row in &rows {
        formatter.write_row(writer, row)?;
    }
    formatter.write_footer(writer)
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
    let _ = action;
    false
}

fn parse_params(input: &str) -> std::collections::HashMap<String, String> {
    let mut params = std::collections::HashMap::new();
    let mut token = String::new();
    let mut in_quotes: Option<char> = None;
    for ch in input.chars() {
        if let Some(quote) = in_quotes {
            if ch == quote {
                in_quotes = None;
            } else {
                token.push(ch);
            }
            continue;
        }
        match ch {
            '"' | '\'' => {
                in_quotes = Some(ch);
            }
            ' ' | '\t' | '\n' | '\r' => {
                push_param_token(&mut params, &token);
                token.clear();
            }
            _ => token.push(ch),
        }
    }
    push_param_token(&mut params, &token);
    params
}

fn push_param_token(params: &mut std::collections::HashMap<String, String>, token: &str) {
    if let Some((key, value)) = token.split_once('=') {
        if !key.is_empty() && !value.is_empty() {
            params.insert(key.to_lowercase(), value.to_string());
        }
    }
}

fn build_command_for(
    action: AdminAction,
    target: AdminTarget,
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<AdminCommand>> {
    if matches!(
        action,
        AdminAction::Archive | AdminAction::Restore | AdminAction::Backup | AdminAction::Export
    ) {
        let command = match action {
            AdminAction::Archive => LifecycleCommand::Archive,
            AdminAction::Restore => LifecycleCommand::Restore,
            AdminAction::Backup => LifecycleCommand::Backup,
            AdminAction::Export => LifecycleCommand::Export,
            _ => return Ok(None),
        };
        return Ok(Some(AdminCommand::Lifecycle(command)));
    }
    match target {
        AdminTarget::Sql => build_sql_command(params),
        AdminTarget::Kv => build_kv_command(action, params),
        AdminTarget::Vector => build_vector_command(action, params),
        AdminTarget::Hnsw => build_hnsw_command(action, params),
        AdminTarget::Columnar => build_columnar_command(action, params),
    }
}

fn build_sql_command(
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<AdminCommand>> {
    let query = params.get("query").cloned();
    if query.is_none() {
        return Ok(None);
    }
    Ok(Some(AdminCommand::Sql(SqlCommand {
        query,
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: false,
    })))
}

fn build_kv_command(
    action: AdminAction,
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<AdminCommand>> {
    match action {
        AdminAction::Read => {
            if let Some(key) = params.get("key") {
                return Ok(Some(AdminCommand::Kv(KvCommand::Get { key: key.clone() })));
            }
            let prefix = params.get("prefix").cloned();
            Ok(Some(AdminCommand::Kv(KvCommand::List { prefix })))
        }
        AdminAction::Create | AdminAction::Update => {
            let key = params.get("key").cloned();
            let value = params.get("value").cloned();
            match (key, value) {
                (Some(key), Some(value)) => {
                    Ok(Some(AdminCommand::Kv(KvCommand::Put { key, value })))
                }
                _ => Ok(None),
            }
        }
        AdminAction::Delete => {
            let key = params.get("key").cloned();
            match key {
                Some(key) => Ok(Some(AdminCommand::Kv(KvCommand::Delete { key }))),
                None => Ok(None),
            }
        }
        AdminAction::Archive | AdminAction::Restore | AdminAction::Backup | AdminAction::Export => {
            Ok(None)
        }
    }
}

fn build_vector_command(
    action: AdminAction,
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<AdminCommand>> {
    let index = params.get("index").cloned();
    match action {
        AdminAction::Read => {
            let query = params.get("query").cloned();
            let index = match index {
                Some(index) => index,
                None => return Ok(None),
            };
            let query = match query {
                Some(query) => query,
                None => return Ok(None),
            };
            let k = params
                .get("k")
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(10);
            Ok(Some(AdminCommand::Vector(VectorCommand::Search {
                index,
                query,
                k,
                progress: false,
            })))
        }
        AdminAction::Create | AdminAction::Update => {
            let key = params.get("key").cloned();
            let vector = params.get("vector").cloned();
            match (index, key, vector) {
                (Some(index), Some(key), Some(vector)) => {
                    Ok(Some(AdminCommand::Vector(VectorCommand::Upsert {
                        index,
                        key,
                        vector,
                    })))
                }
                _ => Ok(None),
            }
        }
        AdminAction::Delete => {
            let key = params.get("key").cloned();
            match (index, key) {
                (Some(index), Some(key)) => Ok(Some(AdminCommand::Vector(VectorCommand::Delete {
                    index,
                    key,
                }))),
                _ => Ok(None),
            }
        }
        AdminAction::Archive | AdminAction::Restore | AdminAction::Backup | AdminAction::Export => {
            Ok(None)
        }
    }
}

fn build_hnsw_command(
    action: AdminAction,
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<AdminCommand>> {
    match action {
        AdminAction::Read => {
            let name = match params.get("name").cloned() {
                Some(name) => name,
                None => return Ok(None),
            };
            Ok(Some(AdminCommand::Hnsw(HnswCommand::Stats { name })))
        }
        AdminAction::Create => {
            let name = match params.get("name").cloned() {
                Some(name) => name,
                None => return Ok(None),
            };
            let dim = match params
                .get("dim")
                .and_then(|value| value.parse::<usize>().ok())
            {
                Some(dim) => dim,
                None => return Ok(None),
            };
            let metric = if let Some(value) = params.get("metric") {
                parse_metric(value).ok_or_else(|| {
                    CliError::InvalidArgument(
                        "Invalid metric. Use metric=cosine|l2|ip.".to_string(),
                    )
                })?
            } else {
                DistanceMetric::Cosine
            };
            Ok(Some(AdminCommand::Hnsw(HnswCommand::Create {
                name,
                dim,
                metric,
            })))
        }
        AdminAction::Delete => {
            let name = match params.get("name").cloned() {
                Some(name) => name,
                None => return Ok(None),
            };
            Ok(Some(AdminCommand::Hnsw(HnswCommand::Drop { name })))
        }
        AdminAction::Update => Err(CliError::InvalidArgument(
            "Update is not supported for HNSW targets.".to_string(),
        )),
        AdminAction::Archive | AdminAction::Restore | AdminAction::Backup | AdminAction::Export => {
            Ok(None)
        }
    }
}

fn build_columnar_command(
    action: AdminAction,
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<AdminCommand>> {
    match action {
        AdminAction::Read => {
            let mode = params
                .get("mode")
                .map(|value| value.as_str())
                .unwrap_or("list");
            match mode {
                "scan" => {
                    let segment = match params.get("segment").cloned() {
                        Some(segment) => segment,
                        None => return Ok(None),
                    };
                    Ok(Some(AdminCommand::Columnar(ColumnarCommand::Scan {
                        segment,
                        progress: false,
                    })))
                }
                "stats" => {
                    let segment = match params.get("segment").cloned() {
                        Some(segment) => segment,
                        None => return Ok(None),
                    };
                    Ok(Some(AdminCommand::Columnar(ColumnarCommand::Stats {
                        segment,
                    })))
                }
                "index_list" => {
                    let segment = match params.get("segment").cloned() {
                        Some(segment) => segment,
                        None => return Ok(None),
                    };
                    Ok(Some(AdminCommand::Columnar(ColumnarCommand::Index(
                        IndexCommand::List { segment },
                    ))))
                }
                "list" => Ok(Some(AdminCommand::Columnar(ColumnarCommand::List))),
                _ => Err(CliError::InvalidArgument(
                    "Unknown columnar mode. Use mode=list|scan|stats|index_list.".to_string(),
                )),
            }
        }
        AdminAction::Create => {
            if let (Some(file), Some(table)) =
                (params.get("file").cloned(), params.get("table").cloned())
            {
                return Ok(Some(AdminCommand::Columnar(ColumnarCommand::Ingest {
                    file: std::path::PathBuf::from(file),
                    table,
                    delimiter: ',',
                    header: true,
                    compression: "lz4".to_string(),
                    row_group_size: None,
                })));
            }
            if let (Some(segment), Some(column), Some(index_type)) = (
                params.get("segment").cloned(),
                params.get("column").cloned(),
                params.get("index_type").cloned(),
            ) {
                return Ok(Some(AdminCommand::Columnar(ColumnarCommand::Index(
                    IndexCommand::Create {
                        segment,
                        column,
                        index_type,
                    },
                ))));
            }
            Ok(None)
        }
        AdminAction::Delete => {
            if let (Some(segment), Some(column)) = (
                params.get("segment").cloned(),
                params.get("column").cloned(),
            ) {
                return Ok(Some(AdminCommand::Columnar(ColumnarCommand::Index(
                    IndexCommand::Drop { segment, column },
                ))));
            }
            Ok(None)
        }
        AdminAction::Update => Err(CliError::InvalidArgument(
            "Update is not supported for columnar targets.".to_string(),
        )),
        AdminAction::Archive | AdminAction::Restore | AdminAction::Backup | AdminAction::Export => {
            Ok(None)
        }
    }
}

fn parse_metric(value: &str) -> Option<DistanceMetric> {
    match value.to_lowercase().as_str() {
        "cosine" => Some(DistanceMetric::Cosine),
        "l2" => Some(DistanceMetric::L2),
        "ip" => Some(DistanceMetric::Ip),
        _ => None,
    }
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
        "t: change target",
        "e: edit params",
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
