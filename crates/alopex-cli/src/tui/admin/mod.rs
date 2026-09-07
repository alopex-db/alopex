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
use tokio::runtime::{Handle, Runtime};

use alopex_embedded::{CreateCatalogRequest, CreateNamespaceRequest};

use crate::client::admin_resources::{fetch_admin_resources, AdminResourcesRequest};
use crate::client::http::ClientError;
use crate::error::{CliError, Result};
use crate::models::{Column, DataType, Row, Value};
use crate::output::formatter::{create_formatter, Formatter};
use crate::ui::mode::UiMode;
use crate::{
    batch::BatchMode,
    cli::{
        ColumnarCommand, DistanceMetric, HnswCommand, IndexCommand, KvCommand,
        LifecycleBackupCommand, LifecycleCommand, LifecycleRestoreCommand, OutputFormat,
        SqlCommand, VectorCommand,
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

    pub fn allows(&self, action: AdminAction) -> bool {
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
    form_fields: Vec<AdminFormField>,
    active_field: usize,
    use_raw_params: bool,
    input_mode: AdminInputMode,
    last_action: Option<AdminAction>,
    selection: Option<SelectionOverlay>,
    focus: AdminFocus,
    resources: ResourceTree,
    preview_scroll: usize,
}

impl<'a> AdminApp<'a> {
    fn new(
        connection_label: impl Into<String>,
        auth: AuthCapabilities,
        backend: AdminBackend<'a>,
        initial_target: Option<AdminTarget>,
    ) -> Self {
        let mut items = default_items();
        for item in &mut items {
            item.enabled = auth.allows(item.action);
        }
        let target = initial_target.unwrap_or(AdminTarget::Sql);
        let selected_action = items.first().map(|item| item.action);
        let form_fields = selected_action
            .map(|action| build_form_fields(target, action))
            .unwrap_or_default();
        let resources = ResourceTree::new(&backend);
        let last_result = if let Some(err) = resources.last_error.as_ref() {
            Some(AdminResult::status(format!("Resource load failed: {err}")))
        } else {
            resources
                .last_status
                .as_ref()
                .map(|message| AdminResult::status(message.clone()))
        };
        Self {
            items,
            selected: 0,
            show_help: false,
            connection_label: connection_label.into(),
            backend,
            last_result,
            target,
            params: String::new(),
            form_fields,
            active_field: 0,
            use_raw_params: false,
            input_mode: AdminInputMode::Normal,
            last_action: selected_action,
            selection: None,
            focus: AdminFocus::Table,
            resources,
            preview_scroll: 0,
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

        let mut should_exit = false;
        while !should_exit {
            terminal.draw(|frame| self.draw(frame))?;

            let timeout = tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or_else(|| Duration::from_secs(0));

            if event::poll(timeout)? {
                loop {
                    if let Event::Key(key) = event::read()? {
                        if self.handle_key(key)? {
                            should_exit = true;
                            break;
                        }
                    }
                    if !event::poll(Duration::from_millis(0))? {
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
        let area = frame.area();
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(5), Constraint::Length(3)])
            .split(area);

        let root_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(25), Constraint::Percentage(75)])
            .split(chunks[0]);

        let right_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
            .split(root_layout[1]);

        self.render_resources(frame, root_layout[0]);
        self.render_input(frame, right_layout[0]);
        self.render_preview(frame, right_layout[1]);
        self.render_status(frame, chunks[1]);

        if self.show_help {
            render_help(frame, area);
        }
        if let Some(selection) = &self.selection {
            render_selection_overlay(frame, area, selection);
        }
    }

    fn render_action_list(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
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
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Actions")
                    .border_style(self.focus_style(AdminFocus::Detail)),
            )
            .highlight_style(
                Style::default()
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        frame.render_stateful_widget(list, area, &mut state);
    }

    fn render_input(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(10), Constraint::Min(6)])
            .split(area);

        self.render_action_list(frame, layout[0]);

        let detail_area = layout[1];
        let selected = self.items.get(self.selected);
        let mut lines = Vec::new();
        if let Some(item) = selected {
            lines.push(Line::from(vec![Span::styled(
                item.title,
                Style::default().add_modifier(Modifier::BOLD),
            )]));
            lines.push(Line::from(""));
            lines.push(Line::from(format!("Target: {}", self.target.label())));
            match self.input_mode {
                AdminInputMode::EditingField => {
                    lines.push(Line::from("Mode: editing field (Enter/Esc to finish)"));
                }
                AdminInputMode::EditingRaw => {
                    lines.push(Line::from("Mode: editing raw params (Enter/Esc to finish)"));
                }
                AdminInputMode::Normal => {}
            }
            if self.use_raw_params {
                lines.push(Line::from("Input: raw parameters (press r to switch)"));
                let line = if self.params.is_empty() {
                    "Params: <empty> (press e to edit)".to_string()
                } else {
                    format!("Params: {}", self.params)
                };
                lines.push(Line::from(line));
                if let Some(example) = self.target.example_for(item.action) {
                    lines.push(Line::from(format!("Example: {example}")));
                }
            } else {
                lines.push(Line::from(
                    "Input: guided fields (Tab to move, e to edit, o to list)",
                ));
                for (idx, field) in self.form_fields.iter().enumerate() {
                    let marker = if idx == self.active_field { ">" } else { " " };
                    let value = if field.value.is_empty() {
                        Span::styled(
                            field.placeholder.to_string(),
                            Style::default().fg(Color::DarkGray),
                        )
                    } else {
                        Span::raw(field.value.clone())
                    };
                    let required = if field.required { " *" } else { "" };
                    let list_hint = if field.list_source.is_some() {
                        Span::styled(" (o)", Style::default().fg(Color::Blue))
                    } else {
                        Span::raw("")
                    };
                    lines.push(Line::from(vec![
                        Span::raw(format!("{marker} ")),
                        Span::styled(
                            format!("{}{}", field.label, required),
                            Style::default().add_modifier(Modifier::BOLD),
                        ),
                        Span::raw(": "),
                        value,
                        list_hint,
                    ]));
                }
            }
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
        let paragraph = Paragraph::new(lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Detail")
                    .border_style(self.focus_style(AdminFocus::Detail)),
            )
            .wrap(Wrap { trim: true });
        frame.render_widget(paragraph, detail_area);
    }

    fn focus_style(&self, focus: AdminFocus) -> Style {
        if self.focus == focus {
            Style::default().fg(Color::Green)
        } else {
            Style::default()
        }
    }

    fn preview_line_count(&self) -> usize {
        let mut lines = Vec::new();
        if let Some(result) = &self.last_result {
            append_result_lines(&mut lines, result);
        } else {
            lines.push(Line::from("No results yet."));
        }
        lines.len()
    }

    fn render_resources(&mut self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        self.resources.ensure_selection_in_range();
        let layout = if self.resources.search.is_some() {
            Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(1), Constraint::Min(3)])
                .split(area)
        } else {
            Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(3)])
                .split(area)
        };

        if let Some(search) = self.resources.search.as_ref() {
            let search_text = format!("/ {search}");
            let style = if self.resources.search_focused {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::Gray)
            };
            frame.render_widget(
                Paragraph::new(search_text)
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Resources")
                            .border_style(self.focus_style(AdminFocus::Table)),
                    )
                    .style(style),
                layout[0],
            );
        }

        let list_area = if layout.len() == 1 {
            layout[0]
        } else {
            layout[1]
        };
        let entries = self.resources.filtered_entries();
        let items = if entries.is_empty() {
            vec![ListItem::new(Line::from("No resources found."))]
        } else {
            entries
                .iter()
                .map(|entry| {
                    let indent = "  ".repeat(entry.depth);
                    let mut line = format!("{indent}{}", entry.label);
                    if !entry.selectable {
                        line = line.to_string();
                    }
                    let style = if entry.selectable {
                        Style::default()
                    } else {
                        Style::default().fg(Color::DarkGray)
                    };
                    ListItem::new(Line::from(Span::styled(line, style)))
                })
                .collect::<Vec<_>>()
        };

        let mut state = ListState::default();
        state.select(Some(self.resources.selected));
        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(if self.resources.search.is_some() {
                        ""
                    } else {
                        "Resources"
                    })
                    .border_style(self.focus_style(AdminFocus::Table)),
            )
            .highlight_style(
                Style::default()
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("> ");

        frame.render_stateful_widget(list, list_area, &mut state);
    }

    fn render_preview(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let mut lines = Vec::new();
        if let Some(result) = &self.last_result {
            append_result_lines(&mut lines, result);
        } else {
            lines.push(Line::from("No results yet."));
        }

        let height = area.height.saturating_sub(2) as usize;
        let start = self.preview_scroll.min(lines.len());
        let end = (start + height).min(lines.len());
        let view = lines[start..end].to_vec();

        let paragraph = Paragraph::new(view)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Status")
                    .border_style(self.focus_style(AdminFocus::Status)),
            )
            .wrap(Wrap { trim: true });
        frame.render_widget(paragraph, area);
    }

    fn render_status(&self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let action = self
            .items
            .get(self.selected)
            .map(|item| item.title)
            .unwrap_or("-");
        let focus_label = match self.focus {
            AdminFocus::Table => "Table",
            AdminFocus::Detail => "Detail",
            AdminFocus::Status => "Status",
        };
        let highlight = Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD);

        let mut spans = Vec::new();
        let push_sep = |spans: &mut Vec<Span<'_>>| {
            spans.push(Span::raw(" | "));
        };

        spans.push(Span::raw("Connection: "));
        spans.push(Span::styled(self.connection_label.to_string(), highlight));
        push_sep(&mut spans);
        spans.push(Span::raw("Focus: "));
        spans.push(Span::styled(focus_label.to_string(), highlight));
        push_sep(&mut spans);
        spans.push(Span::raw("Action: "));
        spans.push(Span::styled(action.to_string(), highlight));

        let mut mode_label = None;
        if self.show_help {
            mode_label = Some("Help");
        } else if self.selection.is_some() {
            mode_label = Some("Selecting option");
        } else if self.input_mode == AdminInputMode::EditingField {
            mode_label = Some("Editing field");
        } else if self.input_mode == AdminInputMode::EditingRaw {
            mode_label = Some("Editing raw params");
        }

        if let Some(mode) = mode_label {
            push_sep(&mut spans);
            spans.push(Span::raw(format!("Mode: {mode}")));
        }

        let (ops_text, move_text) = if self.show_help {
            ("?: close".to_string(), "-".to_string())
        } else if self.selection.is_some() {
            (
                "Enter: choose, /: search, Esc: cancel".to_string(),
                "j/k, g/G, Ctrl+d/u".to_string(),
            )
        } else if matches!(
            self.input_mode,
            AdminInputMode::EditingField | AdminInputMode::EditingRaw
        ) {
            ("Enter: done, Esc: cancel".to_string(), "-".to_string())
        } else {
            match self.focus {
                AdminFocus::Table => (
                    "Enter: select, e: edit, r: raw, R: refresh, a: back, ?: help, q: quit"
                        .to_string(),
                    "j/k, g/G, Ctrl+d/u, h/l".to_string(),
                ),
                AdminFocus::Detail => (
                    "Enter: execute, e: edit, o: list, r: raw, a: back, ?: help, q: quit"
                        .to_string(),
                    "Up/Down, Tab, h/l".to_string(),
                ),
                AdminFocus::Status => (
                    "a: back, ?: help, q: quit".to_string(),
                    "j/k, g/G, Ctrl+d/u, h".to_string(),
                ),
            }
        };

        push_sep(&mut spans);
        spans.push(Span::styled(format!("Ops: {ops_text}"), highlight));
        push_sep(&mut spans);
        if move_text == "-" {
            spans.push(Span::raw("Move: -"));
        } else {
            spans.push(Span::raw(format!("Move: {move_text}")));
        }

        let paragraph = Paragraph::new(Line::from(spans))
            .block(Block::default().borders(Borders::ALL).title("Status"))
            .style(Style::default().fg(Color::Gray))
            .wrap(Wrap { trim: true });
        frame.render_widget(paragraph, area);
    }

    fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        if let Some(selection) = &mut self.selection {
            if selection.search_focused {
                match key.code {
                    KeyCode::Esc => selection.reset_search(),
                    KeyCode::Enter => selection.search_focused = false,
                    KeyCode::Backspace => selection.pop_search(),
                    KeyCode::Char(ch) => selection.push_search(ch),
                    _ => {}
                }
                return Ok(false);
            }
            match key.code {
                KeyCode::Esc => {
                    self.selection = None;
                }
                KeyCode::Enter => {
                    if let Some(value) = selection.selected_value() {
                        if let Some(field) = self.form_fields.get_mut(selection.field_index) {
                            field.value = value;
                        }
                    }
                    self.selection = None;
                }
                KeyCode::Char('/') => {
                    selection.search_focused = true;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    selection.move_up();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    selection.move_down();
                }
                KeyCode::Char('g') => {
                    selection.move_top();
                }
                KeyCode::Char('G') => {
                    selection.move_bottom();
                }
                _ => {}
            }
            return Ok(false);
        }
        match self.input_mode {
            AdminInputMode::EditingField => {
                match key.code {
                    KeyCode::Esc | KeyCode::Enter => {
                        self.input_mode = AdminInputMode::Normal;
                    }
                    KeyCode::Backspace => {
                        if let Some(field) = self.form_fields.get_mut(self.active_field) {
                            field.value.pop();
                        }
                    }
                    KeyCode::Char(ch) => {
                        if let Some(field) = self.form_fields.get_mut(self.active_field) {
                            field.value.push(ch);
                        }
                    }
                    _ => {}
                }
                return Ok(false);
            }
            AdminInputMode::EditingRaw => {
                match key.code {
                    KeyCode::Esc | KeyCode::Enter => {
                        self.input_mode = AdminInputMode::Normal;
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
            AdminInputMode::Normal => {}
        }

        if matches!(self.focus, AdminFocus::Table)
            && self.resources.search_focused
            && key.code == KeyCode::Esc
        {
            self.resources.reset_search();
            return Ok(false);
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Char('a') | KeyCode::Esc => return Ok(true),
            KeyCode::Char('?') => {
                self.show_help = !self.show_help;
                return Ok(false);
            }
            KeyCode::Char('h') | KeyCode::Left => {
                self.focus = self.focus_left();
                return Ok(false);
            }
            KeyCode::Char('l') | KeyCode::Right => {
                self.focus = self.focus_right();
                return Ok(false);
            }
            _ => {}
        }

        match self.focus {
            AdminFocus::Table => {
                if self.resources.search_focused {
                    match key.code {
                        KeyCode::Esc => self.resources.reset_search(),
                        KeyCode::Enter => self.resources.search_focused = false,
                        KeyCode::Backspace => self.resources.pop_search(),
                        KeyCode::Char(ch) => self.resources.push_search(ch),
                        _ => {}
                    }
                    return Ok(false);
                }
                match key.code {
                    KeyCode::Char('e') => {
                        self.focus = AdminFocus::Detail;
                        self.input_mode = if self.use_raw_params {
                            AdminInputMode::EditingRaw
                        } else {
                            AdminInputMode::EditingField
                        };
                    }
                    KeyCode::Char('r') => {
                        self.use_raw_params = !self.use_raw_params;
                        self.focus = AdminFocus::Detail;
                        self.input_mode = if self.use_raw_params {
                            AdminInputMode::EditingRaw
                        } else {
                            AdminInputMode::Normal
                        };
                    }
                    KeyCode::Char('/') => {
                        self.resources.search_focused = true;
                        if self.resources.search.is_none() {
                            self.resources.search = Some(String::new());
                        }
                    }
                    KeyCode::Char('R') => {
                        self.resources.reload(&self.backend);
                        if let Some(err) = self.resources.last_error.clone() {
                            self.last_result =
                                Some(AdminResult::status(format!("Resource load failed: {err}")));
                        } else if let Some(status) = self.resources.last_status.clone() {
                            self.last_result = Some(AdminResult::status(status));
                        }
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        self.resources.move_up();
                        self.sync_target_from_resource();
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        self.resources.move_down();
                        self.sync_target_from_resource();
                    }
                    KeyCode::Char('g') => {
                        self.resources.move_top();
                        self.sync_target_from_resource();
                    }
                    KeyCode::Char('G') => {
                        self.resources.move_bottom();
                        self.sync_target_from_resource();
                    }
                    KeyCode::Char('d') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                        self.resources.page_down();
                        self.sync_target_from_resource();
                    }
                    KeyCode::Char('u') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                        self.resources.page_up();
                        self.sync_target_from_resource();
                    }
                    KeyCode::Enter => {
                        self.apply_resource_selection()?;
                    }
                    _ => {}
                }
            }
            AdminFocus::Detail => match key.code {
                KeyCode::Char(ch) if ch.is_ascii_digit() => {
                    let idx = ch.to_digit(10).unwrap_or(0) as usize;
                    if idx > 0 && idx <= self.items.len() {
                        self.selected = idx - 1;
                        self.refresh_form_for_selection();
                    }
                }
                KeyCode::Char('e') => {
                    self.input_mode = if self.use_raw_params {
                        AdminInputMode::EditingRaw
                    } else {
                        AdminInputMode::EditingField
                    };
                }
                KeyCode::Char('r') => {
                    self.use_raw_params = !self.use_raw_params;
                    self.input_mode = if self.use_raw_params {
                        AdminInputMode::EditingRaw
                    } else {
                        AdminInputMode::Normal
                    };
                }
                KeyCode::Char('o') => {
                    self.open_selection_for_active_field()?;
                }
                KeyCode::Tab => {
                    if !self.use_raw_params && !self.form_fields.is_empty() {
                        self.active_field = (self.active_field + 1) % self.form_fields.len();
                    }
                }
                KeyCode::BackTab => {
                    if !self.use_raw_params && !self.form_fields.is_empty() {
                        if self.active_field == 0 {
                            self.active_field = self.form_fields.len() - 1;
                        } else {
                            self.active_field -= 1;
                        }
                    }
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if self.selected > 0 {
                        self.selected -= 1;
                        self.refresh_form_for_selection();
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if self.selected + 1 < self.items.len() {
                        self.selected += 1;
                        self.refresh_form_for_selection();
                    }
                }
                KeyCode::Enter => {
                    self.execute_selected_action()?;
                }
                _ => {}
            },
            AdminFocus::Status => match key.code {
                KeyCode::Up | KeyCode::Char('k') => {
                    self.preview_scroll = self.preview_scroll.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    let max = self.preview_line_count().saturating_sub(1);
                    self.preview_scroll = (self.preview_scroll + 1).min(max);
                }
                KeyCode::Char('g') => {
                    self.preview_scroll = 0;
                }
                KeyCode::Char('G') => {
                    self.preview_scroll = self.preview_line_count().saturating_sub(1);
                }
                KeyCode::Char('d') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                    self.preview_scroll =
                        (self.preview_scroll + 5).min(self.preview_line_count().saturating_sub(1));
                }
                KeyCode::Char('u') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                    self.preview_scroll = self.preview_scroll.saturating_sub(5);
                }
                _ => {}
            },
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

        let params = if self.use_raw_params {
            parse_params(&self.params)
        } else {
            build_params_from_fields(&self.form_fields)
        };
        if let Err(message) = validate_params(item.action, self.target, &params) {
            self.last_result = Some(AdminResult::status(message));
            return Ok(());
        }
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

        let state = CaptureFormatter::shared_state();
        let mut make_formatter = CaptureFormatter::factory(&state);
        let mut sink = io::sink();
        let result = match &self.backend {
            AdminBackend::Local { db, batch_mode, .. } => {
                execute_local_action(db, batch_mode, request, &mut sink, &mut make_formatter)
            }
            AdminBackend::Remote {
                client, batch_mode, ..
            } => block_on_with_runtime(execute_remote_action(
                client,
                batch_mode,
                request,
                &mut sink,
                &mut make_formatter,
            ))?,
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
        self.preview_scroll = 0;
        Ok(())
    }

    fn refresh_form_for_selection(&mut self) {
        let action = self.items.get(self.selected).map(|item| item.action);
        if action != self.last_action {
            self.last_action = action;
            self.reset_form();
        }
    }

    fn reset_form(&mut self) {
        if let Some(action) = self.items.get(self.selected).map(|item| item.action) {
            self.form_fields = build_form_fields(self.target, action);
            self.active_field = 0;
            self.input_mode = AdminInputMode::Normal;
            self.use_raw_params = false;
            self.last_action = Some(action);
            self.selection = None;
        }
    }

    fn focus_left(&self) -> AdminFocus {
        match self.focus {
            AdminFocus::Table => AdminFocus::Table,
            AdminFocus::Detail => AdminFocus::Table,
            AdminFocus::Status => AdminFocus::Detail,
        }
    }

    fn focus_right(&self) -> AdminFocus {
        match self.focus {
            AdminFocus::Table => AdminFocus::Detail,
            AdminFocus::Detail => AdminFocus::Status,
            AdminFocus::Status => AdminFocus::Status,
        }
    }

    fn apply_resource_selection(&mut self) -> Result<()> {
        let Some(entry) = self.resources.selected_entry() else {
            return Ok(());
        };
        if !entry.selectable {
            if let Some(target) = target_for_resource(&entry) {
                self.ensure_target(target);
            }
            return Ok(());
        }
        match entry.kind {
            ResourceKind::Section(section) => {
                if let Some(target) = section.target() {
                    self.ensure_target(target);
                }
            }
            ResourceKind::Table { name } => {
                self.ensure_target(AdminTarget::Sql);
                if self.set_field_value("table", &name, false).is_none() {
                    let query = format!("SELECT * FROM {name}");
                    if self.set_field_value("query", &query, false).is_none() {
                        self.last_result = Some(AdminResult::status(
                            "No matching field for table.".to_string(),
                        ));
                    }
                }
            }
            ResourceKind::Column { table, name } => {
                self.ensure_target(AdminTarget::Sql);
                let _ = self.set_field_value("table", &table, false);
                if self.set_field_value("columns", &name, true).is_none() {
                    self.last_result = Some(AdminResult::status(
                        "No matching field for column.".to_string(),
                    ));
                }
            }
            ResourceKind::KvKey { key } => {
                self.ensure_target(AdminTarget::Kv);
                if self.set_field_value("key", &key, false).is_none() {
                    self.last_result = Some(AdminResult::status(
                        "No matching field for key.".to_string(),
                    ));
                }
            }
            ResourceKind::ColumnarSegment { id } => {
                self.ensure_target(AdminTarget::Columnar);
                if self.set_field_value("segment", &id, false).is_none() {
                    self.last_result = Some(AdminResult::status(
                        "No matching field for segment.".to_string(),
                    ));
                }
            }
            ResourceKind::ColumnarColumn { segment_id, name } => {
                self.ensure_target(AdminTarget::Columnar);
                let _ = self.set_field_value("segment", &segment_id, false);
                if self.set_field_value("column", &name, false).is_none() {
                    self.last_result = Some(AdminResult::status(
                        "No matching field for column.".to_string(),
                    ));
                }
            }
            ResourceKind::Info => {}
        }
        Ok(())
    }

    fn sync_target_from_resource(&mut self) {
        let Some(entry) = self.resources.selected_entry() else {
            return;
        };
        if let Some(target) = target_for_resource(&entry) {
            self.ensure_target(target);
        }
    }

    fn ensure_target(&mut self, target: AdminTarget) {
        if self.target != target {
            self.target = target;
            self.selected = 0;
            self.last_action = None;
            self.reset_form();
        }
    }

    fn set_field_value(&mut self, key: &str, value: &str, append: bool) -> Option<()> {
        for (idx, field) in self.form_fields.iter_mut().enumerate() {
            if field.key.eq_ignore_ascii_case(key) {
                if append && !field.value.trim().is_empty() {
                    field.value = format!("{},{}", field.value.trim(), value);
                } else {
                    field.value = value.to_string();
                }
                self.active_field = idx;
                return Some(());
            }
        }
        None
    }

    fn open_selection_for_active_field(&mut self) -> Result<()> {
        if self.use_raw_params {
            self.last_result = Some(AdminResult::status(
                "List selection is unavailable while using raw params.".to_string(),
            ));
            return Ok(());
        }
        let Some(field) = self.form_fields.get(self.active_field) else {
            return Ok(());
        };
        let Some(source) = field.list_source else {
            self.last_result = Some(AdminResult::status(
                "No list is available for this field.".to_string(),
            ));
            return Ok(());
        };
        let mut items = load_list_options(&self.backend, &self.form_fields, source)?;
        if items.is_empty() {
            items = self.list_options_from_resources(source);
        }
        items.retain(|item| !item.trim().is_empty());
        if items.is_empty() {
            self.last_result = Some(AdminResult::status(
                "No matching resources were found.".to_string(),
            ));
            return Ok(());
        }
        self.selection = Some(SelectionOverlay::new(
            format!("Select {}", field.label),
            items,
            self.active_field,
        ));
        Ok(())
    }

    fn list_options_from_resources(&self, source: ListSource) -> Vec<String> {
        let mut items = Vec::new();
        match source {
            ListSource::KvKeys => {
                for entry in &self.resources.entries {
                    if let ResourceKind::KvKey { key } = &entry.kind {
                        items.push(key.clone());
                    }
                }
            }
            ListSource::SqlTables => {
                for entry in &self.resources.entries {
                    if let ResourceKind::Table { name } = &entry.kind {
                        items.push(name.clone());
                    }
                }
            }
            ListSource::SqlColumns => {
                let Some(table) = field_value(&self.form_fields, "table") else {
                    return items;
                };
                for entry in &self.resources.entries {
                    if let ResourceKind::Column {
                        table: entry_table,
                        name,
                    } = &entry.kind
                    {
                        if entry_table == &table {
                            items.push(name.clone());
                        }
                    }
                }
            }
            ListSource::ColumnarSegments => {
                for entry in &self.resources.entries {
                    if let ResourceKind::ColumnarSegment { id } = &entry.kind {
                        items.push(id.clone());
                    }
                }
            }
            ListSource::ColumnarColumns => {
                let Some(segment) = field_value(&self.form_fields, "segment") else {
                    return items;
                };
                for entry in &self.resources.entries {
                    if let ResourceKind::ColumnarColumn { segment_id, name } = &entry.kind {
                        if segment_id == &segment {
                            items.push(name.clone());
                        }
                    }
                }
            }
        }
        items.sort();
        items.dedup();
        items
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminInputMode {
    Normal,
    EditingField,
    EditingRaw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdminFocus {
    Table,
    Detail,
    Status,
}

#[derive(Debug, Clone)]
struct AdminFormField {
    key: &'static str,
    label: &'static str,
    value: String,
    placeholder: &'static str,
    required: bool,
    list_source: Option<ListSource>,
}

#[derive(Debug, Clone, Copy)]
enum ListSource {
    KvKeys,
    SqlTables,
    SqlColumns,
    ColumnarSegments,
    ColumnarColumns,
}

#[derive(Debug, Clone)]
struct ResourceEntry {
    label: String,
    kind: ResourceKind,
    depth: usize,
    selectable: bool,
}

#[derive(Debug, Clone)]
enum ResourceKind {
    Section(ResourceSection),
    Table { name: String },
    Column { table: String, name: String },
    KvKey { key: String },
    ColumnarSegment { id: String },
    ColumnarColumn { segment_id: String, name: String },
    Info,
}

struct ResourceTree {
    entries: Vec<ResourceEntry>,
    selected: usize,
    search: Option<String>,
    search_focused: bool,
    last_error: Option<String>,
    last_status: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum ResourceSection {
    SqlTables,
    ColumnarSegments,
    KvKeys,
}

impl ResourceSection {
    fn target(self) -> Option<AdminTarget> {
        match self {
            ResourceSection::SqlTables => Some(AdminTarget::Sql),
            ResourceSection::ColumnarSegments => Some(AdminTarget::Columnar),
            ResourceSection::KvKeys => Some(AdminTarget::Kv),
        }
    }
}

fn target_for_resource(entry: &ResourceEntry) -> Option<AdminTarget> {
    match entry.kind {
        ResourceKind::Section(section) => section.target(),
        ResourceKind::Table { .. } | ResourceKind::Column { .. } => Some(AdminTarget::Sql),
        ResourceKind::KvKey { .. } => Some(AdminTarget::Kv),
        ResourceKind::ColumnarSegment { .. } | ResourceKind::ColumnarColumn { .. } => {
            Some(AdminTarget::Columnar)
        }
        ResourceKind::Info => None,
    }
}

impl ResourceTree {
    fn new(backend: &AdminBackend<'_>) -> Self {
        let (entries, last_error, last_status) = match load_resource_entries(backend) {
            Ok((entries, status)) => (entries, None, status),
            Err(err) => (Vec::new(), Some(err.to_string()), None),
        };
        Self {
            entries,
            selected: 0,
            search: None,
            search_focused: false,
            last_error,
            last_status,
        }
    }

    fn reload(&mut self, backend: &AdminBackend<'_>) {
        match load_resource_entries(backend) {
            Ok((entries, status)) => {
                self.entries = entries;
                self.selected = 0;
                self.last_error = None;
                self.last_status = status;
            }
            Err(err) => {
                self.entries.clear();
                self.selected = 0;
                self.last_error = Some(err.to_string());
                self.last_status = None;
            }
        }
    }

    fn search_term(&self) -> Option<&str> {
        self.search
            .as_deref()
            .filter(|value| !value.trim().is_empty())
    }

    fn filtered_indices(&self) -> Vec<usize> {
        let Some(term) = self.search_term() else {
            return (0..self.entries.len()).collect();
        };
        let term = term.to_lowercase();
        let mut include = vec![false; self.entries.len()];
        for (idx, entry) in self.entries.iter().enumerate() {
            if entry.label.to_lowercase().contains(&term) {
                include[idx] = true;
                let mut depth = entry.depth;
                if depth == 0 {
                    continue;
                }
                for parent_idx in (0..idx).rev() {
                    let parent = &self.entries[parent_idx];
                    if parent.depth < depth {
                        include[parent_idx] = true;
                        depth = parent.depth;
                        if depth == 0 {
                            break;
                        }
                    }
                }
            }
        }
        include
            .iter()
            .enumerate()
            .filter_map(|(idx, keep)| if *keep { Some(idx) } else { None })
            .collect()
    }

    fn filtered_entries(&self) -> Vec<ResourceEntry> {
        let indices = self.filtered_indices();
        indices
            .iter()
            .filter_map(|idx| self.entries.get(*idx))
            .cloned()
            .collect()
    }

    fn selected_entry(&self) -> Option<ResourceEntry> {
        let indices = self.filtered_indices();
        let idx = indices.get(self.selected).copied()?;
        self.entries.get(idx).cloned()
    }

    fn ensure_selection_in_range(&mut self) {
        let len = self.filtered_indices().len();
        if len == 0 {
            self.selected = 0;
        } else if self.selected >= len {
            self.selected = len - 1;
        }
    }

    fn move_up(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
        }
    }

    fn move_down(&mut self) {
        let len = self.filtered_indices().len();
        if self.selected + 1 < len {
            self.selected += 1;
        }
    }

    fn move_top(&mut self) {
        self.selected = 0;
    }

    fn move_bottom(&mut self) {
        let len = self.filtered_indices().len();
        if len > 0 {
            self.selected = len - 1;
        }
    }

    fn page_down(&mut self) {
        let len = self.filtered_indices().len();
        if len == 0 {
            return;
        }
        self.selected = (self.selected + 5).min(len - 1);
    }

    fn page_up(&mut self) {
        self.selected = self.selected.saturating_sub(5);
    }

    fn push_search(&mut self, ch: char) {
        let search = self.search.get_or_insert_with(String::new);
        search.push(ch);
        self.ensure_selection_in_range();
    }

    fn pop_search(&mut self) {
        if let Some(search) = self.search.as_mut() {
            if !search.is_empty() {
                search.pop();
            } else {
                self.reset_search();
            }
        }
        self.ensure_selection_in_range();
    }

    fn reset_search(&mut self) {
        self.search = None;
        self.search_focused = false;
        self.selected = 0;
    }
}

fn build_form_fields(target: AdminTarget, action: AdminAction) -> Vec<AdminFormField> {
    match (target, action) {
        (_, AdminAction::Backup) => vec![form_field(
            "handle",
            "Handle (status)",
            "",
            "backup-handle",
            false,
        )],
        (_, AdminAction::Restore) => vec![
            form_field("source", "Source", "", "s3://bucket/path", false),
            form_field("handle", "Handle (status)", "", "restore-handle", false),
        ],
        (AdminTarget::Sql, AdminAction::Read) => vec![
            form_field("query", "Query", "", "SELECT * FROM table", false),
            form_field_with_list(
                "table",
                "Table",
                "",
                "mytable",
                false,
                Some(ListSource::SqlTables),
            ),
            form_field_with_list(
                "columns",
                "Columns",
                "",
                "col1,col2",
                false,
                Some(ListSource::SqlColumns),
            ),
        ],
        (AdminTarget::Sql, _) => vec![form_field(
            "query",
            "Query",
            "",
            "SELECT * FROM table",
            true,
        )],
        (AdminTarget::Kv, AdminAction::Read) => vec![
            form_field_with_list("key", "Key", "", "mykey", false, Some(ListSource::KvKeys)),
            form_field("prefix", "Prefix", "", "app/", false),
        ],
        (AdminTarget::Kv, AdminAction::Create | AdminAction::Update) => vec![
            form_field_with_list("key", "Key", "", "mykey", true, Some(ListSource::KvKeys)),
            form_field("value", "Value", "", "hello", true),
        ],
        (AdminTarget::Kv, AdminAction::Delete) => vec![form_field_with_list(
            "key",
            "Key",
            "",
            "mykey",
            true,
            Some(ListSource::KvKeys),
        )],
        (AdminTarget::Vector, AdminAction::Read) => vec![
            form_field("index", "Index", "", "myindex", true),
            form_field("query", "Query", "", "[0.1, 0.2]", true),
            form_field("k", "Top K", "10", "10", false),
        ],
        (AdminTarget::Vector, AdminAction::Create | AdminAction::Update) => vec![
            form_field("index", "Index", "", "myindex", true),
            form_field("key", "Key", "", "item1", true),
            form_field("vector", "Vector", "", "[0.1, 0.2]", true),
        ],
        (AdminTarget::Vector, AdminAction::Delete) => vec![
            form_field("index", "Index", "", "myindex", true),
            form_field("key", "Key", "", "item1", true),
        ],
        (AdminTarget::Hnsw, AdminAction::Read) => {
            vec![form_field("name", "Index", "", "myindex", true)]
        }
        (AdminTarget::Hnsw, AdminAction::Create) => vec![
            form_field("name", "Index", "", "myindex", true),
            form_field("dim", "Dimensions", "", "128", true),
            form_field("metric", "Metric", "cosine", "cosine", false),
        ],
        (AdminTarget::Hnsw, AdminAction::Delete) => {
            vec![form_field("name", "Index", "", "myindex", true)]
        }
        (AdminTarget::Columnar, AdminAction::Read) => vec![
            form_field("mode", "Mode", "list", "list|scan|stats|index_list", true),
            form_field_with_list(
                "segment",
                "Segment",
                "",
                "segment_id",
                false,
                Some(ListSource::ColumnarSegments),
            ),
        ],
        (AdminTarget::Columnar, AdminAction::Create) => vec![
            form_field("file", "File", "", "data.csv", false),
            form_field_with_list(
                "table",
                "Table",
                "",
                "mytable",
                false,
                Some(ListSource::SqlTables),
            ),
            form_field_with_list(
                "segment",
                "Segment",
                "",
                "segment_id",
                false,
                Some(ListSource::ColumnarSegments),
            ),
            form_field_with_list(
                "column",
                "Column",
                "",
                "column_name",
                false,
                Some(ListSource::ColumnarColumns),
            ),
            form_field("index_type", "Index Type", "", "minmax", false),
        ],
        (AdminTarget::Columnar, AdminAction::Delete) => vec![
            form_field_with_list(
                "segment",
                "Segment",
                "",
                "segment_id",
                true,
                Some(ListSource::ColumnarSegments),
            ),
            form_field_with_list(
                "column",
                "Column",
                "",
                "column_name",
                true,
                Some(ListSource::ColumnarColumns),
            ),
        ],
        _ => Vec::new(),
    }
}

fn form_field(
    key: &'static str,
    label: &'static str,
    value: &str,
    placeholder: &'static str,
    required: bool,
) -> AdminFormField {
    form_field_with_list(key, label, value, placeholder, required, None)
}

fn form_field_with_list(
    key: &'static str,
    label: &'static str,
    value: &str,
    placeholder: &'static str,
    required: bool,
    list_source: Option<ListSource>,
) -> AdminFormField {
    AdminFormField {
        key,
        label,
        value: value.to_string(),
        placeholder,
        required,
        list_source,
    }
}

fn load_list_options(
    backend: &AdminBackend<'_>,
    fields: &[AdminFormField],
    source: ListSource,
) -> Result<Vec<String>> {
    match source {
        ListSource::KvKeys => {
            let prefix = field_value(fields, "prefix");
            let command = AdminCommand::Kv(KvCommand::List { prefix });
            let capture = capture_admin_command(backend, command, Some(50))?;
            Ok(extract_column_values(
                &capture.columns,
                &capture.rows,
                "key",
            ))
        }
        ListSource::ColumnarSegments => {
            let command = AdminCommand::Columnar(ColumnarCommand::List);
            let capture = capture_admin_command(backend, command, Some(50))?;
            Ok(extract_column_values(
                &capture.columns,
                &capture.rows,
                "segment_id",
            ))
        }
        ListSource::SqlTables => {
            let Some(db) = backend.local_db() else {
                return Err(CliError::InvalidArgument(
                    "Table listing is only available for local admin sessions.".to_string(),
                ));
            };
            let mut tables = db
                .list_tables_simple()?
                .into_iter()
                .map(|table| table.name)
                .collect::<Vec<_>>();
            tables.sort();
            tables.dedup();
            Ok(tables)
        }
        ListSource::SqlColumns => {
            let table = field_value(fields, "table")
                .ok_or_else(|| CliError::InvalidArgument("Select a table first.".to_string()))?;
            let Some(db) = backend.local_db() else {
                return Err(CliError::InvalidArgument(
                    "Column listing is only available for local admin sessions.".to_string(),
                ));
            };
            let mut columns = db
                .get_table_info_simple(&table)?
                .columns
                .into_iter()
                .map(|column| column.name)
                .collect::<Vec<_>>();
            columns.sort();
            columns.dedup();
            Ok(columns)
        }
        ListSource::ColumnarColumns => {
            let segment = field_value(fields, "segment")
                .ok_or_else(|| CliError::InvalidArgument("Select a segment first.".to_string()))?;
            let Some(db) = backend.local_db() else {
                return Err(CliError::InvalidArgument(
                    "Column listing is only available for local admin sessions.".to_string(),
                ));
            };
            let mut columns = list_columnar_columns_from_segment(db, &segment)?;
            columns.sort();
            columns.dedup();
            Ok(columns)
        }
    }
}

fn field_value(fields: &[AdminFormField], key: &str) -> Option<String> {
    fields
        .iter()
        .find(|field| field.key.eq_ignore_ascii_case(key))
        .map(|field| field.value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn capture_admin_command(
    backend: &AdminBackend<'_>,
    command: AdminCommand,
    limit: Option<usize>,
) -> Result<CaptureState> {
    let request = AdminRequest {
        action: AdminAction::Read,
        command,
        limit,
        quiet: true,
        ui_mode: UiMode::Batch,
        connection_label: String::new(),
        output: backend.output_format(),
        data_dir: backend.data_dir().map(PathBuf::from),
    };
    let state = CaptureFormatter::shared_state();
    let mut make_formatter = CaptureFormatter::factory(&state);
    let mut sink = io::sink();
    let result = match backend {
        AdminBackend::Local { db, batch_mode, .. } => {
            execute_local_action(db, batch_mode, request, &mut sink, &mut make_formatter)
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
                &mut make_formatter,
            ))
        }
    };
    result?;
    let capture = state.lock().expect("admin capture lock").clone();
    Ok(capture)
}

fn extract_column_values(columns: &[Column], rows: &[Row], column_name: &str) -> Vec<String> {
    let index = columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(column_name))
        .unwrap_or(0);
    rows.iter()
        .filter_map(|row| row.columns.get(index))
        .map(value_to_string)
        .collect()
}

fn list_columnar_columns_from_segment(
    db: &alopex_embedded::Database,
    segment_id: &str,
) -> Result<Vec<String>> {
    let (table_id, segment_id) = parse_segment_id(segment_id).ok_or_else(|| {
        CliError::InvalidArgument(
            "Invalid segment id. Expected format: table_id:segment_id.".to_string(),
        )
    })?;
    let tables = db.list_tables_simple()?;
    let table = tables
        .into_iter()
        .find(|table| table.table_id == table_id)
        .ok_or_else(|| {
            CliError::InvalidArgument("Unable to resolve table for segment.".to_string())
        })?;
    let batches = db.read_columnar_segment(&table.name, segment_id, None)?;
    let batch = batches
        .first()
        .ok_or_else(|| CliError::InvalidArgument("Columnar segment is empty.".to_string()))?;
    Ok(batch
        .schema
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect())
}

fn parse_segment_id(segment_id: &str) -> Option<(u32, u64)> {
    let (table_id, segment_id) = segment_id.split_once(':')?;
    let table_id = table_id.parse::<u32>().ok()?;
    let segment_id = segment_id.parse::<u64>().ok()?;
    Some((table_id, segment_id))
}

const RESOURCE_LIMIT: usize = 50;
const COLUMNAR_COLUMN_LIMIT: usize = 20;

fn block_on_with_runtime<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = T>,
{
    match Handle::try_current() {
        Ok(handle) => Ok(tokio::task::block_in_place(|| handle.block_on(future))),
        Err(_) => {
            let runtime = Runtime::new().map_err(|err| {
                CliError::InvalidArgument(format!("Failed to start async runtime: {err}"))
            })?;
            Ok(runtime.block_on(future))
        }
    }
}

fn load_resource_entries(
    backend: &AdminBackend<'_>,
) -> Result<(Vec<ResourceEntry>, Option<String>)> {
    match backend {
        AdminBackend::Remote { client, .. } => load_remote_resources(client),
        _ => {
            let mut entries = Vec::new();
            entries.extend(load_sql_resources(backend)?);
            entries.extend(load_columnar_resources(backend)?);
            entries.extend(load_kv_resources(backend)?);
            Ok((entries, None))
        }
    }
}

fn load_remote_resources(client: &HttpClient) -> Result<(Vec<ResourceEntry>, Option<String>)> {
    let request = AdminResourcesRequest {
        limit: Some(RESOURCE_LIMIT),
        include_columnar_columns: Some(true),
        columnar_column_limit: Some(COLUMNAR_COLUMN_LIMIT),
        kv_prefix: None,
    };
    let response = block_on_with_runtime(fetch_admin_resources(client, &request))?;
    match response {
        Ok(response) => {
            let status = truncated_status(&response.truncated);
            Ok((build_remote_entries(response), status))
        }
        Err(ClientError::HttpStatus { status, .. })
            if status == reqwest::StatusCode::FORBIDDEN
                || status == reqwest::StatusCode::UNAUTHORIZED =>
        {
            Ok((remote_listing_denied_entries(), None))
        }
        Err(err) => Err(map_client_error(err)),
    }
}

fn truncated_status(
    truncated: &crate::client::admin_resources::TruncatedSections,
) -> Option<String> {
    let mut sections = Vec::new();
    if truncated.sql_tables {
        sections.push("SQL tables");
    }
    if truncated.columnar_segments {
        sections.push("columnar segments");
    }
    if truncated.kv_keys {
        sections.push("KV keys");
    }
    if sections.is_empty() {
        None
    } else {
        Some(format!(
            "Resources truncated (limit {RESOURCE_LIMIT}): {}.",
            sections.join(", ")
        ))
    }
}

fn build_remote_entries(
    response: crate::client::admin_resources::AdminResourcesResponse,
) -> Vec<ResourceEntry> {
    let mut entries = Vec::new();

    entries.push(ResourceEntry {
        label: "SQL Tables".to_string(),
        kind: ResourceKind::Section(ResourceSection::SqlTables),
        depth: 0,
        selectable: false,
    });
    for table in response.sql_tables {
        let table_name = table.name.clone();
        entries.push(ResourceEntry {
            label: table_name.clone(),
            kind: ResourceKind::Table {
                name: table_name.clone(),
            },
            depth: 1,
            selectable: true,
        });
        for column in table.columns {
            entries.push(ResourceEntry {
                label: column.name.clone(),
                kind: ResourceKind::Column {
                    table: table_name.clone(),
                    name: column.name,
                },
                depth: 2,
                selectable: true,
            });
        }
    }
    if response.truncated.sql_tables {
        let label = format!("Truncated: showing first {RESOURCE_LIMIT} tables.");
        entries.push(truncated_entry(&label, 1));
    }

    entries.push(ResourceEntry {
        label: "Columnar Segments".to_string(),
        kind: ResourceKind::Section(ResourceSection::ColumnarSegments),
        depth: 0,
        selectable: false,
    });
    for segment in response.columnar_segments {
        let segment_id = segment.id.clone();
        entries.push(ResourceEntry {
            label: segment_id.clone(),
            kind: ResourceKind::ColumnarSegment { id: segment_id },
            depth: 1,
            selectable: true,
        });
        if let Some(columns) = segment.columns {
            for column in columns {
                entries.push(ResourceEntry {
                    label: column.clone(),
                    kind: ResourceKind::ColumnarColumn {
                        segment_id: segment.id.clone(),
                        name: column,
                    },
                    depth: 2,
                    selectable: true,
                });
            }
        }
    }
    if response.truncated.columnar_segments {
        let label = format!("Truncated: showing first {RESOURCE_LIMIT} segments.");
        entries.push(truncated_entry(&label, 1));
    }

    entries.push(ResourceEntry {
        label: "KV Keys".to_string(),
        kind: ResourceKind::Section(ResourceSection::KvKeys),
        depth: 0,
        selectable: false,
    });
    for key in response.kv_keys {
        entries.push(ResourceEntry {
            label: key.clone(),
            kind: ResourceKind::KvKey { key },
            depth: 1,
            selectable: true,
        });
    }
    if response.truncated.kv_keys {
        let label = format!("Truncated: showing first {RESOURCE_LIMIT} keys.");
        entries.push(truncated_entry(&label, 1));
    }

    entries
}

fn remote_listing_denied_entries() -> Vec<ResourceEntry> {
    let mut entries = Vec::new();
    for section in [
        ResourceSection::SqlTables,
        ResourceSection::ColumnarSegments,
        ResourceSection::KvKeys,
    ] {
        let label = match section {
            ResourceSection::SqlTables => "SQL Tables",
            ResourceSection::ColumnarSegments => "Columnar Segments",
            ResourceSection::KvKeys => "KV Keys",
        };
        entries.push(ResourceEntry {
            label: label.to_string(),
            kind: ResourceKind::Section(section),
            depth: 0,
            selectable: false,
        });
        entries.push(truncated_entry("Remote listing denied.", 1));
    }
    entries
}

fn truncated_entry(label: &str, depth: usize) -> ResourceEntry {
    ResourceEntry {
        label: label.to_string(),
        kind: ResourceKind::Info,
        depth,
        selectable: false,
    }
}

fn map_client_error(err: ClientError) -> CliError {
    match err {
        ClientError::Request { source, .. } => {
            CliError::ServerConnection(format!("request failed: {source}"))
        }
        ClientError::InvalidUrl(message) => CliError::InvalidArgument(message),
        ClientError::Build(message) => CliError::InvalidArgument(message),
        ClientError::Auth(err) => CliError::InvalidArgument(err.to_string()),
        ClientError::HttpStatus { status, body } => {
            CliError::ServerConnection(format!("server error {status}: {body}"))
        }
    }
}

fn load_sql_resources(backend: &AdminBackend<'_>) -> Result<Vec<ResourceEntry>> {
    let mut entries = Vec::new();
    entries.push(ResourceEntry {
        label: "SQL Tables".to_string(),
        kind: ResourceKind::Section(ResourceSection::SqlTables),
        depth: 0,
        selectable: false,
    });
    let Some(db) = backend.local_db() else {
        entries.push(ResourceEntry {
            label: "Remote listing unavailable".to_string(),
            kind: ResourceKind::Info,
            depth: 1,
            selectable: false,
        });
        return Ok(entries);
    };
    let mut tables = match db.list_tables_simple() {
        Ok(tables) => tables,
        Err(alopex_embedded::Error::CatalogNotFound(_))
        | Err(alopex_embedded::Error::NamespaceNotFound(_, _)) => {
            let _ = db.create_catalog(CreateCatalogRequest::new("default"));
            let _ = db.create_namespace(CreateNamespaceRequest::new("default", "default"));
            match db.list_tables_simple() {
                Ok(tables) => tables,
                Err(err) => {
                    entries.push(ResourceEntry {
                        label: format!("SQL catalog unavailable: {err}"),
                        kind: ResourceKind::Info,
                        depth: 1,
                        selectable: false,
                    });
                    return Ok(entries);
                }
            }
        }
        Err(err) => return Err(err.into()),
    };
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    for table in tables.into_iter().take(RESOURCE_LIMIT) {
        entries.push(ResourceEntry {
            label: table.name.clone(),
            kind: ResourceKind::Table {
                name: table.name.clone(),
            },
            depth: 1,
            selectable: true,
        });
        for column in table.columns {
            entries.push(ResourceEntry {
                label: column.name.clone(),
                kind: ResourceKind::Column {
                    table: table.name.clone(),
                    name: column.name,
                },
                depth: 2,
                selectable: true,
            });
        }
    }
    Ok(entries)
}

fn load_columnar_resources(backend: &AdminBackend<'_>) -> Result<Vec<ResourceEntry>> {
    let mut entries = Vec::new();
    entries.push(ResourceEntry {
        label: "Columnar Segments".to_string(),
        kind: ResourceKind::Section(ResourceSection::ColumnarSegments),
        depth: 0,
        selectable: false,
    });
    let Some(db) = backend.local_db() else {
        entries.push(ResourceEntry {
            label: "Remote listing unavailable".to_string(),
            kind: ResourceKind::Info,
            depth: 1,
            selectable: false,
        });
        return Ok(entries);
    };
    let mut segments = db.list_columnar_segments()?;
    segments.sort();
    let mut expanded = 0;
    for segment in segments.into_iter().take(RESOURCE_LIMIT) {
        entries.push(ResourceEntry {
            label: segment.clone(),
            kind: ResourceKind::ColumnarSegment {
                id: segment.clone(),
            },
            depth: 1,
            selectable: true,
        });
        if expanded < COLUMNAR_COLUMN_LIMIT {
            if let Ok(columns) = list_columnar_columns_from_segment(db, &segment) {
                for column in columns {
                    entries.push(ResourceEntry {
                        label: column.clone(),
                        kind: ResourceKind::ColumnarColumn {
                            segment_id: segment.clone(),
                            name: column,
                        },
                        depth: 2,
                        selectable: true,
                    });
                }
            }
            expanded += 1;
        }
    }
    Ok(entries)
}

fn load_kv_resources(backend: &AdminBackend<'_>) -> Result<Vec<ResourceEntry>> {
    let mut entries = Vec::new();
    entries.push(ResourceEntry {
        label: "KV Keys".to_string(),
        kind: ResourceKind::Section(ResourceSection::KvKeys),
        depth: 0,
        selectable: false,
    });
    let system_prefixes = [
        "__catalog__/",
        "hnsw:",
        "__alopex_",
        "__alopex:",
        "vector:",
        "columnar:",
    ];
    let command = AdminCommand::Kv(KvCommand::List { prefix: None });
    let capture = capture_admin_command(backend, command, Some(RESOURCE_LIMIT))?;
    let keys = extract_column_values(&capture.columns, &capture.rows, "key");
    for key in keys.into_iter().filter(|key| {
        !system_prefixes.iter().any(|prefix| key.starts_with(prefix))
            && !key.trim().is_empty()
            && !key.chars().any(|ch| ch.is_control())
    }) {
        entries.push(ResourceEntry {
            label: key.clone(),
            kind: ResourceKind::KvKey { key },
            depth: 1,
            selectable: true,
        });
    }
    Ok(entries)
}

fn build_params_from_fields(
    fields: &[AdminFormField],
) -> std::collections::HashMap<String, String> {
    let mut params = std::collections::HashMap::new();
    for field in fields {
        if !field.value.trim().is_empty() {
            params.insert(field.key.to_lowercase(), field.value.trim().to_string());
        }
    }
    params
}

fn validate_params(
    action: AdminAction,
    target: AdminTarget,
    params: &std::collections::HashMap<String, String>,
) -> std::result::Result<(), String> {
    if matches!(
        action,
        AdminAction::Archive | AdminAction::Restore | AdminAction::Backup | AdminAction::Export
    ) {
        return Ok(());
    }
    match (target, action) {
        (AdminTarget::Sql, AdminAction::Read) => {
            if params.contains_key("query") {
                return Ok(());
            }
            if let Some(columns) = params.get("columns") {
                if !params.contains_key("table") {
                    return Err("Provide table to use columns.".to_string());
                }
                if columns.trim().is_empty() {
                    return Err("Columns cannot be empty when provided.".to_string());
                }
            }
            if params.contains_key("table") {
                Ok(())
            } else {
                Err("Provide query or table.".to_string())
            }
        }
        (AdminTarget::Kv, AdminAction::Read) => {
            if params.contains_key("key") || params.contains_key("prefix") {
                Ok(())
            } else {
                Err("Provide either key or prefix.".to_string())
            }
        }
        (AdminTarget::Columnar, AdminAction::Read) => {
            let mode = params.get("mode").map(|v| v.as_str()).unwrap_or("list");
            if matches!(mode, "scan" | "stats" | "index_list") && !params.contains_key("segment") {
                Err("Provide segment for scan/stats/index_list.".to_string())
            } else {
                Ok(())
            }
        }
        (AdminTarget::Columnar, AdminAction::Create) => {
            let has_ingest = params.contains_key("file") && params.contains_key("table");
            let has_index = params.contains_key("segment")
                && params.contains_key("column")
                && params.contains_key("index_type");
            if has_ingest || has_index {
                Ok(())
            } else {
                Err("Provide file+table or segment+column+index_type.".to_string())
            }
        }
        _ => {
            let missing = required_keys_for(target, action)
                .into_iter()
                .filter(|key| !params.contains_key(*key))
                .collect::<Vec<_>>();
            if missing.is_empty() {
                Ok(())
            } else {
                Err(format!("Missing: {}", missing.join(", ")))
            }
        }
    }
}

fn required_keys_for(target: AdminTarget, action: AdminAction) -> Vec<&'static str> {
    match (target, action) {
        (AdminTarget::Sql, AdminAction::Read) => Vec::new(),
        (AdminTarget::Sql, _) => vec!["query"],
        (AdminTarget::Kv, AdminAction::Create | AdminAction::Update) => vec!["key", "value"],
        (AdminTarget::Kv, AdminAction::Delete) => vec!["key"],
        (AdminTarget::Vector, AdminAction::Read) => vec!["index", "query"],
        (AdminTarget::Vector, AdminAction::Create | AdminAction::Update) => {
            vec!["index", "key", "vector"]
        }
        (AdminTarget::Vector, AdminAction::Delete) => vec!["index", "key"],
        (AdminTarget::Hnsw, AdminAction::Read) => vec!["name"],
        (AdminTarget::Hnsw, AdminAction::Create) => vec!["name", "dim"],
        (AdminTarget::Hnsw, AdminAction::Delete) => vec!["name"],
        (AdminTarget::Columnar, AdminAction::Delete) => vec!["segment", "column"],
        _ => Vec::new(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminTarget {
    Sql,
    Kv,
    Vector,
    Hnsw,
    Columnar,
}

impl AdminTarget {
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
    fn local_db(&self) -> Option<&alopex_embedded::Database> {
        match self {
            AdminBackend::Local { db, .. } => Some(*db),
            AdminBackend::Remote { .. } => None,
        }
    }

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
    pub initial_target: Option<AdminTarget>,
}

pub fn run_admin_ui(context: AdminContext<'_>) -> Result<()> {
    if !is_tty() {
        let mut writer = io::stdout().lock();
        return write_non_tty_fallback(&mut writer, context.backend.output_format());
    }

    let app = AdminApp::new(
        context.connection_label,
        context.auth,
        context.backend,
        context.initial_target,
    );
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
        let handle = params
            .get("handle")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());
        let source = params
            .get("source")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());

        let command = match action {
            AdminAction::Archive => LifecycleCommand::Archive,
            AdminAction::Restore => {
                if let Some(handle) = handle {
                    LifecycleCommand::Restore {
                        source: None,
                        command: Some(LifecycleRestoreCommand::Status { handle }),
                    }
                } else {
                    LifecycleCommand::Restore {
                        source,
                        command: None,
                    }
                }
            }
            AdminAction::Backup => {
                if let Some(handle) = handle {
                    LifecycleCommand::Backup {
                        command: Some(LifecycleBackupCommand::Status { handle }),
                    }
                } else {
                    LifecycleCommand::Backup { command: None }
                }
            }
            AdminAction::Export => LifecycleCommand::Export,
            _ => return Ok(None),
        };
        return Ok(Some(AdminCommand::Lifecycle(command)));
    }
    match target {
        AdminTarget::Sql => build_sql_command(action, params),
        AdminTarget::Kv => build_kv_command(action, params),
        AdminTarget::Vector => build_vector_command(action, params),
        AdminTarget::Hnsw => build_hnsw_command(action, params),
        AdminTarget::Columnar => build_columnar_command(action, params),
    }
}

fn build_sql_command(
    action: AdminAction,
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<AdminCommand>> {
    let query = if let Some(query) = params.get("query").cloned() {
        Some(query)
    } else if action == AdminAction::Read {
        let table = params.get("table").cloned();
        let columns = params.get("columns").cloned();
        match table {
            Some(table) => {
                let columns = columns
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| "*".to_string());
                Some(format!("SELECT {} FROM {}", columns, table))
            }
            None => None,
        }
    } else {
        None
    };
    if query.is_none() {
        return Ok(None);
    }
    Ok(Some(AdminCommand::Sql(SqlCommand {
        query,
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        params: Vec::new(),
        read_mode: None,
        routing_report: None,
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
        "h/l or Left/Right: move focus",
        "Menu: j/k move, / search, e edit, r raw, Enter select, R refresh",
        "Input: Up/Down action, Tab field, e edit, o list, r raw, Enter execute",
        "Data: j/k scroll",
        "a: back",
        "?: toggle help",
        "q/Esc: quit",
    ]
    .join("\n");

    let help = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("Help"))
        .wrap(Wrap { trim: true });
    frame.render_widget(help, rect);
}

#[derive(Debug, Clone)]
struct SelectionOverlay {
    title: String,
    items: Vec<String>,
    selected: usize,
    field_index: usize,
    search: Option<String>,
    search_focused: bool,
}

impl SelectionOverlay {
    fn new(title: String, items: Vec<String>, field_index: usize) -> Self {
        Self {
            title,
            items,
            selected: 0,
            field_index,
            search: None,
            search_focused: false,
        }
    }

    fn search_term(&self) -> Option<&str> {
        self.search
            .as_deref()
            .filter(|value| !value.trim().is_empty())
    }

    fn filtered_indices(&self) -> Vec<usize> {
        let Some(term) = self.search_term() else {
            return (0..self.items.len()).collect();
        };
        let term = term.to_lowercase();
        self.items
            .iter()
            .enumerate()
            .filter_map(|(idx, item)| {
                if item.to_lowercase().contains(&term) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect()
    }

    fn selected_value(&self) -> Option<String> {
        let indices = self.filtered_indices();
        let idx = indices.get(self.selected).copied()?;
        self.items.get(idx).cloned()
    }

    fn ensure_selection_in_range(&mut self) {
        let len = self.filtered_indices().len();
        if len == 0 {
            self.selected = 0;
        } else if self.selected >= len {
            self.selected = len - 1;
        }
    }

    fn move_up(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
        }
    }

    fn move_down(&mut self) {
        let len = self.filtered_indices().len();
        if self.selected + 1 < len {
            self.selected += 1;
        }
    }

    fn move_top(&mut self) {
        self.selected = 0;
    }

    fn move_bottom(&mut self) {
        let len = self.filtered_indices().len();
        if len > 0 {
            self.selected = len - 1;
        }
    }

    fn push_search(&mut self, ch: char) {
        let search = self.search.get_or_insert_with(String::new);
        search.push(ch);
        self.ensure_selection_in_range();
    }

    fn pop_search(&mut self) {
        if let Some(search) = self.search.as_mut() {
            if !search.is_empty() {
                search.pop();
            } else {
                self.reset_search();
            }
        }
        self.ensure_selection_in_range();
    }

    fn reset_search(&mut self) {
        self.search = None;
        self.search_focused = false;
        self.selected = 0;
    }
}

fn render_selection_overlay(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    selection: &SelectionOverlay,
) {
    let overlay_width = area.width.saturating_sub(6).min(60);
    let overlay_height = area.height.saturating_sub(6).min(16);
    let rect = Rect::new(
        area.x + (area.width.saturating_sub(overlay_width)) / 2,
        area.y + (area.height.saturating_sub(overlay_height)) / 2,
        overlay_width,
        overlay_height,
    );

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(3),
        ])
        .split(rect);

    let search = selection
        .search
        .as_ref()
        .map(|value| format!("/ {value}"))
        .unwrap_or_else(|| "/".to_string());
    let search_style = if selection.search_focused {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Gray)
    };
    frame.render_widget(
        Paragraph::new(search)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(selection.title.as_str()),
            )
            .style(search_style),
        layout[0],
    );

    frame.render_widget(
        Paragraph::new("Enter: choose  Esc: close  /: search  g/G: top/bottom  j/k: move")
            .style(Style::default().fg(Color::DarkGray)),
        layout[1],
    );

    let indices = selection.filtered_indices();
    let items = if indices.is_empty() {
        vec![ListItem::new(Line::from("No options available."))]
    } else {
        indices
            .iter()
            .filter_map(|idx| selection.items.get(*idx))
            .map(|item| ListItem::new(Line::from(item.clone())))
            .collect::<Vec<_>>()
    };
    let mut state = ListState::default();
    state.select(Some(selection.selected));
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL))
        .highlight_style(
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");
    frame.render_stateful_widget(list, layout[2], &mut state);
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

#[derive(Default, Clone)]
struct CaptureState {
    columns: Vec<Column>,
    rows: Vec<Row>,
}

struct CaptureFormatter {
    state: Arc<Mutex<CaptureState>>,
}

impl CaptureFormatter {
    /// Create a fresh shared capture state.
    fn shared_state() -> Arc<Mutex<CaptureState>> {
        Arc::new(Mutex::new(CaptureState::default()))
    }

    /// Formatter factory producing capture formatters bound to `state`.
    ///
    /// The `sql` command creates one formatter per statement result block, so
    /// admin capture hands out multiple formatters sharing one state.
    fn factory(state: &Arc<Mutex<CaptureState>>) -> impl FnMut() -> Box<dyn Formatter> {
        let state = Arc::clone(state);
        move || {
            Box::new(CaptureFormatter {
                state: Arc::clone(&state),
            }) as Box<dyn Formatter>
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{BatchMode, BatchModeSource};
    use alopex_embedded::Database;

    fn make_app<'a>(db: &'a Database) -> AdminApp<'a> {
        let batch_mode = Box::leak(Box::new(BatchMode {
            is_batch: true,
            is_tty: true,
            source: BatchModeSource::Explicit,
        }));
        let backend = AdminBackend::Local {
            db,
            batch_mode,
            output_format: OutputFormat::Table,
            limit: None,
            quiet: true,
            data_dir: None,
        };
        AdminApp::new(
            "local",
            AuthCapabilities::full(),
            backend,
            Some(AdminTarget::Kv),
        )
    }

    fn field_value(app: &AdminApp<'_>, key: &str) -> Option<String> {
        app.form_fields
            .iter()
            .find(|field| field.key.eq_ignore_ascii_case(key))
            .map(|field| field.value.clone())
    }

    #[test]
    fn resource_tree_filters_keep_parents() {
        let entries = vec![
            ResourceEntry {
                label: "SQL Tables".to_string(),
                kind: ResourceKind::Section(ResourceSection::SqlTables),
                depth: 0,
                selectable: false,
            },
            ResourceEntry {
                label: "users".to_string(),
                kind: ResourceKind::Table {
                    name: "users".to_string(),
                },
                depth: 1,
                selectable: true,
            },
            ResourceEntry {
                label: "email".to_string(),
                kind: ResourceKind::Column {
                    table: "users".to_string(),
                    name: "email".to_string(),
                },
                depth: 2,
                selectable: true,
            },
        ];
        let tree = ResourceTree {
            entries,
            selected: 0,
            search: Some("email".to_string()),
            search_focused: false,
            last_error: None,
            last_status: None,
        };
        let indices = tree.filtered_indices();
        assert_eq!(indices, vec![0, 1, 2]);
    }

    #[test]
    fn resource_tree_paging_clamps() {
        let entries = (0..12)
            .map(|idx| ResourceEntry {
                label: format!("item-{idx}"),
                kind: ResourceKind::Info,
                depth: 0,
                selectable: true,
            })
            .collect::<Vec<_>>();
        let mut tree = ResourceTree {
            entries,
            selected: 0,
            search: None,
            search_focused: false,
            last_error: None,
            last_status: None,
        };
        tree.page_down();
        assert_eq!(tree.selected, 5);
        tree.page_down();
        assert_eq!(tree.selected, 10);
        tree.page_down();
        assert_eq!(tree.selected, 11);
        tree.page_up();
        assert_eq!(tree.selected, 6);
    }

    #[test]
    fn selection_overlay_filters_values() {
        let mut overlay = SelectionOverlay::new(
            "Select".to_string(),
            vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
            0,
        );
        overlay.search = Some("et".to_string());
        overlay.ensure_selection_in_range();
        assert_eq!(overlay.selected_value(), Some("beta".to_string()));
        overlay.move_down();
        assert_eq!(overlay.selected_value(), Some("beta".to_string()));
    }

    #[test]
    fn focus_transitions_follow_table_detail_status() {
        let db = Database::open_in_memory().expect("db");
        let mut app = make_app(&db);
        app.focus = AdminFocus::Table;
        assert_eq!(app.focus_right(), AdminFocus::Detail);
        app.focus = AdminFocus::Detail;
        assert_eq!(app.focus_left(), AdminFocus::Table);
        assert_eq!(app.focus_right(), AdminFocus::Status);
        app.focus = AdminFocus::Status;
        assert_eq!(app.focus_left(), AdminFocus::Detail);
        assert_eq!(app.focus_right(), AdminFocus::Status);
    }

    #[test]
    fn resource_selection_sets_sql_fields() {
        let db = Database::open_in_memory().expect("db");
        let mut app = make_app(&db);
        app.resources = ResourceTree {
            entries: vec![
                ResourceEntry {
                    label: "SQL Tables".to_string(),
                    kind: ResourceKind::Section(ResourceSection::SqlTables),
                    depth: 0,
                    selectable: false,
                },
                ResourceEntry {
                    label: "users".to_string(),
                    kind: ResourceKind::Table {
                        name: "users".to_string(),
                    },
                    depth: 1,
                    selectable: true,
                },
            ],
            selected: 1,
            search: None,
            search_focused: false,
            last_error: None,
            last_status: None,
        };
        app.apply_resource_selection().expect("select table");
        assert_eq!(app.target, AdminTarget::Sql);
        assert_eq!(field_value(&app, "table"), Some("users".to_string()));
    }

    #[test]
    fn resource_selection_sets_kv_key() {
        let db = Database::open_in_memory().expect("db");
        let mut app = make_app(&db);
        app.resources = ResourceTree {
            entries: vec![ResourceEntry {
                label: "mykey".to_string(),
                kind: ResourceKind::KvKey {
                    key: "mykey".to_string(),
                },
                depth: 1,
                selectable: true,
            }],
            selected: 0,
            search: None,
            search_focused: false,
            last_error: None,
            last_status: None,
        };
        app.apply_resource_selection().expect("select key");
        assert_eq!(app.target, AdminTarget::Kv);
        assert_eq!(field_value(&app, "key"), Some("mykey".to_string()));
    }
}
