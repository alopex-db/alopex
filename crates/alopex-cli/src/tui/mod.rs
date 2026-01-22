//! TUI application module.

pub mod admin;
pub mod detail;
pub mod keymap;
pub mod renderer;
pub mod search;
pub mod table;

use std::io::{self, IsTerminal, Stdout};
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
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Terminal;

use crate::error::{CliError, Result};
use crate::models::{Column, Row};

use self::detail::DetailPanel;
use self::keymap::{action_for_key, help_items, Action};
use self::search::SearchState;
use self::table::TableView;

/// TUI application state.
pub struct TuiApp<'a> {
    table: TableView,
    search: SearchState,
    detail: DetailPanel,
    show_help: bool,
    connection_label: String,
    row_count: usize,
    processing: bool,
    status_message: Option<String>,
    context_message: Option<String>,
    admin_launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
    admin_requested: bool,
}

/// Result of handling an input event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventResult {
    Continue,
    Exit,
}

impl<'a> TuiApp<'a> {
    pub fn new(
        columns: Vec<Column>,
        rows: Vec<Row>,
        connection_label: impl Into<String>,
        processing: bool,
    ) -> Self {
        let row_count = rows.len();
        let table = TableView::new(columns, rows);
        let search = SearchState::default();
        let detail = DetailPanel::default();
        Self {
            table,
            search,
            detail,
            show_help: false,
            connection_label: connection_label.into(),
            row_count,
            processing,
            status_message: None,
            context_message: None,
            admin_launcher: None,
            admin_requested: false,
        }
    }

    pub fn with_admin_launcher(
        mut self,
        launcher: Option<Box<dyn FnMut() -> Result<()> + 'a>>,
    ) -> Self {
        self.admin_launcher = launcher;
        self
    }

    pub fn with_status_message(mut self, message: impl Into<String>) -> Self {
        self.status_message = Some(message.into());
        self
    }

    pub fn with_context_message(mut self, message: Option<String>) -> Self {
        self.context_message = message;
        self
    }

    pub fn run(mut self) -> Result<()> {
        if !is_tty() {
            return Err(CliError::InvalidArgument(
                "TUI requires a TTY. Run without --tui in batch mode.".to_string(),
            ));
        }
        loop {
            enable_raw_mode()?;
            let mut stdout = io::stdout();
            execute!(stdout, EnterAlternateScreen)?;

            let backend = CrosstermBackend::new(stdout);
            let mut terminal = Terminal::new(backend)?;
            terminal.clear()?;

            let tick_rate = Duration::from_millis(16);
            let mut last_tick = Instant::now();
            let mut processing_cleared = false;

            loop {
                terminal.draw(|frame| self.draw(frame))?;

                if self.processing && !processing_cleared {
                    self.processing = false;
                    processing_cleared = true;
                }

                let timeout = tick_rate
                    .checked_sub(last_tick.elapsed())
                    .unwrap_or_else(|| Duration::from_secs(0));

                if event::poll(timeout)? {
                    if let Event::Key(key) = event::read()? {
                        match self.handle_key(key)? {
                            EventResult::Exit => break,
                            EventResult::Continue => {}
                        }
                    }
                }

                if last_tick.elapsed() >= tick_rate {
                    last_tick = Instant::now();
                }
            }

            cleanup_terminal(terminal)?;

            if self.admin_requested {
                self.admin_requested = false;
                if let Some(launcher) = self.admin_launcher.as_mut() {
                    launcher()?;
                    continue;
                }
            }

            return Ok(());
        }
    }

    pub fn draw(&mut self, frame: &mut ratatui::Frame<'_>) {
        let area = frame.size();
        let mut constraints = Vec::new();
        if self.context_message.is_some() {
            constraints.push(Constraint::Length(3));
        }
        constraints.push(Constraint::Min(5));
        if self.detail.is_visible() {
            constraints.push(Constraint::Length(8));
        } else {
            constraints.push(Constraint::Length(0));
        }
        constraints.push(Constraint::Length(3));
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints(constraints)
            .split(area);
        let mut idx = 0;
        if let Some(context) = self.context_message.as_ref() {
            let header = Paragraph::new(context.clone())
                .block(Block::default().borders(Borders::ALL).title("Command"))
                .wrap(Wrap { trim: true });
            frame.render_widget(header, chunks[idx]);
            idx += 1;
        }
        let table_area = chunks[idx];
        let detail_area = chunks[idx + 1];
        let status_area = chunks[idx + 2];

        self.table.render(frame, table_area, &self.search);

        if self.detail.is_visible() {
            if let Some(selected) = self.table.selected_row() {
                self.detail
                    .render(frame, detail_area, self.table.columns(), selected);
            } else {
                self.detail.render_empty(frame, detail_area);
            }
        }

        let admin_available = self.admin_launcher.is_some();
        render_status(
            frame,
            status_area,
            &self.search,
            self.show_help,
            &self.connection_label,
            self.row_count,
            self.processing,
            self.status_message.as_deref(),
            admin_available,
        );

        if self.show_help {
            render_help(frame, area, admin_available);
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<EventResult> {
        if self.show_help && key.code == KeyCode::Esc {
            self.show_help = false;
            return Ok(EventResult::Continue);
        }
        if let Some(action) = action_for_key(key, self.search.is_active()) {
            return self.handle_action(action);
        }
        Ok(EventResult::Continue)
    }

    fn handle_action(&mut self, action: Action) -> Result<EventResult> {
        match action {
            Action::Quit => {
                if self.show_help {
                    self.show_help = false;
                    return Ok(EventResult::Continue);
                }
                return Ok(EventResult::Exit);
            }
            Action::ToggleHelp => {
                self.show_help = !self.show_help;
            }
            Action::MoveUp => self.table.move_up(),
            Action::MoveDown => self.table.move_down(),
            Action::MoveLeft => self.table.move_left(),
            Action::MoveRight => self.table.move_right(),
            Action::PageUp => self.table.page_up(),
            Action::PageDown => self.table.page_down(),
            Action::JumpTop => self.table.jump_top(),
            Action::JumpBottom => self.table.jump_bottom(),
            Action::ToggleDetail => self.detail.toggle(),
            Action::SearchMode => self.search.activate(),
            Action::SearchNext => {
                let next = self.search.next_match(&self.table)?;
                self.select_match(next);
            }
            Action::SearchPrev => {
                let prev = self.search.prev_match(&self.table)?;
                self.select_match(prev);
            }
            Action::InputChar(ch) => {
                self.search.push_char(ch, &self.table)?;
                self.select_match(self.search.current_match());
            }
            Action::Backspace => {
                self.search.backspace(&self.table)?;
                self.select_match(self.search.current_match());
            }
            Action::ConfirmSearch => {
                self.search.deactivate();
                self.select_match(self.search.current_match());
            }
            Action::CancelSearch => self.search.cancel(),
            Action::DetailUp => self.detail.scroll_up(),
            Action::DetailDown => self.detail.scroll_down(),
            Action::OpenAdmin => {
                if self.admin_launcher.is_some() {
                    self.admin_requested = true;
                    return Ok(EventResult::Exit);
                }
            }
        }
        Ok(EventResult::Continue)
    }

    fn select_match(&mut self, row: Option<usize>) {
        if let Some(row) = row {
            self.table.select_row(row);
        }
    }

    #[allow(dead_code)]
    pub fn selected_index(&self) -> Option<usize> {
        self.table.selected_index()
    }

    #[allow(dead_code)]
    pub fn is_detail_visible(&self) -> bool {
        self.detail.is_visible()
    }

    #[allow(dead_code)]
    pub fn is_help_visible(&self) -> bool {
        self.show_help
    }

    #[allow(dead_code)]
    pub fn take_admin_launcher(&mut self) -> Option<Box<dyn FnMut() -> Result<()> + 'a>> {
        self.admin_launcher.take()
    }

    #[allow(dead_code)]
    pub fn admin_requested(&self) -> bool {
        self.admin_requested
    }
}

#[allow(clippy::too_many_arguments)]
fn render_status(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    search: &SearchState,
    show_help: bool,
    connection_label: &str,
    row_count: usize,
    processing: bool,
    status_message: Option<&str>,
    admin_available: bool,
) {
    let state_label = if processing { "processing" } else { "ready" };
    let focus_label = if show_help {
        "Help"
    } else if search.is_active() || search.has_query() {
        "Search"
    } else {
        "Table"
    };
    let action_label = if show_help {
        "help"
    } else if search.is_active() || search.has_query() {
        "search"
    } else {
        "browse"
    };
    let highlight = Style::default()
        .fg(Color::Yellow)
        .add_modifier(Modifier::BOLD);

    let mut spans = Vec::new();
    let push_sep = |spans: &mut Vec<Span<'_>>| {
        spans.push(Span::raw(" | "));
    };

    spans.push(Span::raw("Connection: "));
    spans.push(Span::styled(connection_label.to_string(), highlight));
    push_sep(&mut spans);
    spans.push(Span::raw("Focus: "));
    spans.push(Span::styled(focus_label.to_string(), highlight));
    push_sep(&mut spans);
    spans.push(Span::raw("Action: "));
    spans.push(Span::styled(action_label.to_string(), highlight));
    spans.push(Span::raw(format!(
        " (Rows: {row_count}, Status: {state_label})"
    )));
    if search.is_active() || search.has_query() {
        push_sep(&mut spans);
        spans.push(Span::raw(format!("Query: /{}", search.query())));
    }
    push_sep(&mut spans);

    let (ops_text, move_text) = if show_help {
        ("?: close".to_string(), "-".to_string())
    } else if search.is_active() {
        (
            "Enter: confirm, Esc: cancel".to_string(),
            "n/N: next/prev".to_string(),
        )
    } else if search.has_query() {
        ("/: search".to_string(), "n/N: next/prev".to_string())
    } else {
        let mut ops = vec!["Enter: detail", "/: search", "?: help", "q/Esc: quit"];
        if admin_available {
            ops.insert(2, "a: admin/back");
        }
        (ops.join(", "), "j/k, h/l, g/G, Ctrl+d/u".to_string())
    };

    spans.push(Span::styled(format!("Ops: {ops_text}"), highlight));
    push_sep(&mut spans);
    if move_text == "-" {
        spans.push(Span::raw("Move: -"));
    } else {
        spans.push(Span::raw(format!("Move: {move_text}")));
    }

    if let Some(message) = status_message {
        push_sep(&mut spans);
        spans.push(Span::raw(message.to_string()));
    }

    let paragraph = Paragraph::new(Line::from(spans))
        .block(Block::default().borders(Borders::ALL).title("Status"))
        .style(Style::default().fg(Color::Gray))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_help(frame: &mut ratatui::Frame<'_>, area: Rect, admin_available: bool) {
    let help_width = area.width.saturating_sub(4).min(60);
    let help_height = area.height.saturating_sub(4).min(18);
    let rect = Rect::new(
        area.x + (area.width.saturating_sub(help_width)) / 2,
        area.y + (area.height.saturating_sub(help_height)) / 2,
        help_width,
        help_height,
    );

    let lines = help_items(admin_available)
        .iter()
        .map(|(key, desc)| format!("{key:<8} {desc}"))
        .collect::<Vec<_>>()
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

pub fn is_tty() -> bool {
    let forced = std::env::var("ALOPEX_TEST_TTY")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false);
    forced || (std::io::stdout().is_terminal() && std::io::stdin().is_terminal())
}
