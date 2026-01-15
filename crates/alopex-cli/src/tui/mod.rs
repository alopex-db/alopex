//! TUI application module.

pub mod detail;
pub mod keymap;
pub mod search;
pub mod table;

use std::io::{self, IsTerminal, Stdout};
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyEvent};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Terminal;

use crate::error::{CliError, Result};
use crate::models::{Column, Row};

use self::detail::DetailPanel;
use self::keymap::{action_for_key, help_items, Action};
use self::search::SearchState;
use self::table::TableView;

/// TUI application state.
pub struct TuiApp {
    table: TableView,
    search: SearchState,
    detail: DetailPanel,
    show_help: bool,
    connection_label: String,
    row_count: usize,
    processing: bool,
}

/// Result of handling an input event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventResult {
    Continue,
    Exit,
}

impl TuiApp {
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
        }
    }

    pub fn run(mut self) -> Result<()> {
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

        cleanup_terminal(terminal)
    }

    pub fn draw(&mut self, frame: &mut ratatui::Frame<'_>) {
        let area = frame.size();
        let (table_area, detail_area, status_area) = split_layout(area, self.detail.is_visible());

        self.table.render(frame, table_area, &self.search);

        if self.detail.is_visible() {
            if let Some(selected) = self.table.selected_row() {
                self.detail
                    .render(frame, detail_area, self.table.columns(), selected);
            } else {
                self.detail.render_empty(frame, detail_area);
            }
        }

        render_status(
            frame,
            status_area,
            &self.search,
            self.show_help,
            &self.connection_label,
            self.row_count,
            self.processing,
        );

        if self.show_help {
            render_help(frame, area);
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<EventResult> {
        if let Some(action) = action_for_key(key, self.search.is_active()) {
            return self.handle_action(action);
        }
        Ok(EventResult::Continue)
    }

    fn handle_action(&mut self, action: Action) -> Result<EventResult> {
        match action {
            Action::Quit => return Ok(EventResult::Exit),
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
}

fn split_layout(area: Rect, show_detail: bool) -> (Rect, Rect, Rect) {
    let chunks = if show_detail {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(5),
                Constraint::Length(8),
                Constraint::Length(3),
            ])
            .split(area)
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(5),
                Constraint::Length(0),
                Constraint::Length(3),
            ])
            .split(area)
    };

    (chunks[0], chunks[1], chunks[2])
}

fn render_status(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    search: &SearchState,
    show_help: bool,
    connection_label: &str,
    row_count: usize,
    processing: bool,
) {
    let state_label = if processing { "processing" } else { "ready" };
    let base_status =
        format!("Connection: {connection_label} | Rows: {row_count} | Status: {state_label}");
    let status_text = if show_help {
        format!("{base_status} | Help: press ? to close")
    } else if search.is_active() {
        format!("{base_status} | /{}", search.query())
    } else if search.has_query() {
        format!("{base_status} | /{} (n/N)", search.query())
    } else {
        format!("{base_status} | q/Esc: quit | ?: help | /: search | Enter: detail")
    };

    let paragraph = Paragraph::new(status_text)
        .block(Block::default().borders(Borders::ALL).title("Status"))
        .style(Style::default().fg(Color::Gray))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_help(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let help_width = area.width.saturating_sub(4).min(60);
    let help_height = area.height.saturating_sub(4).min(18);
    let rect = Rect::new(
        area.x + (area.width.saturating_sub(help_width)) / 2,
        area.y + (area.height.saturating_sub(help_height)) / 2,
        help_width,
        help_height,
    );

    let lines = help_items()
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
    std::io::stdout().is_terminal() && std::io::stdin().is_terminal()
}
