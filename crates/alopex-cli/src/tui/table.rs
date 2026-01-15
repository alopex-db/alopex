//! Table view widget for TUI.

use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Row as TuiRow, Table, TableState};
use ratatui::Frame;

use crate::models::{Column, Row, Value};

use super::search::SearchState;

const MAX_CELL_WIDTH: usize = 32;

/// Scrollable table view state.
pub struct TableView {
    columns: Vec<Column>,
    rows: Vec<Row>,
    state: TableState,
    row_offset: usize,
    col_offset: usize,
    view_height: usize,
}

impl TableView {
    pub fn new(columns: Vec<Column>, rows: Vec<Row>) -> Self {
        let mut state = TableState::default();
        if !rows.is_empty() {
            state.select(Some(0));
        }
        Self {
            columns,
            rows,
            state,
            row_offset: 0,
            col_offset: 0,
            view_height: 10,
        }
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn selected_row(&self) -> Option<&Row> {
        let selected = self.state.selected()?;
        self.rows.get(selected)
    }

    #[allow(dead_code)]
    pub fn selected_index(&self) -> Option<usize> {
        self.state.selected()
    }

    pub fn select_row(&mut self, index: usize) {
        if self.rows.is_empty() {
            return;
        }
        let index = index.min(self.rows.len() - 1);
        self.state.select(Some(index));
        self.ensure_visible(index);
    }

    pub fn move_up(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        let selected = self.state.selected().unwrap_or(0);
        let next = selected.saturating_sub(1);
        self.state.select(Some(next));
        self.ensure_visible(next);
    }

    pub fn move_down(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        let selected = self.state.selected().unwrap_or(0);
        let next = (selected + 1).min(self.rows.len().saturating_sub(1));
        self.state.select(Some(next));
        self.ensure_visible(next);
    }

    pub fn move_left(&mut self) {
        self.col_offset = self.col_offset.saturating_sub(1);
    }

    pub fn move_right(&mut self) {
        if self.col_offset + 1 < self.columns.len() {
            self.col_offset += 1;
        }
    }

    pub fn page_up(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        let selected = self.state.selected().unwrap_or(0);
        let max_visible = self.view_height.saturating_sub(3).max(1);
        let delta = (max_visible / 2).max(1);
        let next = selected.saturating_sub(delta);
        self.state.select(Some(next));
        self.ensure_visible(next);
    }

    pub fn page_down(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        let selected = self.state.selected().unwrap_or(0);
        let max_visible = self.view_height.saturating_sub(3).max(1);
        let delta = (max_visible / 2).max(1);
        let next = (selected + delta).min(self.rows.len().saturating_sub(1));
        self.state.select(Some(next));
        self.ensure_visible(next);
    }

    pub fn jump_top(&mut self) {
        self.state.select(Some(0));
        self.row_offset = 0;
    }

    pub fn jump_bottom(&mut self) {
        if self.rows.is_empty() {
            return;
        }
        let last = self.rows.len() - 1;
        self.state.select(Some(last));
        self.ensure_visible(last);
    }

    pub fn render(&mut self, frame: &mut Frame<'_>, area: Rect, search: &SearchState) {
        self.view_height = area.height as usize;
        let columns = self.visible_columns();
        let header_cells = columns
            .iter()
            .map(|col| Cell::from(col.name.clone()).style(header_style()));
        let mut header = vec![Cell::from("#").style(header_style())];
        header.extend(header_cells);

        let row_range = self.visible_row_range();
        let rows = self.rows[row_range.clone()]
            .iter()
            .enumerate()
            .map(|(idx, row)| {
                let row_index = self.row_offset + idx;
                let mut cells = Vec::with_capacity(columns.len() + 1);
                cells.push(Cell::from((row_index + 1).to_string()));
                for (col_index, value) in row.columns.iter().enumerate().skip(self.col_offset) {
                    if col_index >= self.col_offset + columns.len() {
                        break;
                    }
                    let text = format_value(value);
                    let mut cell = Cell::from(text.clone());
                    if search.matches_cell(row_index, col_index, &text) {
                        cell = cell.style(Style::default().fg(Color::Yellow));
                    }
                    cells.push(cell);
                }
                TuiRow::new(cells)
            });

        let widths = self.column_widths(&columns);
        let mut constraints = Vec::with_capacity(widths.len() + 1);
        constraints.push(Constraint::Length(4));
        constraints.extend(widths.into_iter().map(|w| Constraint::Length(w as u16)));

        let table = Table::new(rows, constraints)
            .header(TuiRow::new(header))
            .block(Block::default().borders(Borders::ALL).title("Results"))
            .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
            .highlight_symbol("▌");

        frame.render_stateful_widget(table, area, &mut self.state);
    }

    fn visible_row_range(&self) -> std::ops::Range<usize> {
        if self.rows.is_empty() {
            return 0..0;
        }
        let max_rows = self.view_height.saturating_sub(3).max(1);
        let end = (self.row_offset + max_rows).min(self.rows.len());
        self.row_offset..end
    }

    fn ensure_visible(&mut self, row: usize) {
        if row < self.row_offset {
            self.row_offset = row;
        }
        let max_visible = self.view_height.saturating_sub(3).max(1);
        if row >= self.row_offset + max_visible {
            self.row_offset = row.saturating_sub(max_visible - 1);
        }
    }

    fn visible_columns(&self) -> Vec<Column> {
        if self.columns.is_empty() {
            return Vec::new();
        }
        let start = self.col_offset.min(self.columns.len() - 1);
        self.columns[start..].to_vec()
    }

    fn column_widths(&self, columns: &[Column]) -> Vec<usize> {
        columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let width = col.name.len();
                let mut max_width = width;
                for row in &self.rows {
                    if let Some(value) = row.columns.get(self.col_offset + idx) {
                        let value_len = format_value(value).len();
                        max_width = max_width.max(value_len);
                    }
                }
                max_width.clamp(4, MAX_CELL_WIDTH)
            })
            .collect()
    }
}

fn header_style() -> Style {
    Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD)
}

pub fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{:.6}", f),
        Value::Text(s) => s.clone(),
        Value::Bytes(b) => {
            let hex: String = b
                .iter()
                .take(32)
                .map(|byte| format!("{:02x}", byte))
                .collect();
            if b.len() > 32 {
                format!("{}...", hex)
            } else {
                hex
            }
        }
        Value::Vector(v) => {
            if v.len() <= 4 {
                format!(
                    "[{}]",
                    v.iter()
                        .map(|x| format!("{:.4}", x))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!(
                    "[{}, ... ({} dims)]",
                    v.iter()
                        .take(3)
                        .map(|x| format!("{:.4}", x))
                        .collect::<Vec<_>>()
                        .join(", "),
                    v.len()
                )
            }
        }
    }
}
