//! Detail panel for selected row.

use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::models::{Column, Row, Value};

/// Detail panel state.
#[derive(Default)]
pub struct DetailPanel {
    visible: bool,
    scroll: u16,
}

impl DetailPanel {
    pub fn toggle(&mut self) {
        self.visible = !self.visible;
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn scroll_up(&mut self) {
        self.scroll = self.scroll.saturating_sub(1);
    }

    pub fn scroll_down(&mut self) {
        self.scroll = self.scroll.saturating_add(1);
    }

    pub fn render(&self, frame: &mut Frame<'_>, area: Rect, columns: &[Column], row: &Row) {
        let content = format_row_detail(columns, row);
        let paragraph = Paragraph::new(content)
            .block(Block::default().borders(Borders::ALL).title("Detail"))
            .style(Style::default().fg(Color::White))
            .scroll((self.scroll, 0))
            .wrap(Wrap { trim: false });
        frame.render_widget(paragraph, area);
    }

    pub fn render_empty(&self, frame: &mut Frame<'_>, area: Rect) {
        let paragraph = Paragraph::new("No row selected")
            .block(Block::default().borders(Borders::ALL).title("Detail"))
            .style(Style::default().fg(Color::Gray));
        frame.render_widget(paragraph, area);
    }
}

fn format_row_detail(columns: &[Column], row: &Row) -> String {
    let mut map = serde_json::Map::new();
    for (idx, column) in columns.iter().enumerate() {
        let value = row
            .columns
            .get(idx)
            .map(value_to_json)
            .unwrap_or(serde_json::Value::Null);
        map.insert(column.name.clone(), value);
    }
    let json = serde_json::Value::Object(map);

    let json_text = serde_json::to_string_pretty(&json).unwrap_or_else(|_| "{}".to_string());
    let yaml_text = serde_yaml::to_string(&json).unwrap_or_else(|_| "".to_string());

    format!("JSON\n{json_text}\n\nYAML\n{yaml_text}")
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int(value) => serde_json::Value::Number((*value).into()),
        Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(value) => serde_json::Value::String(value.clone()),
        Value::Bytes(bytes) => serde_json::Value::Array(
            bytes
                .iter()
                .map(|byte| serde_json::Value::Number((*byte).into()))
                .collect(),
        ),
        Value::Vector(values) => serde_json::Value::Array(
            values
                .iter()
                .filter_map(|value| serde_json::Number::from_f64(*value as f64))
                .map(serde_json::Value::Number)
                .collect(),
        ),
    }
}
