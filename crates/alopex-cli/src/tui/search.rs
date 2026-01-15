//! Incremental search state for TUI.

use regex::Regex;

use crate::error::{CliError, Result};

use super::table::{format_value, TableView};

/// Search state for TUI table.
#[derive(Default)]
pub struct SearchState {
    active: bool,
    query: String,
    regex: Option<Regex>,
    matches: Vec<(usize, usize)>,
    current: Option<usize>,
}

impl SearchState {
    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn has_query(&self) -> bool {
        !self.query.is_empty()
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn activate(&mut self) {
        self.active = true;
    }

    pub fn deactivate(&mut self) {
        self.active = false;
    }

    pub fn cancel(&mut self) {
        self.active = false;
        self.query.clear();
        self.regex = None;
        self.matches.clear();
        self.current = None;
    }

    pub fn push_char(&mut self, ch: char, table: &TableView) -> Result<()> {
        self.query.push(ch);
        self.recompute(table)?;
        Ok(())
    }

    pub fn backspace(&mut self, table: &TableView) -> Result<()> {
        self.query.pop();
        self.recompute(table)?;
        Ok(())
    }

    pub fn next_match(&mut self, table: &TableView) -> Result<Option<usize>> {
        if self.matches.is_empty() {
            self.recompute(table)?;
        }
        if self.matches.is_empty() {
            return Ok(None);
        }
        let next = match self.current {
            None => 0,
            Some(idx) => (idx + 1) % self.matches.len(),
        };
        self.current = Some(next);
        Ok(self.matches.get(next).map(|(row, _)| *row))
    }

    pub fn prev_match(&mut self, table: &TableView) -> Result<Option<usize>> {
        if self.matches.is_empty() {
            self.recompute(table)?;
        }
        if self.matches.is_empty() {
            return Ok(None);
        }
        let prev = match self.current {
            None => 0,
            Some(idx) => idx.saturating_sub(1),
        };
        self.current = Some(prev);
        Ok(self.matches.get(prev).map(|(row, _)| *row))
    }

    pub fn current_match(&self) -> Option<usize> {
        self.current
            .and_then(|idx| self.matches.get(idx).map(|(row, _)| *row))
    }

    pub fn matches_cell(&self, row: usize, col: usize, text: &str) -> bool {
        self.regex
            .as_ref()
            .map(|regex| regex.is_match(text) && self.matches.contains(&(row, col)))
            .unwrap_or(false)
    }

    pub fn recompute(&mut self, table: &TableView) -> Result<()> {
        self.matches.clear();
        self.current = None;
        if self.query.is_empty() {
            self.regex = None;
            return Ok(());
        }

        let regex = regex::RegexBuilder::new(&self.query)
            .case_insensitive(true)
            .build()
            .map_err(|err| CliError::InvalidArgument(err.to_string()))?;

        for (row_idx, row) in table.rows().iter().enumerate() {
            for (col_idx, value) in row.columns.iter().enumerate() {
                let text = format_value(value);
                if regex.is_match(&text) {
                    self.matches.push((row_idx, col_idx));
                }
            }
        }

        if !self.matches.is_empty() {
            self.current = Some(0);
        }
        self.regex = Some(regex);
        Ok(())
    }
}
