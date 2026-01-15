//! Keybindings for TUI actions.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    MoveUp,
    MoveDown,
    MoveLeft,
    MoveRight,
    PageUp,
    PageDown,
    JumpTop,
    JumpBottom,
    ToggleDetail,
    ToggleHelp,
    SearchMode,
    SearchNext,
    SearchPrev,
    InputChar(char),
    Backspace,
    ConfirmSearch,
    CancelSearch,
    DetailUp,
    DetailDown,
    Quit,
}

pub fn action_for_key(event: KeyEvent, search_active: bool) -> Option<Action> {
    if search_active {
        return match event.code {
            KeyCode::Char('n') => Some(Action::SearchNext),
            KeyCode::Char('N') => Some(Action::SearchPrev),
            KeyCode::Enter => Some(Action::ConfirmSearch),
            KeyCode::Esc => Some(Action::CancelSearch),
            KeyCode::Backspace => Some(Action::Backspace),
            KeyCode::Char(ch) => Some(Action::InputChar(ch)),
            _ => None,
        };
    }

    match (event.code, event.modifiers) {
        (KeyCode::Char('q'), _) => Some(Action::Quit),
        (KeyCode::Esc, _) => Some(Action::Quit),
        (KeyCode::Char('?'), _) => Some(Action::ToggleHelp),
        (KeyCode::Char('/'), _) => Some(Action::SearchMode),
        (KeyCode::Char('n'), _) => Some(Action::SearchNext),
        (KeyCode::Char('N'), _) => Some(Action::SearchPrev),
        (KeyCode::Char('k'), _) | (KeyCode::Up, _) => Some(Action::MoveUp),
        (KeyCode::Char('j'), _) | (KeyCode::Down, _) => Some(Action::MoveDown),
        (KeyCode::Char('h'), _) | (KeyCode::Left, _) => Some(Action::MoveLeft),
        (KeyCode::Char('l'), _) | (KeyCode::Right, _) => Some(Action::MoveRight),
        (KeyCode::Char('g'), KeyModifiers::NONE) => Some(Action::JumpTop),
        (KeyCode::Char('G'), _) => Some(Action::JumpBottom),
        (KeyCode::Char('d'), KeyModifiers::CONTROL) => Some(Action::PageDown),
        (KeyCode::Char('u'), KeyModifiers::CONTROL) => Some(Action::PageUp),
        (KeyCode::Enter, _) => Some(Action::ToggleDetail),
        (KeyCode::PageDown, _) => Some(Action::PageDown),
        (KeyCode::PageUp, _) => Some(Action::PageUp),
        (KeyCode::Char('J'), _) => Some(Action::DetailDown),
        (KeyCode::Char('K'), _) => Some(Action::DetailUp),
        _ => None,
    }
}

pub fn help_items() -> Vec<(&'static str, &'static str)> {
    vec![
        ("q", "Quit"),
        ("Esc", "Quit"),
        ("?", "Toggle help"),
        ("/", "Search"),
        ("n/N", "Next/prev match"),
        ("hjkl", "Move selection"),
        ("g/G", "Jump top/bottom"),
        ("Ctrl+d/u", "Page down/up"),
        ("Enter", "Toggle detail"),
        ("J/K", "Scroll detail"),
    ]
}
