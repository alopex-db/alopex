use std::time::Duration;

use alopex_cli::batch::{BatchMode, BatchModeSource};
use alopex_cli::cli::{OutputFormat, SqlCommand};
use alopex_cli::commands::sql::{execute_with_formatter_control, SqlExecutionOptions};
use alopex_cli::models::{Column, DataType, Row, Value};
use alopex_cli::output::formatter::create_formatter;
use alopex_cli::streaming::{CancelSignal, Deadline};
use alopex_cli::tui::{is_tty, TuiApp};
use alopex_embedded::Database;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::backend::TestBackend;
use ratatui::Terminal;

fn sample_columns() -> Vec<Column> {
    vec![
        Column::new("id", DataType::Int),
        Column::new("name", DataType::Text),
    ]
}

fn sample_rows() -> Vec<Row> {
    vec![
        Row::new(vec![Value::Int(1), Value::Text("alpha".to_string())]),
        Row::new(vec![Value::Int(2), Value::Text("beta".to_string())]),
    ]
}

fn batch_mode() -> BatchMode {
    BatchMode {
        is_batch: true,
        is_tty: true,
        source: BatchModeSource::Explicit,
    }
}

#[test]
fn tui_renders_without_panic() {
    let columns = sample_columns();
    let rows = sample_rows();
    let mut app = TuiApp::new(columns, rows, "local", false);
    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).expect("terminal");

    terminal.draw(|frame| app.draw(frame)).expect("draw");
    let buffer = terminal.backend().buffer();
    assert!(buffer.area.width > 0);
}

#[test]
fn tui_keybindings_update_selection() {
    let columns = sample_columns();
    let rows = sample_rows();
    let mut app = TuiApp::new(columns, rows, "local", false);

    assert_eq!(app.selected_index(), Some(0));
    app.handle_key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE))
        .unwrap();
    assert_eq!(app.selected_index(), Some(1));
    app.handle_key(KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE))
        .unwrap();
    assert_eq!(app.selected_index(), Some(0));
}

#[test]
fn tui_toggle_help_and_detail() {
    let columns = sample_columns();
    let rows = sample_rows();
    let mut app = TuiApp::new(columns, rows, "local", false);

    assert!(!app.is_help_visible());
    app.handle_key(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE))
        .unwrap();
    assert!(app.is_help_visible());

    assert!(!app.is_detail_visible());
    app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
        .unwrap();
    assert!(app.is_detail_visible());
}

#[test]
fn tui_search_selects_match() {
    let columns = sample_columns();
    let rows = sample_rows();
    let mut app = TuiApp::new(columns, rows, "local", false);

    app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE))
        .unwrap();
    app.handle_key(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::NONE))
        .unwrap();

    assert_eq!(app.selected_index(), Some(1));
}

#[test]
fn tui_falls_back_in_non_tty() {
    if is_tty() {
        return;
    }

    let db = Database::open_in_memory().expect("db");
    db.execute_sql("CREATE TABLE t (id INTEGER);").unwrap();
    db.execute_sql("INSERT INTO t (id) VALUES (1);").unwrap();

    let cmd = SqlCommand {
        query: Some("SELECT id FROM t".to_string()),
        file: None,
        fetch_size: None,
        max_rows: None,
        deadline: None,
        tui: true,
    };
    let formatter = create_formatter(OutputFormat::Json);
    let cancel = CancelSignal::new();
    let deadline = Deadline::new(Duration::from_secs(1));
    let mut output = Vec::new();

    execute_with_formatter_control(
        &db,
        cmd,
        &batch_mode(),
        &mut output,
        formatter,
        SqlExecutionOptions {
            limit: None,
            quiet: true,
            cancel: &cancel,
            deadline: &deadline,
        },
    )
    .expect("fallback");

    let text = String::from_utf8(output).expect("utf8");
    let value: serde_json::Value = serde_json::from_str(&text).expect("json");
    assert!(value.is_array());
}
