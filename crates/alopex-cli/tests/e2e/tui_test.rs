use alopex_cli::models::{Column, DataType, Row, Value};
use alopex_cli::tui::{EventResult, TuiApp};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

fn sample_data() -> (Vec<Column>, Vec<Row>) {
    let columns = vec![
        Column::new("id", DataType::Int),
        Column::new("name", DataType::Text),
    ];
    let rows = vec![
        Row::new(vec![Value::Int(1), Value::Text("alpha".to_string())]),
        Row::new(vec![Value::Int(2), Value::Text("beta".to_string())]),
    ];
    (columns, rows)
}

#[test]
fn e2e_tui_navigation_search_exit() {
    let (columns, rows) = sample_data();
    let mut app = TuiApp::new(columns, rows, "local", false);

    assert_eq!(app.selected_index(), Some(0));
    app.handle_key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE))
        .unwrap();
    assert_eq!(app.selected_index(), Some(1));

    app.handle_key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE))
        .unwrap();
    app.handle_key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE))
        .unwrap();
    assert_eq!(app.selected_index(), Some(0));

    app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
        .unwrap();
    assert!(app.is_detail_visible());

    let result = app
        .handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
        .unwrap();
    assert_eq!(result, EventResult::Exit);
}
