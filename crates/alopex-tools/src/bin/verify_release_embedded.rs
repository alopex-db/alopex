//! リリース確認(SF-EMB): crates.io から取得した公開版 alopex-embedded を
//! 使い、SQL コーパスを実行して正規化 JSON を標準出力へ書く。
//!
//! alopex-tools は親ワークスペースに含まれない独立クレートであり
//! (crates/alopex-tools/Cargo.toml の [workspace] 空テーブル参照)、
//! ここでの alopex-embedded / alopex-sql への依存は常に crates.io の
//! 公開バージョンを指す(path 依存ではない)。つまりこのバイナリの
//! ビルドが通ること自体が「公開クレートが実際に取得・ビルドできる」
//! ことの検証になる。
//!
//! 比較・期待値照合は行わない(scripts/parity/runner/normalize.py /
//! report.py が既に持つロジックの二重実装を避けるため、Python 側が
//! この出力を読んで既存の scripts/parity/expected/*.json と突き合わせる)。
//!
//! crates/alopex-embedded/tests/parity_corpus.rs の実行・正規化ロジックの
//! 移植(比較・assert 部分を除く)。契約(環境変数・出力スキーマ)は
//! 同一なので、EmbeddedSurface(scripts/parity/runner/surfaces.py)の
//! released モードはこのバイナリを起動し、同じ PARITY_CORPUS_DIR /
//! PARITY_DATA_DIR / PARITY_ROLE / PARITY_OUTPUT を渡す。

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use alopex_embedded::Database;
use alopex_sql::ExecutionResult;
use alopex_sql::SqlValue;
use serde_json::{Map, Value, json};

fn split_sql_statements(text: &str) -> Result<Vec<String>, String> {
    let bytes = text.as_bytes();
    let n = bytes.len();
    let mut statements: Vec<String> = Vec::new();
    let mut buf: Vec<u8> = Vec::new();
    let mut i = 0usize;
    let mut in_squote = false;
    let mut in_dquote = false;

    let mut push_statement = |buf: &mut Vec<u8>| -> Result<(), String> {
        let stmt = String::from_utf8(std::mem::take(buf))
            .map_err(|e| format!("コーパスが UTF-8 でない: {e}"))?;
        let trimmed = stmt.trim();
        if !trimmed.is_empty() {
            statements.push(trimmed.to_string());
        }
        Ok(())
    };

    while i < n {
        let ch = bytes[i];
        if in_squote {
            buf.push(ch);
            if ch == b'\'' {
                if i + 1 < n && bytes[i + 1] == b'\'' {
                    buf.push(b'\'');
                    i += 2;
                    continue;
                }
                in_squote = false;
            }
            i += 1;
            continue;
        }
        if in_dquote {
            buf.push(ch);
            if ch == b'"' {
                in_dquote = false;
            }
            i += 1;
            continue;
        }
        if ch == b'\'' {
            in_squote = true;
            buf.push(ch);
            i += 1;
            continue;
        }
        if ch == b'"' {
            in_dquote = true;
            buf.push(ch);
            i += 1;
            continue;
        }
        if ch == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < n && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if ch == b'/' && bytes.get(i + 1) == Some(&b'*') {
            match text[i + 2..].find("*/") {
                Some(end) => {
                    buf.push(b' ');
                    i = i + 2 + end + 2;
                    continue;
                }
                None => return Err("閉じられていないブロックコメント".to_string()),
            }
        }
        if ch == b';' {
            push_statement(&mut buf)?;
            i += 1;
            continue;
        }
        buf.push(ch);
        i += 1;
    }
    push_statement(&mut buf)?;
    Ok(statements)
}

fn round_sig9(v: f64) -> Value {
    if v.is_nan() {
        return Value::String("NaN".to_string());
    }
    if v.is_infinite() {
        return Value::String(if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string());
    }
    let rounded: f64 = format!("{v:.8e}")
        .parse()
        .expect("{:.8e} は常に有効な float 表記");
    Value::Number(serde_json::Number::from_f64(rounded).expect("有限値の変換は失敗しない"))
}

fn normalize_value(value: &SqlValue) -> Result<Value, String> {
    match value {
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Boolean(b) => Ok(json!(b)),
        SqlValue::Integer(i) => Ok(json!(i)),
        SqlValue::BigInt(i) => Ok(json!(i)),
        SqlValue::Float(f) => Ok(round_sig9(f64::from(*f))),
        SqlValue::Double(f) => Ok(round_sig9(*f)),
        SqlValue::Text(s) => Ok(json!(s)),
        SqlValue::Vector(xs) => Ok(Value::Array(
            xs.iter().map(|x| round_sig9(f64::from(*x))).collect(),
        )),
        SqlValue::Timestamp(t) => Ok(json!(t)),
        SqlValue::Blob(_) => {
            Err("BLOB 値の正規化表現は未定義(コーパスに BLOB を含めない前提)".to_string())
        }
    }
}

fn classify_error(message: &str) -> Option<(&'static str, String)> {
    const MARKER: &str = "unsupported expression:";
    if let Some(pos) = message.find(MARKER) {
        let rest = message[pos + MARKER.len()..].trim_start();
        let ident: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if !ident.is_empty() {
            return Some(("UNSUPPORTED_EXPRESSION", ident));
        }
    }
    None
}

fn normalize_failure(message: String) -> Value {
    json!({
        "type": "error",
        "error_class": "NORMALIZE_ERROR",
        "object": Value::Null,
        "code": Value::Null,
        "message": message,
    })
}

fn execute_statement(db: &Database, sql: &str) -> Value {
    match db.execute_sql(sql) {
        Ok(ExecutionResult::Success) => json!({ "type": "success" }),
        Ok(ExecutionResult::RowsAffected(n)) => {
            json!({ "type": "rows_affected", "count": n })
        }
        Ok(ExecutionResult::Query(qr)) => {
            let columns: Vec<String> = qr.columns.iter().map(|c| c.name.clone()).collect();
            let mut rows: Vec<Value> = Vec::with_capacity(qr.rows.len());
            for (row_index, row) in qr.rows.iter().enumerate() {
                if row.len() != columns.len() {
                    return normalize_failure(format!(
                        "行 {row_index}: 列数 {} が列名数 {} と不一致",
                        row.len(),
                        columns.len()
                    ));
                }
                let mut named = Map::new();
                for (name, value) in columns.iter().zip(row.iter()) {
                    match normalize_value(value) {
                        Ok(v) => {
                            named.insert(name.clone(), v);
                        }
                        Err(msg) => return normalize_failure(msg),
                    }
                }
                rows.push(Value::Object(named));
            }
            json!({ "type": "query", "columns": columns, "rows": rows })
        }
        Err(err) => {
            let code = err.sql_error_code();
            let message = err.to_string();
            match classify_error(&message) {
                Some((class, object)) => json!({
                    "type": "error",
                    "error_class": class,
                    "object": object,
                    "code": code,
                }),
                None => json!({
                    "type": "error",
                    "error_class": Value::Null,
                    "object": Value::Null,
                    "code": code,
                    "message": message,
                }),
            }
        }
    }
}

fn corpus_number(name: &str) -> Option<u32> {
    let digits: String = name.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn corpus_files(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = fs::read_dir(dir)
        .map_err(|e| format!("コーパスディレクトリを読めない {}: {e}", dir.display()))?;
    let mut files: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|ext| ext == "sql"))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .and_then(corpus_number)
                .is_some_and(|num| (1..=7).contains(&num))
        })
        .collect();
    files.sort();
    if files.is_empty() {
        return Err(format!("コーパス SQL が見つからない: {}", dir.display()));
    }
    Ok(files)
}

fn run() -> Result<(), String> {
    let corpus_dir = env::var("PARITY_CORPUS_DIR")
        .map(PathBuf::from)
        .map_err(|_| "PARITY_CORPUS_DIR が未設定".to_string())?;
    let role = env::var("PARITY_ROLE").unwrap_or_else(|_| "writer".to_string());
    let data_dir = env::var("PARITY_DATA_DIR").ok();
    let output_path = env::var("PARITY_OUTPUT").ok();

    if !corpus_dir.is_dir() {
        return Err(format!(
            "コーパスディレクトリがない: {}",
            corpus_dir.display()
        ));
    }

    let files: Vec<PathBuf> = match role.as_str() {
        "writer" => corpus_files(&corpus_dir)?,
        "reader" => {
            if data_dir.is_none() {
                return Err("reader ロールは PARITY_DATA_DIR が必須".to_string());
            }
            let path = corpus_dir.join("99_verify.sql");
            if !path.is_file() {
                return Err(format!("検証クエリが見つからない: {}", path.display()));
            }
            vec![path]
        }
        other => return Err(format!("不正な PARITY_ROLE: {other}")),
    };

    let db = match &data_dir {
        Some(dir) => Database::open(Path::new(dir))
            .map_err(|e| format!("file モードでデータディレクトリを開けない ({dir}): {e}"))?,
        None => Database::open_in_memory().map_err(|e| format!("インメモリで開けない: {e}"))?,
    };

    let mut corpus_names: Vec<String> = Vec::new();
    let mut output_statements: Vec<Value> = Vec::new();
    let mut global_index = 0usize;

    for file in &files {
        let name = file
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or("コーパスファイル名は UTF-8 である必要がある")?
            .to_string();
        corpus_names.push(name.clone());

        let text =
            fs::read_to_string(file).map_err(|e| format!("{} を読めない: {e}", file.display()))?;
        let statements =
            split_sql_statements(&text).map_err(|e| format!("{name} の分割失敗: {e}"))?;

        for sql in &statements {
            global_index += 1;
            let actual = execute_statement(&db, sql);
            output_statements.push(json!({
                "index": global_index,
                "sql": sql,
                "actual": actual,
            }));
        }
    }

    if let Some(path) = &output_path {
        let doc = json!({
            "corpus": corpus_names.join(","),
            "format": 1,
            "statements": output_statements,
        });
        let serialized =
            serde_json::to_string_pretty(&doc).map_err(|e| format!("JSON 直列化失敗: {e}"))?;
        fs::write(path, serialized)
            .map_err(|e| format!("PARITY_OUTPUT へ書けない ({path}): {e}"))?;
    }

    if role == "writer" && data_dir.is_some() {
        db.flush().map_err(|e| format!("flush 失敗: {e}"))?;
    }
    drop(db);

    eprintln!(
        "[verify-release-embedded] role={role} corpus={}",
        corpus_names.join(",")
    );
    eprintln!("[verify-release-embedded] statements={global_index}");
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("環境エラー: {e}");
            ExitCode::from(2)
        }
    }
}
