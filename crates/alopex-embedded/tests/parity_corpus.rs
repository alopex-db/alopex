//! mode-parity 検証(SF-EMB): 組み込み API 経路で SQL コーパスを実行し、
//! 期待値ゴールデン(scripts/parity/expected/*.json)と自己アサートする統合テスト。
//!
//! ハーネス契約(scripts/parity/runner/surfaces.py EmbeddedSurface):
//! - `PARITY_CORPUS_DIR`: コーパス .sql ディレクトリ。未設定時はリポジトリ内の
//!   `scripts/parity/corpus` を使う。
//! - `PARITY_DATA_DIR`  : 未設定ならインメモリ(`Database::open_in_memory`)。
//!   設定時はそのディレクトリを file モード(`Database::open`)で開く。
//! - `PARITY_ROLE`      : `writer` = 番号 01〜07 のコーパスを順に実行 /
//!   `reader` = 99_verify.sql のみ実行(`PARITY_DATA_DIR` 必須)。
//! - `PARITY_OUTPUT`    : 実測の正規化 JSON の書き出し先。スキーマ:
//!   `{"corpus": "<コーパス名>", "format": 1,
//!     "statements": [{"index": <1始まり通し番号>, "sql": "<実行文>", "actual": <正規化結果>}]}`
//!   `actual` は expected/*.json の `expected` キーと同形
//!   (success / rows_affected / query / error の 4 種)。writer は複数コーパスを
//!   連結して 1 ファイルに書くため、`corpus` はカンマ区切りの実行順リストになる。
//!
//! 正規化規則は scripts/parity/runner/normalize.py と同一:
//! - 行は列名をキーとするオブジェクト、列順は `columns` 配列が保持する。
//! - 浮動小数点は有効数字 9 桁へ丸める(Python `%.9g` 相当)。
//! - NULL は JSON null。
//! - エラーは「分類コード + 対象オブジェクト名 + エンジンのエラーコード」へ正規化する。
//!   分類できないエラーを汎用コードへ吸収すると偽陽性一致を生むため、未分類は
//!   error_class を持たず message 全文を保持する(期待値とは必ず不一致になる)。
//!
//! SQL 実行は利用者向け公開 API `Database::execute_sql`(通常経路)のみを使う。
//! 期待値との不一致があっても即 panic せず全文を実行し切り、PARITY_OUTPUT を
//! 書き出した後に不一致サマリを表示して assert 失敗させる。

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use alopex_embedded::Database;
use alopex_sql::ExecutionResult;
use alopex_sql::SqlValue;
use serde_json::{json, Map, Value};

// ---------------------------------------------------------------------------
// SQL コーパスの分割(runner/surfaces.py split_sql_statements の Rust 移植)
// ---------------------------------------------------------------------------

/// SQL テキストを文単位に分割する。
///
/// 文字列リテラル(' / ")内のセミコロン、行コメント(--)、
/// ブロックコメント(/* */)を考慮する。行コメントは除去し(直後の改行は
/// 残る)、ブロックコメントは前後トークンの結合を防ぐため半角スペース
/// 1 つに置換する。Python 実装(surfaces.split_sql_statements)と同一の
/// 出力になること。
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
                    // エスケープ ''
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
                    // コメント 1 つを半角スペース 1 つに置換(トークン結合の防止)
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

// ---------------------------------------------------------------------------
// 値の正規化(runner/normalize.py と同一規則)
// ---------------------------------------------------------------------------

/// 浮動小数点値を有効数字 9 桁へ丸める(Python `float(f"{v:.9g}")` 相当)。
///
/// NaN / Inf は JSON で表現できないため明示的な文字列タグへ写像する
/// (normalize.round_significant と同一)。
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

/// SqlValue を正規化 JSON 値へ変換する(normalize.normalize_scalar と同一規則)。
fn normalize_value(value: &SqlValue) -> Result<Value, String> {
    match value {
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Boolean(b) => Ok(json!(b)),
        SqlValue::Integer(i) => Ok(json!(i)),
        SqlValue::BigInt(i) => Ok(json!(i)),
        SqlValue::Float(f) => Ok(round_sig9(f64::from(*f))),
        SqlValue::Double(f) => Ok(round_sig9(*f)),
        SqlValue::Text(s) => Ok(json!(s)),
        // VECTOR 値は各要素を再帰的に正規化(f32 -> f64 は正確な変換)
        SqlValue::Vector(xs) => Ok(Value::Array(
            xs.iter().map(|x| round_sig9(f64::from(*x))).collect(),
        )),
        // Timestamp の正規化表現はマイクロ秒の整数(コーパスは使用しない)
        SqlValue::Timestamp(t) => Ok(json!(t)),
        SqlValue::Date(days) => Ok(json!(days)),
        SqlValue::Time(micros) => Ok(json!(micros)),
        SqlValue::Interval {
            months,
            days,
            micros,
        } => Ok(json!({ "months": months, "days": days, "microseconds": micros })),
        SqlValue::Decimal(value) => Ok(json!(value.to_string())),
        SqlValue::Blob(_) => {
            Err("BLOB 値の正規化表現は未定義(コーパスに BLOB を含めない前提)".to_string())
        }
    }
}

/// エラーメッセージを「分類コード + 対象オブジェクト名」へ正規化する
/// (runner/normalize.py classify_error と同趣旨)。
///
/// 現行コーパスの期待値に現れる分類は UNSUPPORTED_EXPRESSION のみ
/// (expected/06_subquery.json: `unsupported expression: <TypedExprKind variant>`
/// の variant 名への前方一致)。分類できないメッセージを汎用コードへ吸収すると
/// サーフェス間の偽陽性一致を生むため、未分類は None を返す。
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

// ---------------------------------------------------------------------------
// 文の実行と正規化結果(expected と同形の `actual`)の構築
// ---------------------------------------------------------------------------

/// 1 文を公開 API `Database::execute_sql`(通常経路)で実行し、
/// expected/*.json の `expected` と同形の正規化結果を返す。
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
                // 未分類エラーは error_class を持たず message 全文を保持する
                // (期待値とは必ず不一致 = 偽陽性一致の防止)。
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

/// 正規化不能な結果(スキーマ不正・BLOB 等)の明示表現。期待値と一致しない。
fn normalize_failure(message: String) -> Value {
    json!({
        "type": "error",
        "error_class": "NORMALIZE_ERROR",
        "object": Value::Null,
        "code": Value::Null,
        "message": message,
    })
}

// ---------------------------------------------------------------------------
// 期待値(scripts/parity/expected/*.json)の読み込みと比較
// ---------------------------------------------------------------------------

/// コーパスファイルに対応する期待値(sql, expected)列を読み込む。
/// 期待値は corpus ディレクトリの隣の expected/<stem>.json に置かれる。
fn load_expected_for(corpus_file: &Path) -> Result<Vec<(String, Value)>, String> {
    let corpus_dir = corpus_file
        .parent()
        .ok_or("コーパスファイルに親ディレクトリがない")?;
    let parity_dir = corpus_dir
        .parent()
        .ok_or("コーパスディレクトリに親ディレクトリがない")?;
    let stem = corpus_file
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or("コーパスファイル名が不正")?;
    let path = parity_dir.join("expected").join(format!("{stem}.json"));
    let text =
        fs::read_to_string(&path).map_err(|e| format!("{} を読めない: {e}", path.display()))?;
    let doc: Value = serde_json::from_str(&text)
        .map_err(|e| format!("{} の JSON が不正: {e}", path.display()))?;
    let statements = doc
        .get("statements")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} に statements 配列がない", path.display()))?;
    let mut result = Vec::with_capacity(statements.len());
    for (i, stmt) in statements.iter().enumerate() {
        let sql = stmt
            .get("sql")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{} statements[{i}] に sql がない", path.display()))?;
        let expected = stmt
            .get("expected")
            .cloned()
            .ok_or_else(|| format!("{} statements[{i}] に expected がない", path.display()))?;
        result.push((sql.to_string(), expected));
    }
    Ok(result)
}

/// JSON 値の意味的等価判定。
///
/// verify.py 側の比較は Python の `==`(6 == 6.0 が真)で行われるため、
/// 数値は整数/浮動小数点の型差を吸収して比較する。オブジェクトはキー順不問。
fn json_semantic_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            if let (Some(i), Some(j)) = (x.as_i64(), y.as_i64()) {
                i == j
            } else {
                match (x.as_f64(), y.as_f64()) {
                    (Some(p), Some(q)) => p == q,
                    _ => false,
                }
            }
        }
        (Value::Array(xs), Value::Array(ys)) => {
            xs.len() == ys.len()
                && xs
                    .iter()
                    .zip(ys.iter())
                    .all(|(p, q)| json_semantic_eq(p, q))
        }
        (Value::Object(xs), Value::Object(ys)) => {
            xs.len() == ys.len()
                && xs
                    .iter()
                    .all(|(k, v)| ys.get(k).is_some_and(|w| json_semantic_eq(v, w)))
        }
        _ => a == b,
    }
}

/// SQL 文の同一性サニティチェック用(期待値の sql は空白を手動正規化して
/// あるため、全空白を除去して比較する)。
fn strip_whitespace(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

/// 実行結果列と期待値列を比較し、不一致を mismatches へ追記する。
/// 戻り値は一致した文数。1 文でも不一致があっても全文を比較し切る。
fn compare_corpus(
    corpus_name: &str,
    executed: &[(String, Value)],
    expected: &[(String, Value)],
    mismatches: &mut Vec<String>,
) -> usize {
    if executed.len() != expected.len() {
        mismatches.push(format!(
            "[{corpus_name}] 文数不一致: 実行={} 期待={}",
            executed.len(),
            expected.len()
        ));
    }
    let mut pass = 0usize;
    for (i, ((sql, actual), (expected_sql, expected_value))) in
        executed.iter().zip(expected.iter()).enumerate()
    {
        let stmt_no = i + 1;
        if strip_whitespace(sql) != strip_whitespace(expected_sql) {
            mismatches.push(format!(
                "[{corpus_name}] stmt#{stmt_no} SQL 不一致(文分割ずれの疑い):\n  実行: {sql}\n  期待: {expected_sql}"
            ));
            continue;
        }
        if json_semantic_eq(actual, expected_value) {
            pass += 1;
        } else {
            mismatches.push(format!(
                "[{corpus_name}] stmt#{stmt_no} 結果不一致: {sql}\n  expected: {}\n  actual:   {}",
                serde_json::to_string(expected_value).expect("expected は JSON 由来"),
                serde_json::to_string(actual).expect("actual は JSON 由来"),
            ));
        }
    }
    pass
}

// ---------------------------------------------------------------------------
// メインテスト
// ---------------------------------------------------------------------------

fn default_corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/parity/corpus")
}

/// ファイル名の数値プレフィックス(例: "03_query.sql" -> 3)。
fn corpus_number(name: &str) -> Option<u32> {
    let digits: String = name.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

/// writer コーパス(番号 01〜07)を名前順に列挙する。
///
/// EmbeddedSurface docstring の契約は「writer なら 01〜07 のコーパスを実行」。
/// 99_verify.sql に加えて、S1 デモ専用の追加コーパス(08_server_insert.sql は
/// 第 3 幕でサーバー経由実行する前提のため、writer が実行すると 99_verify の
/// 期待値が壊れる)も含めない。
fn corpus_files(dir: &Path) -> Vec<PathBuf> {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| {
        panic!(
            "環境エラー: コーパスディレクトリを読めない {}: {e}",
            dir.display()
        )
    });
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
    assert!(
        !files.is_empty(),
        "環境エラー: コーパス SQL が見つからない: {}",
        dir.display()
    );
    files
}

#[test]
fn parity_corpus() {
    let corpus_dir = env::var("PARITY_CORPUS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_corpus_dir());
    let role = env::var("PARITY_ROLE").unwrap_or_else(|_| "writer".to_string());
    let data_dir = env::var("PARITY_DATA_DIR").ok();
    let output_path = env::var("PARITY_OUTPUT").ok();

    assert!(
        corpus_dir.is_dir(),
        "環境エラー: コーパスディレクトリがない: {}",
        corpus_dir.display()
    );

    let files: Vec<PathBuf> = match role.as_str() {
        "writer" => corpus_files(&corpus_dir),
        "reader" => {
            assert!(
                data_dir.is_some(),
                "環境エラー: reader ロールは PARITY_DATA_DIR が必須"
            );
            let path = corpus_dir.join("99_verify.sql");
            assert!(
                path.is_file(),
                "環境エラー: 検証クエリが見つからない: {}",
                path.display()
            );
            vec![path]
        }
        other => panic!("環境エラー: 不正な PARITY_ROLE: {other}"),
    };

    let db = match &data_dir {
        Some(dir) => Database::open(Path::new(dir)).unwrap_or_else(|e| {
            panic!("環境エラー: file モードでデータディレクトリを開けない ({dir}): {e}")
        }),
        None => Database::open_in_memory()
            .unwrap_or_else(|e| panic!("環境エラー: インメモリで開けない: {e}")),
    };

    let mut corpus_names: Vec<String> = Vec::new();
    let mut output_statements: Vec<Value> = Vec::new();
    let mut mismatches: Vec<String> = Vec::new();
    let mut summary: Vec<String> = Vec::new();
    let mut global_index = 0usize;

    for file in &files {
        let name = file
            .file_name()
            .and_then(|n| n.to_str())
            .expect("コーパスファイル名は UTF-8")
            .to_string();
        corpus_names.push(name.clone());

        let text = fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("環境エラー: {} を読めない: {e}", file.display()));
        let statements = split_sql_statements(&text)
            .unwrap_or_else(|e| panic!("環境エラー: {name} の分割失敗: {e}"));

        // 不一致でも途中で止めず、全文を実行し切る(自動コミット単位 = 1 文)。
        let mut executed: Vec<(String, Value)> = Vec::with_capacity(statements.len());
        for sql in &statements {
            global_index += 1;
            let actual = execute_statement(&db, sql);
            output_statements.push(json!({
                "index": global_index,
                "sql": sql,
                "actual": actual.clone(),
            }));
            executed.push((sql.clone(), actual));
        }

        match load_expected_for(file) {
            Ok(expected) => {
                let pass = compare_corpus(&name, &executed, &expected, &mut mismatches);
                summary.push(format!("{name}: {pass}/{} 一致", expected.len()));
            }
            Err(e) => {
                mismatches.push(format!("[{name}] 期待値を読めない: {e}"));
                summary.push(format!("{name}: 期待値なし"));
            }
        }
    }

    // 不一致の有無に関わらず、全文実行が完了した時点で実測の正規化 JSON を
    // 必ず書き出す(以降の flush 等が panic しても実行結果が失われないよう、
    // 出力を flush より前に行う)。
    if let Some(path) = &output_path {
        let doc = json!({
            "corpus": corpus_names.join(","),
            "format": 1,
            "statements": output_statements,
        });
        let serialized = serde_json::to_string_pretty(&doc).expect("JSON 直列化失敗");
        fs::write(path, serialized)
            .unwrap_or_else(|e| panic!("環境エラー: PARITY_OUTPUT へ書けない ({path}): {e}"));
    }

    // file モードの writer は CLI(crates/alopex-cli/src/main.rs)と同じく
    // 実行後に flush して永続化する(reader が別プロセスで開く前提)。
    if role == "writer" && data_dir.is_some() {
        db.flush()
            .unwrap_or_else(|e| panic!("環境エラー: flush 失敗: {e}"));
    }
    drop(db);

    println!("[parity] role={role} corpus={}", corpus_names.join(","));
    for line in &summary {
        println!("[parity] {line}");
    }
    for m in &mismatches {
        println!("[parity] MISMATCH {m}");
    }
    assert!(
        mismatches.is_empty(),
        "parity 不一致 {} 件(詳細は標準出力の [parity] MISMATCH 行)",
        mismatches.len()
    );
}

// ---------------------------------------------------------------------------
// ヘルパーの単体テスト
// ---------------------------------------------------------------------------

#[test]
fn split_sql_statements_handles_quotes_and_comments() {
    let text = "SELECT ';' AS a; -- comment; with semicolon\nSELECT 1;\n/* block; comment */ SELECT 'it''s';\n";
    let stmts = split_sql_statements(text).unwrap();
    assert_eq!(stmts, vec!["SELECT ';' AS a", "SELECT 1", "SELECT 'it''s'"]);
}

#[test]
fn split_sql_statements_block_comment_becomes_single_space() {
    // ブロックコメントが空白なしでトークンに挟まれても結合しない
    let stmts = split_sql_statements("SELECT a/* c */FROM t;").unwrap();
    assert_eq!(stmts, vec!["SELECT a FROM t"]);
    // 文頭・文末のコメントは trim で消える
    let stmts = split_sql_statements("/* head */SELECT 1; SELECT 2/* tail */;").unwrap();
    assert_eq!(stmts, vec!["SELECT 1", "SELECT 2"]);
}

#[test]
fn split_sql_statements_rejects_unclosed_block_comment() {
    assert!(split_sql_statements("SELECT 1 /* oops").is_err());
}

#[test]
fn round_sig9_matches_python_percent9g() {
    assert_eq!(round_sig9(82.5), json!(82.5));
    assert_eq!(round_sig9(294.0), json!(294.0));
    assert_eq!(round_sig9(1.0 / 3.0), json!(0.333333333));
    // 0.1 + 0.2 = 0.30000000000000004 -> 有効数字 9 桁で 0.3
    assert_eq!(round_sig9(0.1 + 0.2), json!(0.3));
    assert_eq!(round_sig9(f64::NAN), json!("NaN"));
}

#[test]
fn json_semantic_eq_tolerates_int_float() {
    assert!(json_semantic_eq(&json!(6), &json!(6.0)));
    assert!(json_semantic_eq(
        &json!({"a": [1, 2.0]}),
        &json!({"a": [1.0, 2]})
    ));
    assert!(!json_semantic_eq(&json!(6), &json!("6")));
    assert!(!json_semantic_eq(&json!({"a": 1}), &json!({"a": 1.5})));
    assert!(!json_semantic_eq(&json!(null), &json!(0)));
}

#[test]
fn classify_error_extracts_unsupported_expression_variant() {
    let (class, object) = classify_error(
        "error[ALOPEX-E999]: unsupported expression: ScalarSubquery { subquery: ... }",
    )
    .unwrap();
    assert_eq!(class, "UNSUPPORTED_EXPRESSION");
    assert_eq!(object, "ScalarSubquery");
    assert!(classify_error("error[ALOPEX-C001]: table users not found").is_none());
}
