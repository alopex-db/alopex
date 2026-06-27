#![cfg(target_os = "linux")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PocSpan {
    start: i64,
    finish: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PocExpr {
    kind: String,
    value: String,
    table: String,
    column: String,
    op: String,
    args: Vec<PocExpr>,
    span: PocSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PocFrom {
    kind: String,
    left_table: String,
    right_table: String,
    join_type: String,
    condition: PocExpr,
    span: PocSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PocAst {
    kind: String,
    distinct: bool,
    select_list: Vec<PocExpr>,
    from_clause: PocFrom,
    where_expr: PocExpr,
    span: PocSpan,
}

fn span(start: i64, finish: i64) -> PocSpan {
    PocSpan { start, finish }
}

fn column(table: &str, name: &str, span: PocSpan) -> PocExpr {
    PocExpr {
        kind: "ColumnRef".to_owned(),
        value: String::new(),
        table: table.to_owned(),
        column: name.to_owned(),
        op: String::new(),
        args: Vec::new(),
        span,
    }
}

fn literal(value: &str, span: PocSpan) -> PocExpr {
    PocExpr {
        kind: "StringLiteral".to_owned(),
        value: value.to_owned(),
        table: String::new(),
        column: String::new(),
        op: String::new(),
        args: Vec::new(),
        span,
    }
}

fn binary(op: &str, left: PocExpr, right: PocExpr, span: PocSpan) -> PocExpr {
    PocExpr {
        kind: "BinaryOp".to_owned(),
        value: String::new(),
        table: String::new(),
        column: String::new(),
        op: op.to_owned(),
        args: vec![left, right],
        span,
    }
}

fn expected_ast() -> PocAst {
    let user_id = column("users", "id", span(7, 15));
    let order_user_id = column("orders", "user_id", span(40, 54));
    let status = column("orders", "status", span(61, 74));

    PocAst {
        kind: "Select".to_owned(),
        distinct: true,
        select_list: vec![user_id.clone(), column("orders", "total", span(17, 29))],
        from_clause: PocFrom {
            kind: "Join".to_owned(),
            left_table: "users".to_owned(),
            right_table: "orders".to_owned(),
            join_type: "Inner".to_owned(),
            condition: binary("Eq", user_id, order_user_id, span(40, 54)),
            span: span(31, 54),
        },
        where_expr: binary("Eq", status, literal("paid", span(77, 83)), span(61, 83)),
        span: span(0, 83),
    }
}

fn run_nim_poc(out_dir: &Path) {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("alopex-sql crate is under crates/");
    let container_out_dir = "/workspace/target/nim-sql-parser-poc";
    fs::create_dir_all(out_dir).expect("create PoC artifact directory");

    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &format!("{}:/workspace", repo_root.display()),
            "-w",
            "/workspace/crates/alopex-sql/nim-sql-parser",
            "-e",
            &format!("ALOPEX_POC_OUT_DIR={container_out_dir}"),
            "-e",
            "PATH=/opt/nim/bin:/usr/local/bin:/usr/bin:/bin",
            "nimlang/nim:2.2",
            "sh",
            "-c",
            &format!(
                "nimble install -y msgpack4nim && nim c -r --hints:off --verbosity:0 -o:{container_out_dir}/poc_msgpack_bin tests/poc_msgpack.nim"
            ),
        ])
        .current_dir(repo_root)
        .output()
        .expect("run nimlang/nim:2.2 Docker container for MessagePack PoC");

    assert!(
        output.status.success(),
        "Nim MessagePack PoC failed in nimlang/nim:2.2\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
}

#[test]
fn msgpack_from_nim_roundtrips_into_rust_struct() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("alopex-sql crate is under crates/");
    let out_dir = repo_root.join("target/nim-sql-parser-poc");
    run_nim_poc(&out_dir);

    let msgpack_bytes =
        fs::read(out_dir.join("poc_ast.msgpack")).expect("read Nim MessagePack fixture");
    let ast: PocAst =
        rmp_serde::from_slice(&msgpack_bytes).expect("decode Nim MessagePack with rmp-serde");
    assert_eq!(ast, expected_ast());

    let rust_msgpack = rmp_serde::to_vec_named(&ast).expect("encode Rust AST with rmp-serde");
    let roundtripped: PocAst =
        rmp_serde::from_slice(&rust_msgpack).expect("decode Rust MessagePack with rmp-serde");
    assert_eq!(roundtripped, ast);

    let json_bytes = fs::read(out_dir.join("poc_ast.json")).expect("read Nim JSON control fixture");
    let json_ast: PocAst =
        serde_json::from_slice(&json_bytes).expect("decode Nim JSON control fixture");
    assert_eq!(json_ast, ast);

    let bench =
        fs::read_to_string(out_dir.join("poc_bench.csv")).expect("read PoC benchmark report");
    eprintln!("{bench}");
}
