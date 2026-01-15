use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alopex_cli::client::http::HttpClient;
use alopex_cli::models::{Column, DataType, Row, Value};
use alopex_cli::output::json::JsonFormatter;
use alopex_cli::output::table::TableFormatter;
use alopex_cli::profile::config::ServerConfig as CliServerConfig;
use alopex_cli::streaming::{StreamingWriter, WriteStatus, DEFAULT_BUFFER_LIMIT};
use axum::body::{boxed, Body, Bytes};
use axum::extract::State;
use axum::routing::get;
use axum::Router;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures_util::stream;
use rcgen::generate_simple_self_signed;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;

fn sample_columns() -> Vec<Column> {
    vec![
        Column::new("id", DataType::Int),
        Column::new("name", DataType::Text),
    ]
}

fn sample_row(id: i64) -> Row {
    Row::new(vec![Value::Int(id), Value::Text(format!("name-{id}"))])
}

fn streaming_throughput(c: &mut Criterion) {
    let columns = sample_columns();
    c.bench_function("streaming_json_1m_rows", |b| {
        b.iter_batched(
            || {
                let formatter = Box::new(JsonFormatter::new());
                let output: Vec<u8> = Vec::new();
                (formatter, output)
            },
            |(formatter, mut output)| {
                let mut writer =
                    StreamingWriter::new(&mut output, formatter, columns.clone(), None);
                writer.prepare(None).unwrap();
                for i in 0..1_000_000i64 {
                    match writer.write_row(sample_row(i)).unwrap() {
                        WriteStatus::Continue => {}
                        WriteStatus::LimitReached => break,
                    }
                }
                writer.finish().unwrap();
                output.len()
            },
            BatchSize::SmallInput,
        )
    });
}

fn buffer_limit_memory(c: &mut Criterion) {
    let columns = sample_columns();
    c.bench_function("table_buffer_limit_10mb", |b| {
        b.iter_batched(
            || {
                let formatter = Box::new(TableFormatter::new());
                let output: Vec<u8> = Vec::new();
                (formatter, output)
            },
            |(formatter, mut output)| {
                let mut writer =
                    StreamingWriter::new(&mut output, formatter, columns.clone(), None)
                        .with_buffer_limit(DEFAULT_BUFFER_LIMIT);
                writer.prepare(None).unwrap();
                loop {
                    let row = sample_row(writer.written_count() as i64);
                    if writer.write_row(row).is_err() {
                        break;
                    }
                }
                writer.buffered_bytes()
            },
            BatchSize::SmallInput,
        )
    });
}

fn tui_responsiveness(c: &mut Criterion) {
    c.bench_function("tui_key_handling", |b| {
        b.iter(|| {
            let mut table_state = alopex_cli::tui::TuiApp::new(
                sample_columns(),
                vec![sample_row(1), sample_row(2)],
                "local",
                false,
            );
            for _ in 0..10_000 {
                table_state
                    .handle_key(crossterm::event::KeyEvent::new(
                        crossterm::event::KeyCode::Char('j'),
                        crossterm::event::KeyModifiers::NONE,
                    ))
                    .unwrap();
            }
        })
    });
}

async fn spawn_tls_server(router: Router) -> (String, oneshot::Sender<()>) {
    let cert = generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert");
    let dir = tempfile::tempdir().expect("tempdir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.serialize_pem().expect("cert pem")).expect("write cert");
    std::fs::write(&key_path, cert.serialize_private_key_pem()).expect("write key");

    let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .expect("rustls config");

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();
    tokio::spawn(async move {
        let _ = shutdown_rx.await;
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(5)));
    });

    let server = axum_server::bind_rustls(addr, rustls_config)
        .handle(handle)
        .serve(router.into_make_service());
    tokio::spawn(server);

    (format!("https://{}", addr), shutdown_tx)
}

fn connection_overhead(c: &mut Criterion) {
    let runtime = Runtime::new().expect("runtime");
    c.bench_function("tls_handshake_overhead", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let payload = Arc::new(vec![Bytes::from_static(b"ok")]);
                let router = Router::new()
                    .route(
                        "/api/admin/health",
                        get(|State(payload): State<Arc<Vec<Bytes>>>| async move {
                            let rows = payload.as_ref().clone();
                            let stream =
                                stream::iter(rows.into_iter().map(Ok::<Bytes, Infallible>));
                            let body = boxed(Body::wrap_stream(stream));
                            let mut response = axum::response::Response::new(body);
                            *response.status_mut() = axum::http::StatusCode::OK;
                            response
                        }),
                    )
                    .with_state(payload.clone());

                let (base_url, shutdown) = spawn_tls_server(router).await;
                let config = CliServerConfig {
                    url: base_url,
                    auth: None,
                    token: None,
                    username: None,
                    password_command: None,
                    cert_path: None,
                    key_path: None,
                };
                let client = reqwest::ClientBuilder::new()
                    .danger_accept_invalid_certs(true)
                    .use_rustls_tls()
                    .build()
                    .expect("reqwest client");
                let http = HttpClient::new_with_client(&config, client).expect("http client");

                let start = Instant::now();
                let _: String = http.get_text("api/admin/health").await.unwrap();
                let elapsed = start.elapsed();

                let _ = shutdown.send(());
                elapsed
            })
        })
    });
}

criterion_group!(
    benches,
    streaming_throughput,
    buffer_limit_memory,
    tui_responsiveness,
    connection_overhead
);
criterion_main!(benches);
