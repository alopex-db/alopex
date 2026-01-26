use std::collections::HashSet;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use hyper::{Body, Client, Method, Request, StatusCode};
use tempfile::tempdir;
use tokio::time::sleep;

fn reserve_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind port");
    listener.local_addr().expect("local addr").port()
}

fn reserve_unique_port(used: &mut HashSet<u16>) -> u16 {
    loop {
        let port = reserve_port();
        if used.insert(port) {
            return port;
        }
    }
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "\\\\")
}

fn write_config(dir: &Path, http_port: u16, admin_port: u16, grpc_port: u16) -> PathBuf {
    let config_path = dir.join("alopex.toml");
    let contents = format!(
        "\
http_bind = \"127.0.0.1:{http_port}\"
grpc_bind = \"127.0.0.1:{grpc_port}\"
admin_bind = \"127.0.0.1:{admin_port}\"
data_dir = \"{data_dir}\"
metrics_enabled = false
tracing_enabled = false
audit_log_enabled = false
",
        data_dir = toml_path(dir),
    );
    std::fs::write(&config_path, contents).expect("write config");
    config_path
}

async fn get_status(
    client: &Client<hyper::client::HttpConnector>,
    url: &str,
) -> Option<StatusCode> {
    let request = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Body::empty())
        .ok()?;
    let response = client.request(request).await.ok()?;
    Some(response.status())
}

#[tokio::test]
async fn server_binary_starts_and_serves_health() {
    let temp = tempdir().expect("tempdir");
    let mut used = HashSet::new();
    let http_port = reserve_unique_port(&mut used);
    let admin_port = reserve_unique_port(&mut used);
    let grpc_port = reserve_unique_port(&mut used);
    let config_path = write_config(temp.path(), http_port, admin_port, grpc_port);

    let mut child = Command::new(env!("CARGO_BIN_EXE_alopex-server"))
        .arg("--config")
        .arg(&config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn server");

    let client = Client::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut admin_ok = false;
    let mut api_ok = false;

    while Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            let mut stderr_output = String::new();
            if let Some(mut stderr) = child.stderr.take() {
                let _ = stderr.read_to_string(&mut stderr_output);
            }
            panic!("alopex-server exited early ({status}). stderr:\n{stderr_output}");
        }

        if !admin_ok {
            if let Some(status) =
                get_status(&client, &format!("http://127.0.0.1:{admin_port}/healthz")).await
            {
                admin_ok = status == StatusCode::OK;
            }
        }
        if !api_ok {
            if let Some(status) = get_status(
                &client,
                &format!("http://127.0.0.1:{http_port}/api/admin/health"),
            )
            .await
            {
                api_ok = status == StatusCode::OK;
            }
        }
        if admin_ok && api_ok {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    if !(admin_ok && api_ok) {
        let mut stderr_output = String::new();
        if let Some(mut stderr) = child.stderr.take() {
            let _ = stderr.read_to_string(&mut stderr_output);
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!(
            "alopex-server failed health checks (admin_ok={admin_ok}, api_ok={api_ok}). stderr:\n{stderr_output}"
        );
    }

    let _ = child.kill();
    let _ = child.wait();
}
