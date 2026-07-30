use std::sync::Arc;

use alopex_server::auth::AuthMode;
use alopex_server::config::ServerConfig;
use alopex_server::grpc;
use alopex_server::server::ServerState;
use alopex_server::Server;
use prost::Message;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tonic::transport::Channel;
use tonic::{Code, Request};

const API_KEY: &str = "f2-grpc-key";

async fn build_state() -> (Arc<ServerState>, tempfile::TempDir) {
    let temp = tempdir().expect("tempdir");
    let server = Server::new(ServerConfig {
        data_dir: temp.path().to_path_buf(),
        auth_mode: AuthMode::Dev {
            api_key: API_KEY.to_owned(),
        },
        audit_log_enabled: false,
        ..ServerConfig::default()
    })
    .expect("server");
    (server.state, temp)
}

async fn spawn_network_grpc_server(
    state: Arc<ServerState>,
) -> (Channel, broadcast::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve grpc port");
    let addr = listener.local_addr().expect("grpc address");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handle = tokio::spawn(async move {
        grpc::serve(state, addr, shutdown_rx)
            .await
            .expect("serve grpc transport");
    });

    let endpoint = format!("http://{addr}");
    for _ in 0..40 {
        if let Ok(channel) = Channel::from_shared(endpoint.clone())
            .expect("grpc endpoint")
            .connect()
            .await
        {
            return (channel, shutdown_tx, handle);
        }
        sleep(Duration::from_millis(25)).await;
    }
    let _ = shutdown_tx.send(());
    let _ = handle.await;
    panic!("gRPC transport did not become ready at {endpoint}");
}

fn range() -> grpc::proto::CrdtRangeIdentity {
    grpc::proto::CrdtRangeIdentity {
        cluster_id: "cluster-f2-grpc".into(),
        table_id: 7,
        range_id: "range-f2-grpc".into(),
        lower_bound: Vec::new(),
        has_lower_bound: false,
        upper_bound: Vec::new(),
        has_upper_bound: false,
        schema_version: 1,
        data_epoch: 9,
    }
}

fn authenticated<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("x-api-key", API_KEY.parse().expect("metadata value"));
    request
}

fn create_counter() -> grpc::proto::CreateCounterRequest {
    grpc::proto::CreateCounterRequest {
        object_id: "f2-grpc-counter".into(),
        range: Some(range()),
        request_id: "f2-grpc-counter-create-request".into(),
        operation_id: "f2-grpc-counter-create-operation".into(),
        update_version: 0,
        initial_value: -4,
    }
}

fn read_counter() -> grpc::proto::ReadCounterRequest {
    grpc::proto::ReadCounterRequest {
        object_id: "f2-grpc-counter".into(),
        range: Some(range()),
        request_id: "f2-grpc-counter-read-request".into(),
        operation_id: "f2-grpc-counter-read-operation".into(),
        update_version: 0,
    }
}

fn increment_counter() -> grpc::proto::IncrementCounterRequest {
    grpc::proto::IncrementCounterRequest {
        object_id: "f2-grpc-counter".into(),
        range: Some(range()),
        request_id: "f2-grpc-counter-increment-request".into(),
        operation_id: "f2-grpc-counter-increment-operation".into(),
        update_version: 1,
        delta: 3,
    }
}

fn decrement_counter() -> grpc::proto::DecrementCounterRequest {
    grpc::proto::DecrementCounterRequest {
        object_id: "f2-grpc-counter".into(),
        range: Some(range()),
        request_id: "f2-grpc-counter-decrement-request".into(),
        operation_id: "f2-grpc-counter-decrement-operation".into(),
        update_version: 2,
        delta: 3,
    }
}

fn create_set() -> grpc::proto::CreateSetRequest {
    grpc::proto::CreateSetRequest {
        object_id: "f2-grpc-set".into(),
        range: Some(range()),
        request_id: "f2-grpc-set-create-request".into(),
        operation_id: "f2-grpc-set-create-operation".into(),
        update_version: 0,
    }
}

fn read_set() -> grpc::proto::ReadSetRequest {
    grpc::proto::ReadSetRequest {
        object_id: "f2-grpc-set".into(),
        range: Some(range()),
        request_id: "f2-grpc-set-read-request".into(),
        operation_id: "f2-grpc-set-read-operation".into(),
        update_version: 0,
    }
}

fn add_set() -> grpc::proto::AddSetRequest {
    grpc::proto::AddSetRequest {
        object_id: "f2-grpc-set".into(),
        range: Some(range()),
        request_id: "f2-grpc-set-add-request".into(),
        operation_id: "00000000-0000-0000-0000-000000000903".into(),
        update_version: 1,
        member: "alice".into(),
    }
}

fn remove_set() -> grpc::proto::RemoveSetRequest {
    grpc::proto::RemoveSetRequest {
        object_id: "f2-grpc-set".into(),
        range: Some(range()),
        request_id: "f2-grpc-set-remove-request".into(),
        operation_id: "00000000-0000-0000-0000-000000000904".into(),
        update_version: 2,
        member: "alice".into(),
    }
}

fn contains_set() -> grpc::proto::ContainsSetRequest {
    grpc::proto::ContainsSetRequest {
        object_id: "f2-grpc-set".into(),
        range: Some(range()),
        request_id: "f2-grpc-set-contains-request".into(),
        operation_id: "f2-grpc-set-contains-operation".into(),
        update_version: 0,
        member: "alice".into(),
    }
}

fn list_set() -> grpc::proto::ListSetRequest {
    grpc::proto::ListSetRequest {
        object_id: "f2-grpc-set".into(),
        range: Some(range()),
        request_id: "f2-grpc-set-list-request".into(),
        operation_id: "f2-grpc-set-list-operation".into(),
        update_version: 0,
    }
}

macro_rules! assert_unauthenticated {
    ($client:expr, $method:ident, $message:expr, $label:literal) => {{
        match $client.$method(Request::new($message)).await {
            Err(error) => assert_eq!(error.code(), Code::Unauthenticated, $label),
            Ok(_) => panic!("{} accepted a request without an API key", $label),
        }
    }};
}

macro_rules! assert_unknown_fields_ignored {
    ($($request:ty),+ $(,)?) => {
        $(
            let decoded = <$request>::decode(&[0xf8, 0x07, 0x01][..])
                .expect("unknown protobuf field must be ignored");
            assert_eq!(decoded, <$request>::default(), stringify!($request));
        )+
    };
}

macro_rules! assert_counter_outcome {
    ($outcome:expr, $label:literal) => {{
        let outcome = &$outcome;
        assert_eq!(outcome.object_type, "counter", "{} object type", $label);
        assert_eq!(outcome.actor, "dev", "{} actor", $label);
        assert_eq!(outcome.state, "committed", "{} state", $label);
        assert_eq!(outcome.routing_kind, "local_only", "{} routing", $label);
        assert!(outcome.has_value, "{} has value", $label);
    }};
}

macro_rules! assert_set_outcome {
    ($outcome:expr, $label:literal) => {{
        let outcome = &$outcome;
        assert_eq!(outcome.object_type, "set", "{} object type", $label);
        assert_eq!(outcome.actor, "dev", "{} actor", $label);
        assert_eq!(outcome.state, "committed", "{} state", $label);
        assert_eq!(outcome.routing_kind, "local_only", "{} routing", $label);
        assert!(outcome.has_value, "{} has value", $label);
    }};
}

#[tokio::test]
async fn f2_grpc_register_covers_all_crdt_rpcs_status_and_unknown_fields() {
    let (state, _temp) = build_state().await;
    let (channel, shutdown, handle) = spawn_network_grpc_server(state).await;
    let mut client = grpc::proto::alopex_service_client::AlopexServiceClient::new(channel);

    assert_unauthenticated!(client, create_counter, create_counter(), "CreateCounter");
    assert_unauthenticated!(client, read_counter, read_counter(), "ReadCounter");
    assert_unauthenticated!(
        client,
        increment_counter,
        increment_counter(),
        "IncrementCounter"
    );
    assert_unauthenticated!(
        client,
        decrement_counter,
        decrement_counter(),
        "DecrementCounter"
    );
    assert_unauthenticated!(client, create_set, create_set(), "CreateSet");
    assert_unauthenticated!(client, read_set, read_set(), "ReadSet");
    assert_unauthenticated!(client, add_set, add_set(), "AddSet");
    assert_unauthenticated!(client, remove_set, remove_set(), "RemoveSet");
    assert_unauthenticated!(client, contains_set, contains_set(), "ContainsSet");
    assert_unauthenticated!(client, list_set, list_set(), "ListSet");

    assert_unknown_fields_ignored!(
        grpc::proto::CreateCounterRequest,
        grpc::proto::ReadCounterRequest,
        grpc::proto::IncrementCounterRequest,
        grpc::proto::DecrementCounterRequest,
        grpc::proto::CreateSetRequest,
        grpc::proto::ReadSetRequest,
        grpc::proto::AddSetRequest,
        grpc::proto::RemoveSetRequest,
        grpc::proto::ContainsSetRequest,
        grpc::proto::ListSetRequest,
    );

    let counter_create = client
        .create_counter(authenticated(create_counter()))
        .await
        .expect("Counter create")
        .into_inner();
    assert_counter_outcome!(counter_create, "Counter create");
    assert_eq!(counter_create.value, -4);
    assert_eq!(counter_create.duplicate_count, 0);
    let counter_create_replay = client
        .create_counter(authenticated(create_counter()))
        .await
        .expect("Counter create replay")
        .into_inner();
    assert_eq!(counter_create_replay.duplicate_count, 1);
    assert_eq!(counter_create_replay.value, counter_create.value);

    let counter_read = client
        .read_counter(authenticated(read_counter()))
        .await
        .expect("Counter read")
        .into_inner();
    assert_counter_outcome!(counter_read, "Counter read");
    assert_eq!(counter_read.value, -4);
    assert_eq!(counter_read.duplicate_count, 0);
    let counter_read_repeat = client
        .read_counter(authenticated(read_counter()))
        .await
        .expect("Counter read repeat")
        .into_inner();
    assert_eq!(counter_read_repeat.duplicate_count, 0);
    assert_eq!(counter_read_repeat.value, counter_read.value);

    let counter_increment = client
        .increment_counter(authenticated(increment_counter()))
        .await
        .expect("Counter increment")
        .into_inner();
    assert_counter_outcome!(counter_increment, "Counter increment");
    assert_eq!(counter_increment.value, -1);
    assert_eq!(counter_increment.duplicate_count, 0);
    let counter_increment_replay = client
        .increment_counter(authenticated(increment_counter()))
        .await
        .expect("Counter increment replay")
        .into_inner();
    assert_eq!(counter_increment_replay.duplicate_count, 1);
    assert_eq!(counter_increment_replay.value, counter_increment.value);

    let counter_decrement = client
        .decrement_counter(authenticated(decrement_counter()))
        .await
        .expect("Counter decrement")
        .into_inner();
    assert_counter_outcome!(counter_decrement, "Counter decrement");
    assert_eq!(counter_decrement.value, -4);
    assert_eq!(counter_decrement.duplicate_count, 0);
    let counter_decrement_replay = client
        .decrement_counter(authenticated(decrement_counter()))
        .await
        .expect("Counter decrement replay")
        .into_inner();
    assert_eq!(counter_decrement_replay.duplicate_count, 1);
    assert_eq!(counter_decrement_replay.value, counter_decrement.value);

    let set_create = client
        .create_set(authenticated(create_set()))
        .await
        .expect("Set create")
        .into_inner();
    assert_set_outcome!(set_create, "Set create");
    assert!(set_create.members.is_empty());
    assert_eq!(set_create.duplicate_count, 0);
    let set_create_replay = client
        .create_set(authenticated(create_set()))
        .await
        .expect("Set create replay")
        .into_inner();
    assert_eq!(set_create_replay.duplicate_count, 1);
    assert_eq!(set_create_replay.members, set_create.members);

    let set_read = client
        .read_set(authenticated(read_set()))
        .await
        .expect("Set read")
        .into_inner();
    assert_set_outcome!(set_read, "Set read");
    assert!(set_read.members.is_empty());
    assert_eq!(set_read.duplicate_count, 0);
    let set_read_repeat = client
        .read_set(authenticated(read_set()))
        .await
        .expect("Set read repeat")
        .into_inner();
    assert_eq!(set_read_repeat.duplicate_count, 0);
    assert_eq!(set_read_repeat.members, set_read.members);

    let set_add = client
        .add_set(authenticated(add_set()))
        .await
        .expect("Set add")
        .into_inner();
    assert_set_outcome!(set_add, "Set add");
    assert_eq!(set_add.members, ["alice"]);
    assert_eq!(set_add.duplicate_count, 0);
    let set_add_replay = client
        .add_set(authenticated(add_set()))
        .await
        .expect("Set add replay")
        .into_inner();
    assert_eq!(set_add_replay.duplicate_count, 1);
    assert_eq!(set_add_replay.members, set_add.members);

    let set_contains = client
        .contains_set(authenticated(contains_set()))
        .await
        .expect("Set contains")
        .into_inner();
    assert_set_outcome!(set_contains, "Set contains");
    assert_eq!(set_contains.members, ["alice"]);
    assert_eq!(set_contains.duplicate_count, 0);
    let set_contains_repeat = client
        .contains_set(authenticated(contains_set()))
        .await
        .expect("Set contains repeat")
        .into_inner();
    assert_eq!(set_contains_repeat.duplicate_count, 0);
    assert_eq!(set_contains_repeat.members, set_contains.members);

    let set_list = client
        .list_set(authenticated(list_set()))
        .await
        .expect("Set list")
        .into_inner();
    assert_set_outcome!(set_list, "Set list");
    assert_eq!(set_list.members, ["alice"]);
    assert_eq!(set_list.duplicate_count, 0);
    let set_list_repeat = client
        .list_set(authenticated(list_set()))
        .await
        .expect("Set list repeat")
        .into_inner();
    assert_eq!(set_list_repeat.duplicate_count, 0);
    assert_eq!(set_list_repeat.members, set_list.members);

    let set_remove = client
        .remove_set(authenticated(remove_set()))
        .await
        .expect("Set remove")
        .into_inner();
    assert_set_outcome!(set_remove, "Set remove");
    assert!(set_remove.members.is_empty());
    assert_eq!(set_remove.duplicate_count, 0);
    let set_remove_replay = client
        .remove_set(authenticated(remove_set()))
        .await
        .expect("Set remove replay")
        .into_inner();
    assert_eq!(set_remove_replay.duplicate_count, 1);
    assert_eq!(set_remove_replay.members, set_remove.members);

    let _ = shutdown.send(());
    handle.await.expect("gRPC server shutdown");
}
