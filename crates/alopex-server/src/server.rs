use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use alopex_core::kv::any::AnyKV;
use alopex_core::kv::async_adapter::{AsyncKVStoreAdapter, AsyncKVTransactionAdapter};
use alopex_core::kv::AsyncKVStore;
use alopex_core::types::TxnMode;
use alopex_sql::catalog::{Catalog, CatalogError, PersistentCatalog};
use alopex_sql::storage::async_storage::AsyncTxnBridge;
use alopex_sql::storage::erased::ErasedAsyncSqlTransaction;
use tokio::sync::{broadcast, Semaphore};
use tracing::info;

use crate::audit::AuditLogger;
use crate::auth::AuthMiddleware;
use crate::config::ServerConfig;
use crate::error::{Result, ServerError};
use crate::metrics::Metrics;
use crate::ops::backup::BackupCoordinator;
use crate::ops::memory::MemoryControlPolicy;
use crate::ops::recovery::{RecoveryCoordinator, RecoveryInfo};
use crate::ops::restore::RestoreCoordinator;
use crate::ops::state::{LifecycleStateManager, Mode};
use crate::session::{SessionConfig, SessionManager, TransactionFactory};
use crate::tls;

pub struct Server {
    pub state: Arc<ServerState>,
}

pub struct ServerState {
    pub config: ServerConfig,
    pub store: Arc<AnyKV>,
    pub catalog: Arc<RwLock<dyn Catalog + Send + Sync>>,
    pub async_store: Arc<AsyncKVStoreAdapter<AnyKV>>,
    pub session_manager: Arc<SessionManager>,
    pub metrics: Metrics,
    pub audit: AuditLogger,
    pub auth: AuthMiddleware,
    pub start_time: Instant,
    pub lifecycle_state: Arc<LifecycleStateManager>,
    pub recovery_info: RecoveryInfo,
    pub backup_coordinator: BackupCoordinator,
    pub restore_coordinator: RestoreCoordinator,
    pub admission_permits: Arc<Semaphore>,
    pub admission_waiters: AtomicUsize,
}

impl Server {
    pub fn new(config: ServerConfig) -> Result<Self> {
        config.validate()?;
        let (store, recovery_info) = RecoveryCoordinator::open_store(&config.data_dir)?;
        let lifecycle_state = Arc::new(LifecycleStateManager::new(Mode::Normal));
        RecoveryCoordinator::apply_initial_mode(&lifecycle_state, &recovery_info);

        let store = Arc::new(store);
        let catalog = load_catalog(store.clone())?;
        let async_store = Arc::new(AsyncKVStoreAdapter::from_arc(
            store.clone(),
            TxnMode::ReadWrite,
        ));
        let metrics = Metrics::new()?;
        let audit = AuditLogger::new(config.audit_log_output.clone())?;
        let auth = AuthMiddleware::new(config.auth_mode.clone());

        let txn_factory = build_txn_factory(async_store.clone(), catalog.clone(), metrics.clone());
        let session_manager = Arc::new(SessionManager::new(
            SessionConfig {
                ttl: config.session_ttl,
            },
            txn_factory,
        ));
        let data_dir = config.data_dir.clone();
        let checkpoint = {
            let store = store.clone();
            Arc::new(move || match store.as_ref() {
                AnyKV::Lsm(kv) => {
                    kv.checkpoint()?;
                    Ok(())
                }
                _ => Err(ServerError::BadRequest(
                    "checkpoint unsupported for current storage engine".to_string(),
                )),
            })
        };
        let backup_coordinator =
            BackupCoordinator::new(data_dir.clone(), lifecycle_state.clone(), checkpoint);
        let restore_coordinator = RestoreCoordinator::new(data_dir, lifecycle_state.clone());
        let admission_permits = Arc::new(Semaphore::new(config.max_concurrency));

        Ok(Self {
            state: Arc::new(ServerState {
                config,
                store,
                catalog,
                async_store,
                session_manager,
                metrics,
                audit,
                auth,
                start_time: Instant::now(),
                lifecycle_state,
                recovery_info,
                backup_coordinator,
                restore_coordinator,
                admission_permits,
                admission_waiters: AtomicUsize::new(0),
            }),
        })
    }

    pub async fn run(self) -> Result<()> {
        if self.state.config.tracing_enabled {
            init_tracing();
        }
        info!(
            max_concurrency = self.state.config.max_concurrency,
            max_queue_len = self.state.config.max_queue_len,
            query_timeout_ms = self.state.config.query_timeout.as_millis(),
            "Admission control configuration applied"
        );

        let (shutdown_tx, _) = broadcast::channel(2);
        let http_state = self.state.clone();
        let admin_state = self.state.clone();
        let grpc_state = self.state.clone();
        let cleanup_state = self.state.clone();
        let http_shutdown = shutdown_tx.subscribe();
        let admin_shutdown = shutdown_tx.subscribe();
        let grpc_shutdown = shutdown_tx.subscribe();
        let cleanup_shutdown = shutdown_tx.subscribe();

        let tls_config = if let Some(tls) = &self.state.config.tls {
            let config = tls::build_rustls_config(tls)?;
            Some(axum_server::tls_rustls::RustlsConfig::from_config(config))
        } else {
            None
        };

        let http_task = tokio::spawn(run_http(http_state, tls_config.clone(), http_shutdown));
        let admin_task = tokio::spawn(run_admin(admin_state, tls_config, admin_shutdown));
        let grpc_task = tokio::spawn(run_grpc(grpc_state, grpc_shutdown));
        let cleanup_task = tokio::spawn(run_cleanup(cleanup_state, cleanup_shutdown));

        wait_for_shutdown(shutdown_tx.clone()).await;
        let _ = shutdown_tx.send(());

        http_task
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))??;
        admin_task
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))??;
        grpc_task
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))??;
        cleanup_task
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))??;

        self.state.audit.flush()?;
        Ok(())
    }
}

impl ServerState {
    pub async fn begin_sql_txn(
        &self,
    ) -> Result<AsyncTxnBridge<'static, AsyncKVTransactionAdapter>> {
        let txn = self.async_store.begin_async().await?;
        let mut bridge =
            AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, self.catalog.clone());
        let policy = MemoryControlPolicy::from_env_with_metrics(self.metrics.clone()).sql_policy();
        bridge.set_memory_policy(policy);
        Ok(bridge)
    }
}

fn build_txn_factory(
    store: Arc<AsyncKVStoreAdapter<AnyKV>>,
    catalog: Arc<RwLock<dyn Catalog + Send + Sync>>,
    metrics: Metrics,
) -> TransactionFactory {
    Arc::new(move || {
        let store = store.clone();
        let catalog = catalog.clone();
        let metrics = metrics.clone();
        Box::pin(async move {
            let txn = store.begin_async().await?;
            let mut bridge: AsyncTxnBridge<'static, AsyncKVTransactionAdapter> =
                AsyncTxnBridge::with_catalog(txn, TxnMode::ReadWrite, catalog);
            let policy = MemoryControlPolicy::from_env_with_metrics(metrics).sql_policy();
            bridge.set_memory_policy(policy);
            Ok(Box::new(bridge) as Box<dyn ErasedAsyncSqlTransaction>)
        })
    })
}

fn load_catalog(store: Arc<AnyKV>) -> Result<Arc<RwLock<dyn Catalog + Send + Sync>>> {
    let catalog = match PersistentCatalog::load(store.clone()) {
        Ok(catalog) => catalog,
        Err(CatalogError::Kv(alopex_core::Error::NotFound)) => PersistentCatalog::new(store),
        Err(err) => return Err(ServerError::Catalog(err)),
    };
    let catalog: Arc<RwLock<dyn Catalog + Send + Sync>> = Arc::new(RwLock::new(catalog));
    Ok(catalog)
}

async fn run_http(
    state: Arc<ServerState>,
    tls_config: Option<axum_server::tls_rustls::RustlsConfig>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let app = crate::http::router(state.clone());
    let addr = state.config.http_bind;

    if let Some(tls) = tls_config {
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            let _ = shutdown.recv().await;
            shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
        });
        axum_server::bind_rustls(addr, tls)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))?;
    } else {
        let shutdown_signal = async move {
            let _ = shutdown.recv().await;
        };
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown_signal)
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))?;
    }
    Ok(())
}

async fn run_admin(
    state: Arc<ServerState>,
    tls_config: Option<axum_server::tls_rustls::RustlsConfig>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let app = crate::http::admin_router(state.clone());
    let addr = state.config.admin_bind;

    if let Some(tls) = tls_config {
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            let _ = shutdown.recv().await;
            shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
        });
        axum_server::bind_rustls(addr, tls)
            .handle(handle)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))?;
    } else {
        let shutdown_signal = async move {
            let _ = shutdown.recv().await;
        };
        axum::Server::bind(&addr)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(shutdown_signal)
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))?;
    }
    Ok(())
}

async fn run_grpc(state: Arc<ServerState>, shutdown: broadcast::Receiver<()>) -> Result<()> {
    let addr = state.config.grpc_bind;
    crate::grpc::serve(state, addr, shutdown).await
}

async fn run_cleanup(state: Arc<ServerState>, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
    let mut interval = tokio::time::interval(state.config.session_ttl);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                state.session_manager.cleanup_expired();
            }
            _ = shutdown.recv() => break,
        }
    }
    Ok(())
}

async fn wait_for_shutdown(signal: broadcast::Sender<()>) {
    #[cfg(unix)]
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            let _ = signal.send(());
            return;
        }
    };

    #[cfg(unix)]
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    let _ = signal.send(());
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}
