//! Async utilities shared across the Alopex workspace.
//!
//! This module provides a conditional `Send` bound that works on both native
//! targets and `wasm32`, plus boxed `Future`/`Stream` type aliases that use it.

use core::future::Future;
use core::pin::Pin;

use futures_core::Stream;

/// A marker trait used to conditionally require `Send`.
///
/// - On native targets, `MaybeSend` implies `Send`.
/// - On `wasm32`, `MaybeSend` has no additional requirements.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

/// `wasm32` variant of [`MaybeSend`].
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}

#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// A boxed future that is `Send` on native targets and not `Send` on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed future that does not require `Send` (for `wasm32`).
#[cfg(target_arch = "wasm32")]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// A boxed stream that is `Send` on native targets and not `Send` on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// A boxed stream that does not require `Send` (for `wasm32`).
#[cfg(target_arch = "wasm32")]
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + 'a>>;
