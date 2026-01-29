//! Runtime-agnostic async utilities.

use core::future::Future;
use core::pin::Pin;
use futures_core::stream::Stream;
#[cfg(feature = "tokio")]
use std::time::Duration;

/// A conditional Send marker trait.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

/// A no-op marker trait on wasm32.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}

#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// A boxed future with conditional Send bounds.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed future without Send bounds on wasm32.
#[cfg(target_arch = "wasm32")]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// A boxed stream with conditional Send bounds.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// A boxed stream without Send bounds on wasm32.
#[cfg(target_arch = "wasm32")]
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + 'a>>;

/// Error returned when a timeout expires.
#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutError;

#[cfg(feature = "tokio")]
impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "timeout elapsed")
    }
}

#[cfg(feature = "tokio")]
impl std::error::Error for TimeoutError {}

/// Runtime-agnostic instant wrapper (tokio-backed).
#[cfg(feature = "tokio")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(InstantInner);

#[cfg(feature = "tokio")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct InstantInner(tokio::time::Instant);

#[cfg(feature = "tokio")]
impl Instant {
    /// Returns the current instant.
    pub fn now() -> Self {
        Self(InstantInner(tokio::time::Instant::now()))
    }

    /// Returns the amount of time elapsed since this instant was created.
    pub fn elapsed(&self) -> Duration {
        self.0 .0.elapsed()
    }

    /// Returns the duration since another instant.
    pub fn duration_since(&self, earlier: Instant) -> Duration {
        self.0 .0.duration_since(earlier.0 .0)
    }

    /// Returns the duration since another instant, or None if earlier is later.
    pub fn checked_duration_since(&self, earlier: Instant) -> Option<Duration> {
        self.0 .0.checked_duration_since(earlier.0 .0)
    }

    /// Adds the specified duration, returning None on overflow.
    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        self.0 .0.checked_add(duration).map(InstantInner).map(Self)
    }

    /// Subtracts the specified duration, returning None on underflow.
    pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
        self.0 .0.checked_sub(duration).map(InstantInner).map(Self)
    }
}

/// Sleeps for the specified duration (tokio-backed).
#[cfg(feature = "tokio")]
pub fn sleep(duration: Duration) -> BoxFuture<'static, ()> {
    Box::pin(tokio::time::sleep(duration))
}

/// Runs a future with a timeout (tokio-backed).
#[cfg(feature = "tokio")]
pub fn timeout<'a, F, T>(duration: Duration, future: F) -> BoxFuture<'a, Result<T, TimeoutError>>
where
    F: Future<Output = T> + MaybeSend + 'a,
{
    Box::pin(async move {
        tokio::time::timeout(duration, future)
            .await
            .map_err(|_| TimeoutError)
    })
}

#[cfg(test)]
mod tests {
    use super::{BoxFuture, BoxStream, MaybeSend};
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use futures_core::stream::Stream;

    fn assert_maybe_send<T: MaybeSend>() {}

    #[test]
    fn maybe_send_marker_compiles() {
        assert_maybe_send::<u64>();
    }

    struct OneShot {
        fired: bool,
    }

    impl Stream for OneShot {
        type Item = u64;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.fired {
                Poll::Ready(None)
            } else {
                self.fired = true;
                Poll::Ready(Some(1))
            }
        }
    }

    #[test]
    fn box_future_and_stream_compile() {
        let _future: BoxFuture<'_, u64> = Box::pin(async { 42 });
        let _stream: BoxStream<'_, u64> = Box::pin(OneShot { fired: false });
    }
}
