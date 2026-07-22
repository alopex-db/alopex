//! Authenticated cluster-frame dispatch.
//!
//! Raw frames must pass a peer authenticator before they can be represented as
//! [`VerifiedClusterFrame`]. Handlers receive only that verified type, making
//! unauthenticated delivery impossible through this dispatcher API.

use crate::{ClusterId, NodeId};
use std::{collections::BTreeMap, error::Error, fmt};

/// Logical destination for a cluster frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClusterFrameKind {
    GossipEvidence,
    Raft,
    /// Authenticated snapshot-plus-journal replica transfer data plane.
    RangeTransfer,
    Application,
}

/// Untrusted input received from a transport before identity verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundClusterFrame {
    pub claimed_node_id: NodeId,
    pub claimed_cluster_id: ClusterId,
    pub kind: ClusterFrameKind,
    pub payload: Vec<u8>,
}

/// Peer identity that an authenticator has bound to a transport session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPeerIdentity {
    node_id: NodeId,
    cluster_id: ClusterId,
}

impl VerifiedPeerIdentity {
    pub fn new(node_id: impl Into<NodeId>, cluster_id: impl Into<ClusterId>) -> Self {
        Self {
            node_id: node_id.into(),
            cluster_id: cluster_id.into(),
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }
}

/// A frame whose peer identity has been verified by the configured trust
/// boundary. It has no public constructor by design.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedClusterFrame {
    peer: VerifiedPeerIdentity,
    kind: ClusterFrameKind,
    payload: Vec<u8>,
}

impl VerifiedClusterFrame {
    fn from_authenticated(peer: VerifiedPeerIdentity, inbound: InboundClusterFrame) -> Self {
        Self {
            peer,
            kind: inbound.kind,
            payload: inbound.payload,
        }
    }

    pub fn peer(&self) -> &VerifiedPeerIdentity {
        &self.peer
    }

    pub fn kind(&self) -> ClusterFrameKind {
        self.kind
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Authenticates an inbound transport frame and binds it to an allowed peer.
pub trait ClusterPeerAuthenticator {
    fn authenticate(
        &self,
        inbound: &InboundClusterFrame,
    ) -> Result<VerifiedPeerIdentity, PeerAuthenticationError>;
}

/// A classified rejection at the peer-authentication boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerAuthenticationError {
    UntrustedPeer,
    NodeIdMismatch,
    ClusterIdMismatch,
}

impl fmt::Display for PeerAuthenticationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UntrustedPeer => {
                f.write_str("peer is not trusted by the configured cluster authenticator")
            }
            Self::NodeIdMismatch => {
                f.write_str("authenticated peer identity does not match claimed node_id")
            }
            Self::ClusterIdMismatch => {
                f.write_str("authenticated peer identity does not match claimed cluster_id")
            }
        }
    }
}

impl Error for PeerAuthenticationError {}

/// Receives one verified frame for one logical destination.
pub trait ClusterFrameHandler: Send {
    fn handle(&mut self, frame: VerifiedClusterFrame) -> Result<(), ClusterFrameHandlerError>;
}

/// A handler's classified processing failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterFrameHandlerError {
    message: String,
}

impl ClusterFrameHandlerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ClusterFrameHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for ClusterFrameHandlerError {}

/// Result metadata emitted only after one handler accepts a verified frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameDispatchOutcome {
    pub peer: VerifiedPeerIdentity,
    pub kind: ClusterFrameKind,
}

/// Classified terminal result for registration or dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterFrameDispatchError {
    Authentication(PeerAuthenticationError),
    DuplicateHandler { kind: ClusterFrameKind },
    HandlerUnavailable { kind: ClusterFrameKind },
    HandlerFailure(ClusterFrameHandlerError),
    Shutdown,
}

impl fmt::Display for ClusterFrameDispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Authentication(error) => write!(f, "peer authentication failed: {error}"),
            Self::DuplicateHandler { kind } => write!(f, "handler already registered for {kind:?}"),
            Self::HandlerUnavailable { kind } => write!(f, "no handler registered for {kind:?}"),
            Self::HandlerFailure(error) => write!(f, "cluster frame handler failed: {error}"),
            Self::Shutdown => f.write_str("cluster frame dispatcher is shut down"),
        }
    }
}

impl Error for ClusterFrameDispatchError {}

/// The sole dispatcher for an authenticated cluster transport session.
///
/// A destination has at most one consumer, which prevents racing `subscribe`
/// calls from losing or double-processing a frame.
pub struct ClusterFrameDispatcher {
    handlers: BTreeMap<ClusterFrameKind, Box<dyn ClusterFrameHandler>>,
    shutdown: bool,
}

impl Default for ClusterFrameDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterFrameDispatcher {
    pub fn new() -> Self {
        Self {
            handlers: BTreeMap::new(),
            shutdown: false,
        }
    }

    pub fn register_handler(
        &mut self,
        kind: ClusterFrameKind,
        handler: Box<dyn ClusterFrameHandler>,
    ) -> Result<(), ClusterFrameDispatchError> {
        if self.shutdown {
            return Err(ClusterFrameDispatchError::Shutdown);
        }
        if self.handlers.contains_key(&kind) {
            return Err(ClusterFrameDispatchError::DuplicateHandler { kind });
        }
        self.handlers.insert(kind, handler);
        Ok(())
    }

    pub fn remove_handler(&mut self, kind: ClusterFrameKind) -> bool {
        self.handlers.remove(&kind).is_some()
    }

    pub fn shutdown(&mut self) {
        self.shutdown = true;
        self.handlers.clear();
    }

    pub fn handler_count(&self) -> usize {
        self.handlers.len()
    }

    /// Verifies the peer before converting the raw frame to the only type a
    /// handler can consume.
    pub fn authenticate_and_dispatch(
        &mut self,
        authenticator: &dyn ClusterPeerAuthenticator,
        inbound: InboundClusterFrame,
    ) -> Result<FrameDispatchOutcome, ClusterFrameDispatchError> {
        if self.shutdown {
            return Err(ClusterFrameDispatchError::Shutdown);
        }
        let peer = authenticator
            .authenticate(&inbound)
            .map_err(ClusterFrameDispatchError::Authentication)?;
        if peer.node_id() != &inbound.claimed_node_id {
            return Err(ClusterFrameDispatchError::Authentication(
                PeerAuthenticationError::NodeIdMismatch,
            ));
        }
        if peer.cluster_id() != &inbound.claimed_cluster_id {
            return Err(ClusterFrameDispatchError::Authentication(
                PeerAuthenticationError::ClusterIdMismatch,
            ));
        }
        self.dispatch_verified(VerifiedClusterFrame::from_authenticated(peer, inbound))
    }

    fn dispatch_verified(
        &mut self,
        frame: VerifiedClusterFrame,
    ) -> Result<FrameDispatchOutcome, ClusterFrameDispatchError> {
        let kind = frame.kind();
        let peer = frame.peer().clone();
        let handler = self
            .handlers
            .get_mut(&kind)
            .ok_or(ClusterFrameDispatchError::HandlerUnavailable { kind })?;
        handler
            .handle(frame)
            .map_err(ClusterFrameDispatchError::HandlerFailure)?;
        Ok(FrameDispatchOutcome { peer, kind })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct TestAuthenticator {
        trusted: bool,
    }

    impl ClusterPeerAuthenticator for TestAuthenticator {
        fn authenticate(
            &self,
            inbound: &InboundClusterFrame,
        ) -> Result<VerifiedPeerIdentity, PeerAuthenticationError> {
            if !self.trusted {
                return Err(PeerAuthenticationError::UntrustedPeer);
            }
            Ok(VerifiedPeerIdentity::new(
                inbound.claimed_node_id.clone(),
                inbound.claimed_cluster_id.clone(),
            ))
        }
    }

    struct MismatchedIdentityAuthenticator;

    impl ClusterPeerAuthenticator for MismatchedIdentityAuthenticator {
        fn authenticate(
            &self,
            _inbound: &InboundClusterFrame,
        ) -> Result<VerifiedPeerIdentity, PeerAuthenticationError> {
            Ok(VerifiedPeerIdentity::new("node-other", "cluster-a"))
        }
    }

    struct CountingHandler(Arc<Mutex<Vec<VerifiedClusterFrame>>>);

    impl ClusterFrameHandler for CountingHandler {
        fn handle(&mut self, frame: VerifiedClusterFrame) -> Result<(), ClusterFrameHandlerError> {
            self.0.lock().expect("test mutex").push(frame);
            Ok(())
        }
    }

    fn inbound() -> InboundClusterFrame {
        InboundClusterFrame {
            claimed_node_id: NodeId::new("node-a"),
            claimed_cluster_id: ClusterId::new("cluster-a"),
            kind: ClusterFrameKind::Application,
            payload: vec![1, 2, 3],
        }
    }

    #[test]
    fn untrusted_frame_never_reaches_a_handler() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut dispatcher = ClusterFrameDispatcher::new();
        dispatcher
            .register_handler(
                ClusterFrameKind::Application,
                Box::new(CountingHandler(received.clone())),
            )
            .unwrap();

        let error = dispatcher
            .authenticate_and_dispatch(&TestAuthenticator { trusted: false }, inbound())
            .unwrap_err();

        assert_eq!(
            error,
            ClusterFrameDispatchError::Authentication(PeerAuthenticationError::UntrustedPeer)
        );
        assert!(received.lock().expect("test mutex").is_empty());
    }

    #[test]
    fn exactly_one_consumer_can_receive_a_verified_frame() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut dispatcher = ClusterFrameDispatcher::new();
        dispatcher
            .register_handler(
                ClusterFrameKind::Application,
                Box::new(CountingHandler(received.clone())),
            )
            .unwrap();

        assert_eq!(
            dispatcher
                .register_handler(
                    ClusterFrameKind::Application,
                    Box::new(CountingHandler(Arc::new(Mutex::new(Vec::new())))),
                )
                .unwrap_err(),
            ClusterFrameDispatchError::DuplicateHandler {
                kind: ClusterFrameKind::Application
            }
        );

        let outcome = dispatcher
            .authenticate_and_dispatch(&TestAuthenticator { trusted: true }, inbound())
            .unwrap();
        assert_eq!(outcome.peer.node_id().as_str(), "node-a");
        assert_eq!(received.lock().expect("test mutex").len(), 1);
    }

    #[test]
    fn authenticated_identity_must_match_the_claimed_frame_identity() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut dispatcher = ClusterFrameDispatcher::new();
        dispatcher
            .register_handler(
                ClusterFrameKind::Application,
                Box::new(CountingHandler(received.clone())),
            )
            .unwrap();

        assert_eq!(
            dispatcher
                .authenticate_and_dispatch(&MismatchedIdentityAuthenticator, inbound())
                .unwrap_err(),
            ClusterFrameDispatchError::Authentication(PeerAuthenticationError::NodeIdMismatch)
        );
        assert!(received.lock().expect("test mutex").is_empty());
    }

    #[test]
    fn removed_or_shutdown_handler_has_a_classified_terminal_outcome() {
        let mut dispatcher = ClusterFrameDispatcher::new();
        dispatcher
            .register_handler(
                ClusterFrameKind::Application,
                Box::new(CountingHandler(Arc::new(Mutex::new(Vec::new())))),
            )
            .unwrap();
        assert!(dispatcher.remove_handler(ClusterFrameKind::Application));

        assert_eq!(
            dispatcher
                .authenticate_and_dispatch(&TestAuthenticator { trusted: true }, inbound())
                .unwrap_err(),
            ClusterFrameDispatchError::HandlerUnavailable {
                kind: ClusterFrameKind::Application
            }
        );

        dispatcher.shutdown();
        assert_eq!(
            dispatcher
                .authenticate_and_dispatch(&TestAuthenticator { trusted: true }, inbound())
                .unwrap_err(),
            ClusterFrameDispatchError::Shutdown
        );
    }
}
