//! Embedded facade for the v0.9 durable changefeed lifecycle.
//!
//! This module has no remote client or local-WAL fallback.  A handle exists
//! only after authorization and Durable preflight both permit feed creation.

use std::sync::Mutex;

use alopex_cluster::{
    changefeed::{
        ChangefeedAccessRequest, ChangefeedAction, ChangefeedAuthorization,
        ChangefeedAuthorizationDecision, DurableProfileAdapter, FeedCoordinator, FeedDelivery,
        FeedRequest,
    },
    ChangefeedOutcome, FeedIdentity, RoutingOutcome,
};

use crate::{Error, Result};

/// Result of embedded feed creation. A denied or unavailable feed returns the
/// canonical outcome and deliberately does not hand out a usable handle.
pub struct CreateChangefeedResult {
    /// Canonical result of the create lifecycle operation.
    pub outcome: ChangefeedOutcome,
    /// Handle present only when the feed was created successfully.
    pub changefeed: Option<Changefeed>,
}

/// Stateful embedded handle for one authorized feed lifecycle.
pub struct Changefeed {
    feed: FeedIdentity,
    routing: RoutingOutcome,
    authorization: ChangefeedAuthorization,
    tenant: String,
    coordinator: Mutex<FeedCoordinator>,
}

impl Changefeed {
    /// Builds a handle only after the create action was authorized. The
    /// adapter's preflight is passed directly to the coordinator, so a local
    /// embedded database cannot silently replace unavailable Durable service.
    pub(crate) fn create(
        adapter: DurableProfileAdapter,
        authorization: ChangefeedAuthorization,
        tenant: String,
        feed: FeedIdentity,
        routing: RoutingOutcome,
        request: FeedRequest,
    ) -> Result<CreateChangefeedResult> {
        let create_access = ChangefeedAccessRequest {
            action: ChangefeedAction::Create,
            tenant: tenant.clone(),
            range_id: feed.range.range_id.clone(),
        };
        if authorization.authorize(create_access) == ChangefeedAuthorizationDecision::Denied {
            return Ok(CreateChangefeedResult {
                outcome: ChangefeedAuthorizationDecision::Denied
                    .denied_outcome(feed, routing, request.operation_id, request.request_id)
                    .map_err(Error::ChangefeedModel)?,
                changefeed: None,
            });
        }

        let mut coordinator = FeedCoordinator::new(adapter.preflight());
        let outcome = coordinator
            .create(feed.clone(), routing.clone(), request)
            .map_err(Error::ChangefeedCoordinator)?;
        let changefeed = if outcome.failure_class.is_none() {
            Some(Self {
                feed,
                routing,
                authorization,
                tenant,
                coordinator: Mutex::new(coordinator),
            })
        } else {
            None
        };
        Ok(CreateChangefeedResult {
            outcome,
            changefeed,
        })
    }

    /// Subscribes after verifying range generation/epoch and `changefeed.read`.
    pub fn subscribe(
        &self,
        expected_generation: u64,
        expected_epoch: u64,
        request: FeedRequest,
    ) -> Result<ChangefeedOutcome> {
        if let Some(denied) = self.denied(ChangefeedAction::Subscribe, &request)? {
            return Ok(denied);
        }
        self.coordinator()?
            .subscribe(
                &self.feed.feed_id,
                expected_generation,
                expected_epoch,
                request,
            )
            .map_err(Error::ChangefeedCoordinator)
    }

    /// Polls the authorized feed without converting a classified failure into
    /// an empty successful response.
    pub fn poll(&self, max_events: usize, request: FeedRequest) -> Result<FeedDelivery> {
        if let Some(denied) = self.denied(ChangefeedAction::Poll, &request)? {
            return Ok(FeedDelivery {
                outcome: denied,
                events: Vec::new(),
            });
        }
        self.coordinator()?
            .poll(&self.feed.feed_id, max_events, request)
            .map_err(Error::ChangefeedCoordinator)
    }

    /// Returns the same authorized, range-ordered batch contract as polling.
    pub fn stream(&self, max_events: usize, request: FeedRequest) -> Result<FeedDelivery> {
        if let Some(denied) = self.denied(ChangefeedAction::Stream, &request)? {
            return Ok(FeedDelivery {
                outcome: denied,
                events: Vec::new(),
            });
        }
        self.coordinator()?
            .stream(&self.feed.feed_id, max_events, request)
            .map_err(Error::ChangefeedCoordinator)
    }

    /// Accepts an acknowledgement only under the `changefeed.ack` scope.
    pub fn ack(
        &self,
        ack_id: impl Into<String>,
        request: FeedRequest,
    ) -> Result<ChangefeedOutcome> {
        if let Some(denied) = self.denied(ChangefeedAction::Ack, &request)? {
            return Ok(denied);
        }
        self.coordinator()?
            .ack(&self.feed.feed_id, ack_id, request)
            .map_err(Error::ChangefeedCoordinator)
    }

    /// Resumes strictly after the supplied encoded checkpoint under
    /// `changefeed.read` authorization.
    pub fn resume(&self, checkpoint: &str, request: FeedRequest) -> Result<FeedDelivery> {
        if let Some(denied) = self.denied(ChangefeedAction::Resume, &request)? {
            return Ok(FeedDelivery {
                outcome: denied,
                events: Vec::new(),
            });
        }
        self.coordinator()?
            .resume(&self.feed.feed_id, checkpoint, request)
            .map_err(Error::ChangefeedCoordinator)
    }

    /// Cancels the feed under `changefeed.ack` authorization.
    pub fn cancel(&self, request: FeedRequest) -> Result<ChangefeedOutcome> {
        if let Some(denied) = self.denied(ChangefeedAction::Cancel, &request)? {
            return Ok(denied);
        }
        self.coordinator()?
            .cancel(&self.feed.feed_id, request)
            .map_err(Error::ChangefeedCoordinator)
    }

    /// Closes the feed under `changefeed.ack` authorization.
    pub fn close(&self, request: FeedRequest) -> Result<ChangefeedOutcome> {
        if let Some(denied) = self.denied(ChangefeedAction::Close, &request)? {
            return Ok(denied);
        }
        self.coordinator()?
            .close(&self.feed.feed_id, request)
            .map_err(Error::ChangefeedCoordinator)
    }

    fn coordinator(&self) -> Result<std::sync::MutexGuard<'_, FeedCoordinator>> {
        self.coordinator
            .lock()
            .map_err(|_| Error::ChangefeedLockPoisoned)
    }

    fn denied(
        &self,
        action: ChangefeedAction,
        request: &FeedRequest,
    ) -> Result<Option<ChangefeedOutcome>> {
        let access = ChangefeedAccessRequest {
            action,
            tenant: self.tenant.clone(),
            range_id: self.feed.range.range_id.clone(),
        };
        if self.authorization.authorize(access).permits() {
            Ok(None)
        } else {
            Ok(Some(
                ChangefeedAuthorizationDecision::Denied
                    .denied_outcome(
                        self.feed.clone(),
                        self.routing.clone(),
                        request.operation_id.clone(),
                        request.request_id.clone(),
                    )
                    .map_err(Error::ChangefeedModel)?,
            ))
        }
    }
}
