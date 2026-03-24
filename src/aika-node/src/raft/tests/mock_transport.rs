use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::raft::{
    rpc::{
        AppendEntriesArgs, AppendEntriesReply, RaftTransport, RequestVoteArgs, RequestVoteReply,
        TransportError,
    },
    state::NodeId,
};

/// A controllable in-process transport for Raft unit tests.
///
/// Call `queue_append_entries_reply` before sending RPCs to pre-program what
/// the "network" returns.  Use `sent_vote_requests` / `sent_append_entries`
/// to assert on what was actually sent.
///
/// Replies are consumed FIFO per peer.  If no reply is queued for a peer,
/// the call returns `TransportError::Unreachable`.
pub struct MockTransport<C> {
    vote_replies: Mutex<HashMap<NodeId, VecDeque<Result<RequestVoteReply, TransportError>>>>,
    append_replies: Mutex<HashMap<NodeId, VecDeque<Result<AppendEntriesReply, TransportError>>>>,

    pub sent_vote_requests: Mutex<Vec<(NodeId, RequestVoteArgs)>>,
    pub sent_append_entries: Mutex<Vec<(NodeId, AppendEntriesArgs<C>)>>,
}

impl<C: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static> MockTransport<C> {
    pub fn new() -> Arc<Self> {
        Arc::new(MockTransport {
            vote_replies: Mutex::new(HashMap::new()),
            append_replies: Mutex::new(HashMap::new()),
            sent_vote_requests: Mutex::new(Vec::new()),
            sent_append_entries: Mutex::new(Vec::new()),
        })
    }

    /// Queue a reply that will be returned the next time `send_append_entries`
    /// is called for `peer`.
    pub fn queue_append_entries_reply(
        &self,
        peer: &str,
        reply: Result<AppendEntriesReply, TransportError>,
    ) {
        self.append_replies
            .lock()
            .unwrap()
            .entry(peer.to_string())
            .or_default()
            .push_back(reply);
    }

    pub fn sent_vote_requests(&self) -> Vec<(NodeId, RequestVoteArgs)> {
        self.sent_vote_requests.lock().unwrap().clone()
    }

    pub fn sent_append_entries(&self) -> Vec<(NodeId, AppendEntriesArgs<C>)> {
        self.sent_append_entries.lock().unwrap().clone()
    }

    /// Queue an immediate success reply for `send_append_entries` to every
    /// peer in `peers`.
    pub fn queue_append_success_for_all(&self, peers: &[&str], term: u64) {
        for peer in peers {
            self.queue_append_entries_reply(
                peer,
                Ok(AppendEntriesReply {
                    term,
                    success: true,
                    conflict_index: None,
                    conflict_term: None,
                }),
            );
        }
    }
}

#[async_trait]
impl<C: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static> RaftTransport<C>
    for MockTransport<C>
{
    async fn send_request_vote(
        &self,
        peer: &NodeId,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply, TransportError> {
        self.sent_vote_requests
            .lock()
            .unwrap()
            .push((peer.clone(), args));

        self.vote_replies
            .lock()
            .unwrap()
            .get_mut(peer)
            .and_then(|q| q.pop_front())
            .unwrap_or(Err(TransportError::Unreachable(peer.clone())))
    }

    async fn send_append_entries(
        &self,
        peer: &NodeId,
        args: AppendEntriesArgs<C>,
    ) -> Result<AppendEntriesReply, TransportError> {
        self.sent_append_entries
            .lock()
            .unwrap()
            .push((peer.clone(), args));

        self.append_replies
            .lock()
            .unwrap()
            .get_mut(peer)
            .and_then(|q| q.pop_front())
            .unwrap_or(Err(TransportError::Unreachable(peer.clone())))
    }
}
