use std::sync::Arc;
use tokio::sync::Mutex;

use crate::raft::{
    RaftNode,
    election::{self, ElectionConfig},
    log::RaftLog,
    replication::ReplicationConfig,
    rpc::{RequestVoteArgs, RequestVoteReply},
    state::{RaftState, Role},
    storage::RaftStorage,
};

use super::mock_transport::MockTransport;

// region Helpers

fn make_storage() -> (Arc<RaftStorage>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let s = Arc::new(RaftStorage::new(dir.path().to_path_buf()).unwrap());
    (s, dir)
}

fn make_state(id: &str, peers: &[&str]) -> Arc<Mutex<RaftState>> {
    Arc::new(Mutex::new(RaftState::new(
        id.into(),
        peers.iter().map(|s| s.to_string()).collect(),
    )))
}

fn empty_log<C: Clone + std::fmt::Debug + Default + 'static>() -> Arc<Mutex<RaftLog<C>>> {
    Arc::new(Mutex::new(RaftLog::new()))
}

fn make_vote_request(term: u64, candidate: &str) -> RequestVoteArgs {
    RequestVoteArgs {
        term,
        candidate_id: candidate.into(),
        last_log_index: 0,
        last_log_term: 0,
    }
}

fn make_raft_node(id: u64, peers: &[&str]) -> (RaftNode, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let node = RaftNode::new(
        id,
        peers.iter().map(|s| s.to_string()).collect(),
        dir.path().to_path_buf(),
        ElectionConfig::default(),
        ReplicationConfig::default(),
    );
    (node, dir)
}

// endregion

// region handle_request_vote

#[tokio::test]
async fn denies_stale_term() {
    let (node, _dir) = make_raft_node(1, &["peer1"]);
    // Set current term to 5.
    node.state.lock().await.persistent.current_term = 5;

    let reply = node
        .handle_request_vote(RequestVoteArgs {
            term: 4, // stale
            candidate_id: "peer1".into(),
            last_log_index: 0,
            last_log_term: 0,
        })
        .await;

    assert!(!reply.vote_granted);
    assert_eq!(reply.term, 5);
}

#[tokio::test]
async fn grants_if_not_voted_and_log_up_to_date() {
    let (node, _dir) = make_raft_node(1, &["peer1"]);
    // No prior vote, empty log.
    let reply = node
        .handle_request_vote(make_vote_request(1, "peer1"))
        .await;

    assert!(reply.vote_granted);
}

#[tokio::test]
async fn denies_if_already_voted_for_different_candidate() {
    let (node, _dir) = make_raft_node(1, &["peer1", "peer2"]);
    node.state.lock().await.persistent.current_term = 1;
    node.state.lock().await.persistent.voted_for = Some("peer2".into());

    let reply = node
        .handle_request_vote(make_vote_request(1, "peer1"))
        .await;

    assert!(!reply.vote_granted);
}

#[tokio::test]
async fn grants_if_voted_same_candidate_again() {
    let (node, _dir) = make_raft_node(1, &["peer1"]);
    node.state.lock().await.persistent.current_term = 1;
    node.state.lock().await.persistent.voted_for = Some("peer1".into());

    let reply = node
        .handle_request_vote(make_vote_request(1, "peer1"))
        .await;

    assert!(reply.vote_granted);
}

#[tokio::test]
async fn steps_down_on_higher_term_in_vote_request() {
    let (node, _dir) = make_raft_node(1, &["peer1"]);
    node.state.lock().await.persistent.current_term = 3;
    node.state.lock().await.role = Role::Leader;

    let reply = node
        .handle_request_vote(RequestVoteArgs {
            term: 7,
            candidate_id: "peer1".into(),
            last_log_index: 0,
            last_log_term: 0,
        })
        .await;

    let sg = node.state.lock().await;
    assert_eq!(sg.role, Role::Follower);
    assert_eq!(sg.persistent.current_term, 7);
    // Vote granted to peer1 since we stepped down and have no prior vote.
    assert!(reply.vote_granted);
}

#[tokio::test]
async fn denies_if_candidate_log_behind() {
    let (node, _dir) = make_raft_node(1, &["peer1"]);
    // Our log has 3 entries at term 2; candidate only has term 1.
    {
        let mut lg = node.log.lock().await;
        lg.append_command(2, crate::common::Command::NoOp);
        lg.append_command(2, crate::common::Command::NoOp);
        lg.append_command(2, crate::common::Command::NoOp);
    }
    node.state.lock().await.persistent.current_term = 2;

    let reply = node
        .handle_request_vote(RequestVoteArgs {
            term: 2,
            candidate_id: "peer1".into(),
            last_log_index: 3,
            last_log_term: 1, // lower term than our last_term=2 → not up-to-date
        })
        .await;

    assert!(!reply.vote_granted);
}

#[tokio::test]
async fn persists_vote_before_replying() {
    let (node, _dir) = make_raft_node(1, &["peer1"]);

    let reply = node
        .handle_request_vote(make_vote_request(1, "peer1"))
        .await;

    assert!(reply.vote_granted);
    // Storage should have the vote persisted.
    let stored = node.storage.load_persistent_state().unwrap().unwrap();
    assert_eq!(stored.voted_for, Some("peer1".into()));
}

// endregion

// region handle_vote_response

#[tokio::test]
async fn handle_vote_response_steps_down_on_higher_term() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2", "n3"]);
    state.lock().await.persistent.current_term = 2;
    state.lock().await.role = Role::Candidate;

    let mut votes = 1usize; // self-vote
    let won = election::handle_vote_response(
        state.clone(),
        storage,
        "n2".into(),
        2,
        RequestVoteReply {
            term: 10,
            vote_granted: false,
        },
        &mut votes,
        3,
    )
    .await;

    assert!(!won);
    let sg = state.lock().await;
    assert_eq!(sg.role, Role::Follower);
    assert_eq!(sg.persistent.current_term, 10);
}

#[tokio::test]
async fn handle_vote_response_ignores_stale_election_term() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2"]);
    state.lock().await.persistent.current_term = 3; // moved on
    state.lock().await.role = Role::Candidate;

    let mut votes = 1usize;
    let won = election::handle_vote_response(
        state.clone(),
        storage,
        "n2".into(),
        2, // reply is for old election term 2
        RequestVoteReply {
            term: 3,
            vote_granted: true,
        },
        &mut votes,
        2,
    )
    .await;

    // election_term (2) != current_term (3) → stale → ignore
    assert!(!won);
    assert_eq!(votes, 1); // not incremented
}

#[tokio::test]
async fn handle_vote_response_ignores_if_no_longer_candidate() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2"]);
    state.lock().await.persistent.current_term = 1;
    state.lock().await.role = Role::Follower; // already stepped down

    let mut votes = 1usize;
    let won = election::handle_vote_response(
        state,
        storage,
        "n2".into(),
        1,
        RequestVoteReply {
            term: 1,
            vote_granted: true,
        },
        &mut votes,
        2,
    )
    .await;

    assert!(!won);
    assert_eq!(votes, 1); // not incremented
}

#[tokio::test]
async fn handle_vote_response_counts_granted_votes() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2", "n3", "n4"]);
    state.lock().await.persistent.current_term = 1;
    state.lock().await.role = Role::Candidate;

    let mut votes = 1usize; // self-vote
    let won = election::handle_vote_response(
        state,
        storage,
        "n2".into(),
        1,
        RequestVoteReply {
            term: 1,
            vote_granted: true,
        },
        &mut votes,
        4, // cluster_size=4 → need >2
    )
    .await;

    assert!(!won); // only 2 votes, need 3
    assert_eq!(votes, 2);
}

#[tokio::test]
async fn handle_vote_response_returns_true_on_majority() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2", "n3"]);
    state.lock().await.persistent.current_term = 1;
    state.lock().await.role = Role::Candidate;

    let mut votes = 1usize; // self-vote
    // First granted vote — still not majority (2/3).
    let _won = election::handle_vote_response(
        state.clone(),
        Arc::clone(&storage),
        "n2".into(),
        1,
        RequestVoteReply {
            term: 1,
            vote_granted: true,
        },
        &mut votes,
        3,
    )
    .await;
    assert_eq!(votes, 2);

    // Second granted vote — now 3/3 → majority.
    let won = election::handle_vote_response(
        state,
        storage,
        "n3".into(),
        1,
        RequestVoteReply {
            term: 1,
            vote_granted: true,
        },
        &mut votes,
        3,
    )
    .await;
    assert!(won);
    assert_eq!(votes, 3);
}

// endregion

// region start_election

#[tokio::test]
async fn start_election_increments_term() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2", "n3"]);
    state.lock().await.persistent.current_term = 4;
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let timer = election::ElectionTimer::start(
        election::ElectionConfig::default(),
        tokio::sync::mpsc::channel(1).0,
    );

    election::start_election(state.clone(), log, transport, storage, &timer, tx)
        .await
        .unwrap();

    assert_eq!(state.lock().await.persistent.current_term, 5);
}

#[tokio::test]
async fn start_election_sets_role_to_candidate() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2"]);
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let timer = election::ElectionTimer::start(
        election::ElectionConfig::default(),
        tokio::sync::mpsc::channel(1).0,
    );

    election::start_election(state.clone(), log, transport, storage, &timer, tx)
        .await
        .unwrap();

    assert_eq!(state.lock().await.role, Role::Candidate);
}

#[tokio::test]
async fn start_election_votes_for_self() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2"]);
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let timer = election::ElectionTimer::start(
        election::ElectionConfig::default(),
        tokio::sync::mpsc::channel(1).0,
    );

    election::start_election(state.clone(), log, transport, storage, &timer, tx)
        .await
        .unwrap();

    assert_eq!(state.lock().await.persistent.voted_for, Some("n1".into()));
}

#[tokio::test]
async fn start_election_persists_state() {
    let (storage, dir) = make_storage();
    let state = make_state("n1", &["n2"]);
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let timer = election::ElectionTimer::start(
        election::ElectionConfig::default(),
        tokio::sync::mpsc::channel(1).0,
    );

    election::start_election(
        state.clone(),
        log,
        transport,
        Arc::clone(&storage),
        &timer,
        tx,
    )
    .await
    .unwrap();

    let storage2 = RaftStorage::new(dir.path().to_path_buf()).unwrap();
    let ps = storage2.load_persistent_state().unwrap().unwrap();
    assert_eq!(ps.current_term, 1);
    assert_eq!(ps.voted_for, Some("n1".into()));
}

#[tokio::test]
async fn start_election_sends_vote_requests_to_all_peers() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2", "n3"]);
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::<crate::common::Command>::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let timer = election::ElectionTimer::start(
        election::ElectionConfig::default(),
        tokio::sync::mpsc::channel(1).0,
    );

    election::start_election(state, log, Arc::clone(&transport), storage, &timer, tx)
        .await
        .unwrap();

    // Give spawned tasks a moment to run.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let sent = transport.sent_vote_requests();
    let peers_contacted: std::collections::HashSet<String> =
        sent.iter().map(|(peer, _)| peer.clone()).collect();
    assert!(peers_contacted.contains("n2"));
    assert!(peers_contacted.contains("n3"));
}

// endregion

// region become_leader

#[tokio::test]
async fn become_leader_requires_candidate_role() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2"]);
    // Role is Follower — become_leader must fail.
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::<crate::common::Command>::new();
    transport.queue_append_success_for_all(&["n2"], 1);

    let result = election::become_leader(state, log, transport, storage).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn become_leader_sets_role_to_leader() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2"]);
    state.lock().await.role = Role::Candidate;
    state.lock().await.persistent.current_term = 1;
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::<crate::common::Command>::new();
    transport.queue_append_success_for_all(&["n2"], 1);

    election::become_leader(state.clone(), log, transport, storage)
        .await
        .unwrap();

    assert_eq!(state.lock().await.role, Role::Leader);
}

#[tokio::test]
async fn become_leader_initialises_leader_state() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2", "n3"]);
    state.lock().await.role = Role::Candidate;
    state.lock().await.persistent.current_term = 1;
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::<crate::common::Command>::new();
    transport.queue_append_success_for_all(&["n2", "n3"], 1);

    election::become_leader(state.clone(), log.clone(), transport, storage)
        .await
        .unwrap();

    let sg = state.lock().await;
    let ls = sg.leader_state.as_ref().unwrap();
    // LeaderState is initialised before the no-op is appended (log was empty
    // at election time → last_log_index=0 → next_index=1 for both peers).
    assert_eq!(ls.peers["n2"].next_index, 1);
    assert_eq!(ls.peers["n3"].next_index, 1);
}

#[tokio::test]
async fn become_leader_appends_noop_entry() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2"]);
    state.lock().await.role = Role::Candidate;
    state.lock().await.persistent.current_term = 3;
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::<crate::common::Command>::new();
    transport.queue_append_success_for_all(&["n2"], 3);

    election::become_leader(state, log.clone(), transport, storage)
        .await
        .unwrap();

    let lg = log.lock().await;
    assert_eq!(lg.last_index(), 1);
    assert!(matches!(
        lg.get(1).unwrap().command,
        crate::common::Command::NoOp
    ));
}

#[tokio::test]
async fn become_leader_broadcasts_heartbeat() {
    let (storage, _dir) = make_storage();
    let state = make_state("n1", &["n2", "n3"]);
    state.lock().await.role = Role::Candidate;
    state.lock().await.persistent.current_term = 1;
    let log = empty_log::<crate::common::Command>();
    let transport = MockTransport::<crate::common::Command>::new();
    transport.queue_append_success_for_all(&["n2", "n3"], 1);

    election::become_leader(state, log, Arc::clone(&transport), storage)
        .await
        .unwrap();

    // Give spawned heartbeat tasks time to run.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let sent = transport.sent_append_entries();
    let heartbeat_peers: std::collections::HashSet<String> = sent
        .iter()
        .filter(|(_, args)| args.entries.is_empty())
        .map(|(peer, _)| peer.clone())
        .collect();
    assert!(heartbeat_peers.contains("n2"));
    assert!(heartbeat_peers.contains("n3"));
}

// endregion
