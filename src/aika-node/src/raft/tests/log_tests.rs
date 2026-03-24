use crate::raft::log::{LogEntry, RaftLog};

// region Helpers
fn log_with(pairs: &[(u64, u64)]) -> RaftLog<u32> {
    // pairs: (term, index)
    let mut log = RaftLog::new();
    for &(term, index) in pairs {
        let _ = log.append_command(term, index as u32);
    }
    log
}

// endregion

// region empty log

#[test]
fn empty_log_has_zero_index_and_term() {
    let log: RaftLog<u32> = RaftLog::new();
    assert_eq!(log.last_index(), 0);
    assert_eq!(log.last_term(), 0);
}

// endregion

// region append

#[test]
fn append_command_assigns_sequential_indices() {
    let mut log = RaftLog::new();
    let i1 = log.append_command(1, 10u32);
    let i2 = log.append_command(1, 20u32);
    let i3 = log.append_command(2, 30u32);
    assert_eq!(i1, 1);
    assert_eq!(i2, 2);
    assert_eq!(i3, 3);
    assert_eq!(log.last_index(), 3);
}

#[test]
fn append_stores_term_correctly() {
    let mut log = RaftLog::new();
    log.append_command(5, 0u32);
    assert_eq!(log.last_term(), 5);
    assert_eq!(log.get(1).unwrap().term, 5);
}

// endregion

// region get

#[test]
fn get_returns_none_for_index_zero() {
    let log = log_with(&[(1, 1), (1, 2)]);
    assert!(log.get(0).is_none());
}

#[test]
fn get_returns_none_past_end() {
    let log = log_with(&[(1, 1)]);
    assert!(log.get(2).is_none());
}

#[test]
fn get_returns_correct_entry() {
    let mut log = RaftLog::new();
    log.append_command(3, 99u32);
    let e = log.get(1).unwrap();
    assert_eq!(e.term, 3);
    assert_eq!(e.index, 1);
    assert_eq!(e.command, 99);
}

// endregion

// region slice

#[test]
fn slice_empty_when_from_equals_to() {
    let log = log_with(&[(1, 1), (1, 2), (1, 3)]);
    assert_eq!(log.slice(2, 2).len(), 0);
}

#[test]
fn slice_returns_correct_range() {
    let log = log_with(&[(1, 1), (2, 2), (3, 3), (4, 4)]);
    let s = log.slice(2, 4);
    assert_eq!(s.len(), 2);
    assert_eq!(s[0].index, 2);
    assert_eq!(s[1].index, 3);
}

#[test]
fn slice_clamps_to_log_end() {
    let log = log_with(&[(1, 1), (1, 2)]);
    let s = log.slice(1, 999);
    assert_eq!(s.len(), 2);
}

#[test]
fn slice_on_empty_log_returns_empty() {
    let log: RaftLog<u32> = RaftLog::new();
    assert_eq!(log.slice(1, 5).len(), 0);
}

// endregion

// region entries_from

#[test]
fn entries_from_empty_log_returns_empty() {
    let log: RaftLog<u32> = RaftLog::new();
    assert!(log.entries_from(1).is_empty());
}

#[test]
fn entries_from_returns_correct_suffix() {
    let log = log_with(&[(1, 1), (1, 2), (2, 3), (2, 4)]);
    let v = log.entries_from(3);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].index, 3);
    assert_eq!(v[1].index, 4);
}

#[test]
fn entries_from_1_returns_all() {
    let log = log_with(&[(1, 1), (2, 2)]);
    assert_eq!(log.entries_from(1).len(), 2);
}

// endregion

// region is_at_least_as_up_to_date

#[test]
fn up_to_date_higher_term_wins() {
    let log = log_with(&[(3, 1), (3, 2)]); // last_term=3, last_index=2
    assert!(log.is_at_least_as_up_to_date(1, 5)); // term 5 > 3 → up-to-date
}

#[test]
fn up_to_date_lower_term_loses() {
    let log = log_with(&[(5, 1)]);
    assert!(!log.is_at_least_as_up_to_date(10, 3)); // term 3 < 5 → not up-to-date
}

#[test]
fn up_to_date_same_term_longer_log_wins() {
    let log = log_with(&[(2, 1), (2, 2)]); // last_term=2, last_index=2
    assert!(log.is_at_least_as_up_to_date(5, 2)); // same term, index 5 > 2
}

#[test]
fn up_to_date_same_term_shorter_log_loses() {
    let log = log_with(&[(2, 1), (2, 2), (2, 3)]); // last_index=3
    assert!(!log.is_at_least_as_up_to_date(2, 2)); // same term, index 2 < 3
}

#[test]
fn up_to_date_exactly_equal_is_up_to_date() {
    let log = log_with(&[(2, 1), (2, 2)]);
    assert!(log.is_at_least_as_up_to_date(2, 2)); // same term and length
}

// endregion

// region truncate_from

#[test]
fn truncate_from_removes_tail() {
    let mut log = log_with(&[(1, 1), (1, 2), (1, 3), (1, 4)]);
    log.truncate_from(2);
    assert_eq!(log.last_index(), 2);
    assert!(log.get(3).is_none());
}

#[test]
fn truncate_from_zero_empties_log() {
    let mut log = log_with(&[(1, 1), (2, 2)]);
    log.truncate_from(0);
    assert_eq!(log.last_index(), 0);
}

#[test]
fn truncate_from_past_end_is_noop() {
    let mut log = log_with(&[(1, 1), (1, 2)]);
    log.truncate_from(5);
    assert_eq!(log.last_index(), 2);
}

// endregion

// region append_entries_from_leader

#[test]
fn append_entries_from_leader_no_conflict() {
    let mut log = log_with(&[(1, 1)]);
    let new_entries = vec![
        LogEntry {
            term: 1,
            index: 2,
            command: 20u32,
        },
        LogEntry {
            term: 1,
            index: 3,
            command: 30u32,
        },
    ];
    log.append_entries_from_leader(1, new_entries);
    assert_eq!(log.last_index(), 3);
}

#[test]
fn append_entries_from_leader_skips_matching_entries() {
    let mut log = log_with(&[(1, 1), (1, 2)]);
    // Re-send same entries (idempotent).
    let e = log.get(2).unwrap().clone();
    log.append_entries_from_leader(1, vec![e]);
    assert_eq!(log.last_index(), 2);
}

#[test]
fn append_entries_from_leader_truncates_on_conflict() {
    let mut log = log_with(&[(1, 1), (1, 2), (1, 3)]);
    // Leader sends index 2 with term 2 — conflicts with existing term 1 at index 2.
    let new_entries = vec![
        LogEntry {
            term: 2,
            index: 2,
            command: 99u32,
        },
        LogEntry {
            term: 2,
            index: 3,
            command: 100u32,
        },
    ];
    log.append_entries_from_leader(1, new_entries);
    assert_eq!(log.last_index(), 3);
    assert_eq!(log.get(2).unwrap().term, 2);
    assert_eq!(log.get(3).unwrap().term, 2);
}

#[test]
fn append_entries_from_leader_empty_is_noop() {
    let mut log = log_with(&[(1, 1)]);
    log.append_entries_from_leader(1, vec![]);
    assert_eq!(log.last_index(), 1);
}

// endregion

// region from_entries (recovery path)

#[test]
fn from_entries_roundtrip() {
    let original = log_with(&[(1, 1), (2, 2), (2, 3)]);
    let entries: Vec<_> = (1..=original.last_index())
        .map(|i| original.get(i).unwrap().clone())
        .collect();
    let recovered = RaftLog::from_entries(entries);
    assert_eq!(recovered.last_index(), 3);
    assert_eq!(recovered.get(2).unwrap().term, 2);
}

#[test]
#[should_panic]
fn from_entries_panics_on_wrong_index() {
    let entries = vec![
        LogEntry {
            term: 1,
            index: 1,
            command: 0u32,
        },
        LogEntry {
            term: 1,
            index: 3,
            command: 0u32,
        }, // gap — should panic
    ];
    RaftLog::from_entries(entries);
}

// endregion
