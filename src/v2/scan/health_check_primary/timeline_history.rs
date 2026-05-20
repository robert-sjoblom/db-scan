//! Parses the contents of a postgres timeline-history file.
//!
//! See `https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-TIMELINES`
//! for the format. Lines are tab-separated `<prev_tli> <switch_lsn> <reason>`;
//! Lines starting with `#` are comments.

#[derive(Debug, PartialEq, Eq)]
#[expect(clippy::used_underscore_items, reason = "will be used shortly")]
pub struct TimelineHistoryEntry {
    pub previous_tli: i32,
    pub switch_lsn: String,
    pub reason: String,
}

impl super::PrimaryHealthCheckResult {
    /// Parse the collected timeline-history file. Returns an empty vec when
    /// `timeline_history` is None (TL=1, no history file yet).
    pub fn _timeline_history_entries(&self) -> Vec<TimelineHistoryEntry> {
        let Some(history) = &self.timeline_history else {
            return Vec::new();
        };

        history
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .filter_map(|line| {
                let mut parts = line.splitn(3, char::is_whitespace);
                let prev = parts.next()?.trim().parse::<i32>().ok()?;
                let lsn = parts.next()?.trim().to_owned();
                let reason = parts.next().unwrap_or("").trim().to_owned();
                Some(TimelineHistoryEntry {
                    previous_tli: prev,
                    switch_lsn: lsn,
                    reason,
                })
            })
            .collect()
    }

    /// LSN at which `from_tli` ended and a new timeline forked off, if recorded.
    pub fn _fork_lsn_for(&self, from_tli: i32) -> Option<String> {
        #[expect(clippy::used_underscore_items, reason = "will be used shortly")]
        self._timeline_history_entries()
            .into_iter()
            .find(|e| e.previous_tli == from_tli)
            .map(|e| e.switch_lsn)
    }
}

#[cfg(test)]
#[expect(clippy::used_underscore_items, reason = "will be used shortly")]
mod tests {
    use crate::v2::{
        scan::health_check_primary::PrimaryHealthCheckResult, tests_common::PrimaryHealthBuilder,
    };

    use super::*;
    use pretty_assertions::assert_eq;

    fn with_history(history: &str) -> PrimaryHealthCheckResult {
        PrimaryHealthBuilder::new()
            .with_timeline_history(history)
            .build()
    }

    #[test]
    fn parses_single_entry() {
        let h = with_history("1\t0/3000000\tno recovery target specified\n");
        assert_eq!(
            h._timeline_history_entries(),
            vec![TimelineHistoryEntry {
                previous_tli: 1,
                switch_lsn: "0/3000000".to_owned(),
                reason: "no recovery target specified".to_owned(),
            }]
        );
    }

    #[test]
    fn skips_blank_and_comment_lines() {
        let h = with_history("# header comment\n\n1\t0/3000000\treason1\n");
        assert_eq!(h._timeline_history_entries().len(), 1);
    }

    #[test]
    fn parses_multi_line_history() {
        let h = with_history("1\t0/3000000\treason1\n2\t0/5000000\treason2\n");
        let entries = h._timeline_history_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].previous_tli, 2);
        assert_eq!(entries[1].switch_lsn, "0/5000000");
    }

    #[test]
    fn fork_lsn_returns_switch_point() {
        let h = with_history("1\t0/3000000\treason1\n2\t0/5000000\treason2\n");
        assert_eq!(h._fork_lsn_for(1), Some("0/3000000".to_owned()));
        assert_eq!(h._fork_lsn_for(2), Some("0/5000000".to_owned()));
        assert_eq!(h._fork_lsn_for(3), None);
    }

    #[test]
    fn none_history_returns_no_entries() {
        let h: PrimaryHealthCheckResult = PrimaryHealthBuilder::new().build();
        assert_eq!(h._timeline_history_entries(), vec![]);
    }
}
