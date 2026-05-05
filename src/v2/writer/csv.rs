use std::{
    fs::File,
    io::{BufWriter, Write as _},
    path::Path,
};

use super::view::{ClusterView, RenderMode};

/// CSV writer that streams rows as they arrive.
///
/// Constructed with an optional path; if `None` or file creation fails, operates as a no-op.
/// Errors are logged internally — all public methods are infallible.
pub struct CsvWriter {
    writer: Option<BufWriter<File>>,
    path: String,
}

impl CsvWriter {
    pub fn new(path: Option<&str>) -> Self {
        let Some(path) = path else {
            return Self {
                writer: None,
                path: String::new(),
            };
        };

        let writer = Self::open(path);
        Self {
            writer,
            path: path.to_owned(),
        }
    }

    fn open(path: &str) -> Option<BufWriter<File>> {
        let file = match File::create(Path::new(path)) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(path = %path, error = %e, "failed to create CSV file");
                return None;
            }
        };
        let mut writer = BufWriter::new(file);
        if let Err(e) = writeln!(
            writer,
            "status,cluster,primary,replicas,lag_bytes,reason,details_json"
        ) {
            tracing::error!(path = %path, error = %e, "failed to write CSV header");
            return None;
        }
        Some(writer)
    }

    pub fn write_row(&mut self, view: &ClusterView) {
        let Some(ref mut writer) = self.writer else {
            return;
        };
        if let Err(e) = writeln!(
            writer,
            "{},{},{},{},{},{},\"{}\"",
            view.status.as_str(),
            view.name,
            view.primary_content(RenderMode::Plain),
            view.replicas_content(RenderMode::Plain),
            view.lag_bytes.map(|l| l.to_string()).unwrap_or_default(),
            view.reason.short,
            view.reason.details_json.replace('"', "\"\"")
        ) {
            tracing::error!(path = %self.path, error = %e, "failed to write CSV row");
        }
    }

    pub fn flush(&mut self) {
        let Some(ref mut writer) = self.writer else {
            return;
        };
        if let Err(e) = writer.flush() {
            tracing::error!(path = %self.path, error = %e, "failed to flush CSV");
        } else {
            tracing::info!(path = %self.path, "CSV written successfully");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::view::{NodeView, PrimaryView, ReasonView, Status};
    use super::*;

    #[test]
    fn test_csv_strips_sigils_from_primary_and_replicas() {
        let view = ClusterView {
            status: Status::Healthy,
            name: "test-cluster".to_owned(),
            primary: PrimaryView::Single(NodeView {
                display: "db002".to_owned(),
                timeline: Some(7),
            }),
            replicas: super::super::view::ReplicasView::List(vec![
                super::super::view::ReplicaView {
                    node: NodeView {
                        display: "db003".to_owned(),
                        timeline: Some(7),
                    },
                    conn_count: 1,
                    backup_lag: None,
                },
            ]),
            lag_bytes: None,
            disk: "-".to_owned(),
            reason: ReasonView {
                short: "-".to_owned(),
                details_json: "{}".to_owned(),
            },
            failover: false,
        };

        let primary_csv = view.primary_content(RenderMode::Plain);
        let replicas_csv = view.replicas_content(RenderMode::Plain);

        assert_eq!(
            primary_csv, "db002",
            "CSV primary must not contain superscript sigils"
        );
        assert_eq!(
            replicas_csv, "db003",
            "CSV replicas must not contain superscript sigils"
        );

        let primary_terminal = view.primary_content(RenderMode::WithSigils);
        let replicas_terminal = view.replicas_content(RenderMode::WithSigils);

        assert_eq!(
            primary_terminal, "db002\u{2077}",
            "terminal primary must include timeline sigil"
        );
        assert_eq!(
            replicas_terminal, "db003\u{2077}",
            "terminal replicas must include timeline sigil"
        );
    }
}
