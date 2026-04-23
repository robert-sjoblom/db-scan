use super::units::to_superscript;

pub(crate) const RESET: &str = "\x1b[0m";

#[derive(Clone, Copy)]
pub(crate) enum RenderMode {
    Plain,
    WithSigils,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Status {
    Healthy = 0,
    Unknown = 1,
    Degraded = 2,
    Critical = 3,
}

impl Status {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Status::Critical => "CRITICAL",
            Status::Degraded => "DEGRADED",
            Status::Unknown => "UNKNOWN",
            Status::Healthy => "HEALTHY",
        }
    }

    pub(crate) fn color(&self) -> &'static str {
        match self {
            Status::Critical => "\x1b[31m",
            Status::Degraded => "\x1b[33m",
            Status::Unknown => "\x1b[90m",
            Status::Healthy => "\x1b[32m",
        }
    }
}

pub(crate) struct NodeView {
    /// Pre-formatted display token: "db002" or "db002@sto1", no sigils.
    pub display: String,
    /// Timeline ID; None if unknown or timelines have converged (no sigil needed).
    pub timeline: Option<i32>,
}

impl NodeView {
    pub(crate) fn render(&self, mode: RenderMode) -> String {
        match (mode, self.timeline) {
            (RenderMode::WithSigils, Some(tl)) => format!("{}{}", self.display, to_superscript(tl)),
            _ => self.display.clone(),
        }
    }
}

pub(crate) enum PrimaryView {
    None,
    Dash,
    Single(NodeView),
    SplitBrain {
        true_primary: NodeView,
        stale: Vec<NodeView>,
    },
}

impl PrimaryView {
    pub(crate) fn render(&self, mode: RenderMode) -> String {
        match self {
            PrimaryView::None => "(none)".to_string(),
            PrimaryView::Dash => "-".to_string(),
            PrimaryView::Single(node) => node.render(mode),
            PrimaryView::SplitBrain {
                true_primary,
                stale,
            } => {
                let stale_strs: Vec<String> = stale.iter().map(|n| n.render(mode)).collect();
                format!("{} vs {}", true_primary.render(mode), stale_strs.join(","))
            }
        }
    }
}

pub(crate) struct ReplicaView {
    pub node: NodeView,
    pub conn_count: usize,
    pub backup_lag: Option<String>,
}

impl ReplicaView {
    pub(crate) fn render(&self, mode: RenderMode) -> String {
        let node_str = self.node.render(mode);
        match (self.conn_count > 1, &self.backup_lag) {
            (true, Some(lag)) => format!("{}(×{}{})", node_str, self.conn_count, lag),
            (true, None) => format!("{}(×{})", node_str, self.conn_count),
            (false, Some(lag)) => format!("{}{}", node_str, lag),
            (false, None) => node_str,
        }
    }
}

pub(crate) enum ReplicasView {
    None,
    List(Vec<ReplicaView>),
    Unknown {
        reachable: u32,
    },
    /// (replica_node, true_primary_node) pairs for split-brain following display.
    SplitBrainFollowing(Vec<(NodeView, NodeView)>),
}

impl ReplicasView {
    pub(crate) fn render(&self, mode: RenderMode) -> String {
        match self {
            ReplicasView::None => "-".to_string(),
            ReplicasView::List(replicas) => {
                if replicas.is_empty() {
                    return "-".to_string();
                }
                replicas
                    .iter()
                    .map(|r| r.render(mode))
                    .collect::<Vec<_>>()
                    .join(",")
            }
            ReplicasView::Unknown { reachable } => format!("?/2 ({} reachable)", reachable),
            ReplicasView::SplitBrainFollowing(pairs) => {
                if pairs.is_empty() {
                    return "-".to_string();
                }
                pairs
                    .iter()
                    .map(|(replica, primary)| {
                        format!("{}→{}", replica.render(mode), primary.render(mode))
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            }
        }
    }
}

pub(crate) struct ReasonView {
    pub short: String,
    pub details_json: String,
}

pub(crate) struct ClusterView {
    pub status: Status,
    pub name: String,
    pub primary: PrimaryView,
    pub replicas: ReplicasView,
    pub lag_bytes: Option<u64>,
    pub disk: String,
    pub reason: ReasonView,
    pub failover: bool,
}

impl ClusterView {
    pub(crate) fn primary_content(&self, mode: RenderMode) -> String {
        let base = self.primary.render(mode);
        if self.failover {
            format!("{} (failover)", base)
        } else {
            base
        }
    }

    pub(crate) fn replicas_content(&self, mode: RenderMode) -> String {
        self.replicas.render(mode)
    }
}

impl Ord for ClusterView {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.status
            .cmp(&other.status)
            .then_with(|| self.name.cmp(&other.name))
    }
}

impl PartialOrd for ClusterView {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ClusterView {
    fn eq(&self, other: &Self) -> bool {
        self.status == other.status && self.name == other.name
    }
}

impl Eq for ClusterView {}
