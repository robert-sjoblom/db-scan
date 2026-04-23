use std::io::IsTerminal;

use super::{
    WriterOptions,
    units::{display_width, format_lag},
    view::{ClusterView, RESET, RenderMode},
};

pub(crate) fn render_table(views: &[ClusterView], options: &WriterOptions) -> String {
    if views.is_empty() {
        return "No clusters to display.".to_string();
    }

    let use_color = !options.no_color && std::io::stdout().is_terminal();

    let primary_strs: Vec<String> = views
        .iter()
        .map(|v| v.primary_content(RenderMode::WithSigils))
        .collect();
    let replicas_strs: Vec<String> = views
        .iter()
        .map(|v| v.replicas_content(RenderMode::WithSigils))
        .collect();

    let has_disk_info = views.iter().any(|v| v.disk != "-");
    let has_sigils = primary_strs
        .iter()
        .chain(replicas_strs.iter())
        .any(|s| s.chars().any(|c| "⁰¹²³⁴⁵⁶⁷⁸⁹".contains(c)));

    let mut max_cluster = "CLUSTER".len();
    let mut max_primary = "PRIMARY".len();
    let mut max_replicas = "REPLICAS".len();
    let mut max_lag = "LAG".len();
    let mut max_disk = "DISK".len();
    let mut max_reason = "REASON".len();

    for (i, view) in views.iter().enumerate() {
        max_cluster = max_cluster.max(view.name.len());
        max_primary = max_primary.max(display_width(&primary_strs[i]));
        max_replicas = max_replicas.max(display_width(&replicas_strs[i]));
        max_lag = max_lag.max(format_lag(view.lag_bytes).len());
        max_disk = max_disk.max(view.disk.len());
        max_reason = max_reason.max(view.reason.short.len());
    }

    let mut output = String::new();

    output.push_str(&format!(
        "{:<8} {:<width_cluster$} {:<width_primary$} {:<width_replicas$} {:<width_lag$} {:<width_disk$} {}\n",
        "STATUS",
        "CLUSTER",
        "PRIMARY",
        "REPLICAS",
        "LAG",
        "DISK",
        "REASON",
        width_cluster = max_cluster,
        width_primary = max_primary,
        width_replicas = max_replicas,
        width_lag = max_lag,
        width_disk = max_disk,
    ));

    for (i, view) in views.iter().enumerate() {
        let status_str = if use_color {
            format!("{}{}{}", view.status.color(), view.status.as_str(), RESET)
        } else {
            view.status.as_str().to_string()
        };

        let status_padding = if use_color {
            8 + view.status.color().len() + RESET.len()
        } else {
            8
        };

        let primary_str = &primary_strs[i];
        let replicas_str = &replicas_strs[i];

        // Pad primary and replicas by display width, not byte length, to handle superscripts.
        let primary_pad = max_primary + primary_str.len() - display_width(primary_str);
        let replicas_pad = max_replicas + replicas_str.len() - display_width(replicas_str);

        output.push_str(&format!(
            "{:<status_padding$} {:<width_cluster$} {:<primary_pad$} {:<replicas_pad$} {:<width_lag$} {:<width_disk$} {}\n",
            status_str,
            view.name,
            primary_str,
            replicas_str,
            format_lag(view.lag_bytes),
            view.disk,
            view.reason.short,
            status_padding = status_padding,
            width_cluster = max_cluster,
            primary_pad = primary_pad,
            replicas_pad = replicas_pad,
            width_lag = max_lag,
            width_disk = max_disk,
        ));
    }

    if has_sigils || has_disk_info {
        output.push('\n');
    }
    if has_sigils {
        output.push_str("⁷ = timeline id\n");
    }
    if has_disk_info {
        output.push_str("DISK: io=I/O errors, fs=filesystem errors, blk=block device errors\n");
    }

    output
}
