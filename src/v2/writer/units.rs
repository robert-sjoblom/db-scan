/// Parse PostgreSQL interval lag to estimated bytes.
/// Used for backup operations where time-based lag is more accurate than LSN diff.
pub fn parse_lag_to_bytes(lag: &str) -> Option<u64> {
    // Format: HH:MM:SS.microseconds
    let parts: Vec<&str> = lag.split(':').collect();
    if parts.len() != 3 {
        return None;
    }

    let hours: u64 = parts[0].parse().ok()?;
    let minutes: u64 = parts[1].parse().ok()?;
    let seconds_parts: Vec<&str> = parts[2].split('.').collect();
    let seconds: u64 = seconds_parts[0].parse().ok()?;

    let total_seconds = hours * 3600 + minutes * 60 + seconds;
    Some(total_seconds * 16_000_000)
}

/// Maps a non-negative integer to its Unicode superscript representation.
pub(crate) fn to_superscript(n: i32) -> String {
    const DIGITS: [char; 10] = ['⁰', '¹', '²', '³', '⁴', '⁵', '⁶', '⁷', '⁸', '⁹'];
    if n < 0 {
        return String::new();
    }
    n.to_string()
        .chars()
        .map(|c| DIGITS[(c as u8 - b'0') as usize])
        .collect()
}

/// Display column width: counts Unicode scalar values (safe for ASCII + superscript digits).
pub(crate) fn display_width(s: &str) -> usize {
    s.chars().count()
}

/// Format lag in human-readable form.
pub(crate) fn format_lag(lag: Option<u64>) -> String {
    match lag {
        None => "-".to_string(),
        Some(0) => "0B".to_string(),
        Some(bytes) => {
            if bytes >= 1_000_000_000 {
                format!("{:.1}GB", bytes as f64 / 1_000_000_000.0)
            } else if bytes >= 1_000_000 {
                format!("{:.0}MB", bytes as f64 / 1_000_000.0)
            } else if bytes >= 1_000 {
                format!("{:.0}KB", bytes as f64 / 1_000.0)
            } else {
                format!("{}B", bytes)
            }
        }
    }
}

/// Format bytes in a human-readable format (KB, MB, GB).
pub(crate) fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}
