/// Parse `PostgreSQL` interval lag to estimated bytes.
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

const SUPERSCRIPT_DIGITS: [char; 10] = [
    '\u{2070}', '\u{b9}', '\u{b2}', '\u{b3}', '\u{2074}', '\u{2075}', '\u{2076}', '\u{2077}',
    '\u{2078}', '\u{2079}',
];

/// Maps a non-negative integer to its Unicode superscript representation.
pub(crate) fn to_superscript(n: i32) -> String {
    if n < 0 {
        return String::new();
    }
    n.to_string()
        .chars()
        .map(|c| SUPERSCRIPT_DIGITS[(c as u8 - b'0') as usize])
        .collect()
}

pub(crate) fn contains_superscript_digit(s: &str) -> bool {
    s.chars().any(|c| SUPERSCRIPT_DIGITS.contains(&c))
}

/// Display column width: counts Unicode scalar values (safe for ASCII + superscript digits).
pub(crate) fn display_width(s: &str) -> usize {
    s.chars().count()
}

/// Format lag in human-readable form.
pub(crate) fn format_lag(lag: Option<u64>) -> String {
    match lag {
        None => "-".to_owned(),
        Some(0) => "0B".to_owned(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_lag_to_bytes_zero_lag() {
        assert_eq!(parse_lag_to_bytes("00:00:00.001234"), Some(0));
    }

    #[test]
    fn parse_lag_to_bytes_one_second() {
        // 1 second * 16MB/s = 16,000,000 bytes
        assert_eq!(parse_lag_to_bytes("00:00:01.000000"), Some(16_000_000));
    }

    #[test]
    fn parse_lag_to_bytes_one_minute() {
        // 60 seconds * 16MB/s = 960,000,000 bytes
        assert_eq!(parse_lag_to_bytes("00:01:00.000000"), Some(960_000_000));
    }

    #[test]
    fn parse_lag_to_bytes_complex() {
        // 1h 30m 45s = 5445 seconds * 16MB/s = 87,120,000,000 bytes
        assert_eq!(parse_lag_to_bytes("01:30:45.123456"), Some(87_120_000_000));
    }

    #[test]
    fn parse_lag_to_bytes_invalid_format() {
        assert_eq!(parse_lag_to_bytes("invalid"), None);
        assert_eq!(parse_lag_to_bytes("00:00"), None);
        assert_eq!(parse_lag_to_bytes(""), None);
    }
}
