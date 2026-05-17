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
