use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum Stage {
    Prometheus,
    DatabasePortal,
    Scan,
    Clustering,
    Analyze,
    Write,
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Stage::Prometheus => write!(f, "Prometheus"),
            Stage::DatabasePortal => write!(f, "Node Discovery"),
            Stage::Scan => write!(f, "Scan"),
            Stage::Analyze => write!(f, "Analysis"),
            Stage::Write => write!(f, "Output"),
            Stage::Clustering => write!(f, "Clustering"),
        }
    }
}

pub enum Event {
    Start(Stage),
    End(Stage),
    Complete,
}

#[derive(Debug)]
struct TimingRecord {
    start: Instant,
    stop: Option<Instant>,
}

pub async fn reporter(mut rx: UnboundedReceiver<Event>) -> Option<String> {
    let mut timings = HashMap::new();

    while let Some(event) = rx.recv().await {
        match event {
            Event::Start(t) => {
                timings.insert(
                    t,
                    TimingRecord {
                        start: Instant::now(),
                        stop: None,
                    },
                );
            }
            Event::End(t) => {
                if let Some(timing) = timings.get_mut(&t) {
                    timing.stop = Some(Instant::now());
                } else {
                    tracing::warn!(timing = ?t, "received end event for unknown timing")
                }
            }
            Event::Complete => break,
        }
    }

    let durations: HashMap<Stage, Duration> = timings
        .into_iter()
        .filter_map(|(k, v)| match v.stop {
            Some(stop) => Some((k, stop.duration_since(v.start))),
            None => {
                tracing::warn!(timing = ?k, "incomplete timing: received start without end");
                None
            }
        })
        .collect();

    format_timings(&durations)
}

fn format_duration(duration: &Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs >= 60.0 {
        format!("{:>5.1} m", secs / 60.0)
    } else if secs >= 1.0 {
        format!("{:>5.2} s", secs)
    } else {
        format!("{:>5.0} ms", secs * 1000.0)
    }
}

fn format_timings(durations: &HashMap<Stage, Duration>) -> Option<String> {
    if durations.is_empty() {
        return None;
    }

    let mut sorted: Vec<_> = durations.iter().collect();
    sorted.sort_by_key(|(k, _)| *k);

    let mut output = String::new();
    let mut total = Duration::ZERO;

    for (timing, duration) in sorted {
        let label = format!("{timing}");
        let formatted_duration = format_duration(duration);
        output.push_str(&format!("{:<14} {}\n", label, formatted_duration));
        total += *duration;
    }

    output.push_str("────────────────────────\n");
    output.push_str(&format!("{:<14} {}\n", "Total", format_duration(&total)));

    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn format_duration_milliseconds() {
        assert_eq!(format_duration(&Duration::from_millis(123)), "  123 ms");
        assert_eq!(format_duration(&Duration::from_millis(5)), "    5 ms");
    }

    #[test]
    fn format_duration_seconds() {
        assert_eq!(format_duration(&Duration::from_secs_f64(1.5)), " 1.50 s");
        assert_eq!(format_duration(&Duration::from_secs(30)), "30.00 s");
    }

    #[test]
    fn format_duration_minutes() {
        assert_eq!(format_duration(&Duration::from_secs(90)), "  1.5 m");
        assert_eq!(format_duration(&Duration::from_secs(300)), "  5.0 m");
    }

    #[test]
    fn format_timings_aligns_columns_and_includes_total() {
        let mut durations = HashMap::new();
        durations.insert(Stage::Prometheus, Duration::from_millis(123));
        durations.insert(Stage::Scan, Duration::from_millis(5));

        let output = format_timings(&durations).unwrap();

        assert_eq!(
            output,
            "Prometheus       123 ms\n\
             Scan               5 ms\n\
             ────────────────────────\n\
             Total            128 ms\n"
        );
    }

    #[tokio::test]
    async fn reporter_formats_single_timing() {
        let (tx, rx) = mpsc::unbounded_channel();

        tx.send(Event::Start(Stage::Scan)).unwrap();
        tx.send(Event::End(Stage::Scan)).unwrap();
        tx.send(Event::Complete).unwrap();

        let output = reporter(rx).await.unwrap();

        assert!(output.contains("Scan"));
        assert!(output.contains("ms"));
    }

    #[tokio::test]
    async fn reporter_outputs_in_declaration_order_not_arrival_order() {
        let (tx, rx) = mpsc::unbounded_channel();

        // Send in reverse order
        tx.send(Event::Start(Stage::Write)).unwrap();
        tx.send(Event::End(Stage::Write)).unwrap();
        tx.send(Event::Start(Stage::Prometheus)).unwrap();
        tx.send(Event::End(Stage::Prometheus)).unwrap();
        tx.send(Event::Complete).unwrap();

        let output = reporter(rx).await.unwrap();

        let prometheus_pos = output.find("Prometheus").unwrap();
        let output_pos = output.find("Output").unwrap();
        assert!(
            prometheus_pos < output_pos,
            "Prometheus should appear before Output in output"
        );
    }

    #[tokio::test]
    async fn reporter_handles_empty_timings() {
        let (tx, rx) = mpsc::unbounded_channel();
        tx.send(Event::Complete).unwrap();

        let output = reporter(rx).await;

        assert!(output.is_none());
    }
}
