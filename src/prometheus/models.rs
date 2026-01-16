use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusResponse {
    status: String,
    data: PrometheusData,
}

impl PrometheusResponse {
    #[tracing::instrument(level = "debug", skip(self), fields(
        result_type = %self.data.result_type,
        result_len = self.data.result.len(),
    ))]
    pub fn get_metrics(self) -> Vec<PrometheusResult> {
        tracing::debug!("extracting prometheus metrics");
        self.data.result
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PrometheusData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PrometheusResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusResult {
    metric: HashMap<String, String>,
    value: (f64, String), // [timestamp, value]
}

impl PrometheusResult {
    /// Gets the ip address (instance key) and the bytes value from the prometheus
    /// result
    pub fn get_bytes(&self) -> Option<(String, u64)> {
        let value: u64 = match self.value.1.parse() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    raw_value = %self.value.1,
                    error = %e,
                    instance = ?self.metric.get("instance"),
                    "failed to parse prometheus metric value as u64"
                );
                return None;
            }
        };

        let instance = self.metric.get("instance")?;
        let ip = instance.split(':').next()?;

        if ip.is_empty() {
            tracing::warn!(
                instance = %instance,
                "extracted empty IP from instance field"
            );
            return None;
        }

        Some((ip.to_string(), value))
    }
}

pub enum Query {
    SizeBytes,
    AvailBytes,
}

impl Query {
    /// Creates a prometheus PromQL query with the given host
    pub fn with_host(&self, host: &str) -> String {
        match self {
            Query::SizeBytes => {
                format!(
                    "node_filesystem_size_bytes{{host=~\"{host}\",mountpoint=\"/var/lib/pgsql\",fstype!=\"rootfs\"}}"
                )
            }
            Query::AvailBytes => {
                format!(
                    "node_filesystem_avail_bytes{{host=~\"{host}\",mountpoint=\"/var/lib/pgsql\",fstype!=\"rootfs\"}}"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_report_prometheus_result_ok() {
        let mut metric = HashMap::new();
        metric.insert("instance".to_string(), "127.3.18.28:9999".to_string());

        // (Timestamp, Value)
        let value = (2000f64, "415626584064".to_string());
        let res = PrometheusResult { value, metric };

        let actual = res.get_bytes();
        let expected = Some(("127.3.18.28".to_string(), 415626584064u64));

        assert_eq!(actual, expected)
    }

    #[test]
    fn test_report_prometheus_missing() {
        let mut metric = HashMap::new();
        metric.insert(
            "not_instance_key".to_string(),
            "127.3.18.28:9999".to_string(),
        );

        let value = (2000f64, "415626584064".to_string());
        let res = PrometheusResult { value, metric };

        let actual = res.get_bytes();
        let expected = None;

        assert_eq!(actual, expected)
    }
}
