//! Prometheus integration for backup progress tracking
//!
//! This module is only available when the `prometheus` feature is enabled.
//! It provides functionality to query Prometheus for disk metrics to estimate
//! pg_basebackup progress.

#[cfg(feature = "prometheus")]
pub mod client {
    use reqwest::Url;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    // Re-export ClientWithMiddleware for use in other modules
    use reqwest_middleware::ClientBuilder;
    pub use reqwest_middleware::ClientWithMiddleware;

    /// Prometheus URL - configure at compile time or use default (don't use default)
    const PROMETHEUS_URL: &str = match option_env!("PROMETHEUS_URL") {
        Some(url) => url,
        None => "https://prometheus.example.com",
    };

    #[derive(Debug, Serialize, Deserialize)]
    struct PrometheusResponse {
        status: String,
        data: PrometheusData,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct PrometheusData {
        #[serde(rename = "resultType")]
        result_type: String,
        result: Vec<PrometheusResult>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct PrometheusResult {
        metric: HashMap<String, String>,
        value: (f64, String), // [timestamp, value]
    }

    /// Create a default HTTP client for Prometheus queries
    ///
    /// Returns a client with a 5 second timeout. Use this to create a shared
    /// client when making multiple Prometheus queries.
    pub fn create_client() -> Result<ClientWithMiddleware, Box<dyn std::error::Error + Send + Sync>>
    {
        Ok(ClientBuilder::new(
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()?,
        )
        .build())
    }

    /// Get used bytes on a filesystem mountpoint
    /// Calculates: size_bytes - avail_bytes = used_bytes
    ///
    /// If client is None, creates a default client with 5s timeout.
    /// Pass a custom client for testing or custom configuration.
    #[tracing::instrument(skip(client), level = "debug")]
    pub async fn get_filesystem_used_bytes(
        hostname: &str,
        mountpoint: &str,
        client: Option<&ClientWithMiddleware>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Create default client if none provided
        let default_client;
        let client = match client {
            Some(c) => c,
            None => {
                default_client = create_client()?;
                &default_client
            }
        };

        // Get total size
        let size_query = format!(
            "node_filesystem_size_bytes{{host=\"{}\",mountpoint=\"{}\"}}",
            hostname, mountpoint
        );

        // Get available space
        let avail_query = format!(
            "node_filesystem_avail_bytes{{host=\"{}\",mountpoint=\"{}\"}}",
            hostname, mountpoint
        );

        let base_url = format!("{}/api/v1/query", PROMETHEUS_URL);

        // Query for size
        let size_url = Url::parse_with_params(&base_url, &[("query", &size_query)])?;
        let size_response: PrometheusResponse = client.get(size_url).send().await?.json().await?;

        tracing::debug!(
            status = %size_response.status,
            result_count = size_response.data.result.len(),
            "received size response from prometheus"
        );

        let size_bytes = size_response
            .data
            .result
            .first()
            .and_then(|r| r.value.1.parse::<u64>().ok())
            .ok_or("No size data from Prometheus")?;

        tracing::debug!(size_bytes = size_bytes, "parsed size_bytes");

        // Query for available
        let avail_url = Url::parse_with_params(&base_url, &[("query", &avail_query)])?;
        let avail_response: PrometheusResponse = client.get(avail_url).send().await?.json().await?;

        tracing::debug!(
            status = %avail_response.status,
            result_count = avail_response.data.result.len(),
            "received avail response from prometheus"
        );

        let avail_bytes = avail_response
            .data
            .result
            .first()
            .and_then(|r| r.value.1.parse::<u64>().ok())
            .ok_or("No avail data from Prometheus")?;

        tracing::debug!(avail_bytes = avail_bytes, "parsed avail_bytes");

        // Used = Size - Available
        let used_bytes = size_bytes.saturating_sub(avail_bytes);
        tracing::debug!(used_bytes = used_bytes, "calculated used_bytes");

        Ok(used_bytes)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rvcr::{VCRMiddleware, VCRMode};
        use std::path::PathBuf;

        #[tokio::test]
        async fn test_get_filesystem_used_bytes() {
            let client = initialize_test_client(
                "tests/prometheus/get_filesystem_used_bytes.vcr.json",
                VCRMode::Replay,
            );

            let result = get_filesystem_used_bytes(
                "prod-pg-app008-db003.sto3.example.com",
                "/var/lib/pgsql",
                Some(&client),
            )
            .await;

            assert!(
                result.is_ok(),
                "Failed to get filesystem used bytes: {:?}",
                result.err()
            );
            let used_bytes = result.unwrap();
            assert!(used_bytes > 0, "Used bytes should be greater than 0");

            // Expected: size (858553069568) - avail (442926485504) = 415626584064 bytes
            assert_eq!(used_bytes, 415626584064);
        }

        fn initialize_test_client(path: &str, mode: VCRMode) -> ClientWithMiddleware {
            let mut bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            bundle.push(path);

            let mw = VCRMiddleware::try_from(bundle.clone())
                .unwrap()
                .with_mode(mode);

            let client = reqwest::ClientBuilder::new()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap();

            ClientBuilder::new(client).with(mw).build()
        }
    }
}

#[cfg(not(feature = "prometheus"))]
pub mod client {
    /// Stub implementation when prometheus feature is disabled
    #[allow(dead_code)]
    pub async fn get_filesystem_used_bytes(
        _hostname: &str,
        _mountpoint: &str,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        Err("Prometheus support not enabled at compile time".into())
    }
}

/// Estimate pg_basebackup progress by comparing primary DB size vs replica filesystem usage
/// Returns progress percentage (0.0 - 100.0+)
///
/// This is a rough estimate assuming the used bytes on the replica are mostly from the backup.
/// This may be inaccurate if there's other data on the filesystem.
#[allow(dead_code)]
pub fn estimate_backup_progress(replica_used_bytes: u64, primary_db_size: u64) -> f64 {
    if primary_db_size > 0 {
        (replica_used_bytes as f64 / primary_db_size as f64) * 100.0
    } else {
        0.0
    }
}
