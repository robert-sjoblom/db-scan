#[derive(Debug)]
pub struct FileSystemMetrics {
    pub size_bytes: u64,
    pub used_bytes: u64,
}

#[cfg(feature = "prometheus")]
mod models;

#[cfg(feature = "prometheus")]
pub mod client {
    use std::collections::HashMap;

    use reqwest::Url;
    use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};

    use crate::prometheus::{
        FileSystemMetrics,
        models::{PrometheusResponse, Query},
    };

    const PROMETHEUS_URL: &str = match option_env!("PROMETHEUS_URL") {
        Some(url) => url,
        None => "https://prometheus.example.com",
    };

    pub async fn get_batch_filesystem_data(
        hostname: impl AsRef<str>,
    ) -> HashMap<String, FileSystemMetrics> {
        let http_client = match create_client() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "failed to create prometheus client, backup progress disabled");
                return HashMap::new();
            }
        };

        _get_batch_filesystem_data(hostname.as_ref(), &http_client)
            .await
            .inspect_err(
                |e| tracing::warn!(error = %e, "failed to fetch batch data from prometheus"),
            )
            .unwrap_or_default()
    }

    type Ip = String;
    type Bytes = u64;

    #[tracing::instrument(level = "info", skip(client), fields(hostname = %hostname))]
    async fn _get_batch_filesystem_data(
        hostname: &str,
        client: &ClientWithMiddleware,
    ) -> anyhow::Result<HashMap<String, FileSystemMetrics>> {
        let (size_result, avail_result) = tokio::join!(
            client.get(build_query(hostname, Query::SizeBytes)?).send(),
            client.get(build_query(hostname, Query::AvailBytes)?).send()
        );

        let size_result: PrometheusResponse = size_result?.json().await?;
        let avail_result: PrometheusResponse = avail_result?.json().await?;

        let size_bytes_map: HashMap<Ip, Bytes> = size_result
            .get_metrics()
            .into_iter()
            .filter_map(|r| r.get_bytes())
            .collect();

        let avail_bytes_map: HashMap<Ip, Bytes> = avail_result
            .get_metrics()
            .into_iter()
            .filter_map(|r| r.get_bytes())
            .collect();

        tracing::debug!(
            size_count = size_bytes_map.len(),
            avail_count = avail_bytes_map.len(),
            "collected filesystem metrics from prometheus"
        );

        let size_count = size_bytes_map.len();
        let avail_count = avail_bytes_map.len();

        let map: HashMap<Ip, FileSystemMetrics> = size_bytes_map
            .into_iter()
            .filter_map(|(ip, size_bytes)| {
                let Some(avail_bytes) = avail_bytes_map.get(&ip) else {
                    tracing::warn!(ip = %ip, "missing avail_bytes for host");
                    return None;
                };

                Some((
                    ip,
                    FileSystemMetrics {
                        size_bytes,
                        used_bytes: size_bytes - avail_bytes,
                    },
                ))
            })
            .collect();

        let matched_count = map.len();
        let dropped_count = size_count.max(avail_count) - matched_count;

        if dropped_count > 0 {
            tracing::warn!(
                size_only = size_count.saturating_sub(matched_count),
                avail_only = avail_count.saturating_sub(matched_count),
                matched = matched_count,
                dropped = dropped_count,
                "some hosts had incomplete prometheus metrics"
            );
        }

        Ok(map)
    }

    /// Create a default HTTP client for Prometheus queries
    ///
    /// Returns a client with a 5 second timeout. Use this to create a shared
    /// client when making multiple Prometheus queries.
    fn create_client() -> anyhow::Result<ClientWithMiddleware> {
        Ok(ClientBuilder::new(
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()?,
        )
        .build())
    }

    fn build_query(host: &str, query: Query) -> anyhow::Result<Url> {
        let base_url = format!("{}/api/v1/query", PROMETHEUS_URL);
        Ok(Url::parse_with_params(
            &base_url,
            &[("query", query.with_host(host))],
        )?)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rvcr::{VCRMiddleware, VCRMode, VCRReplaySearch};
        use std::path::PathBuf;

        #[test]
        fn test_build_size_query() {
            let url = build_query("prod-pg-app001", Query::AvailBytes).unwrap();

            assert_eq!(url.scheme(), "https");
            assert_eq!(url.host_str(), Some("prometheus.example.com"));
            assert_eq!(url.path(), "/api/v1/query");

            let query_param = url.query_pairs().find(|(k, _)| k == "query");
            assert!(query_param.is_some(), "query parameter not found");

            let promql = query_param.unwrap().1;
            assert!(
                promql.contains("node_filesystem_avail_bytes"),
                "missing metric name"
            );
            assert!(
                promql.contains("host=~\"prod-pg-app001\""),
                "missing host filter"
            );
            assert!(
                promql.contains("mountpoint=\"/var/lib/pgsql\""),
                "missing mountpoint"
            );
        }

        #[tokio::test]
        async fn test_get_batch_filesystem_data() {
            let client = initialize_test_client(
                "tests/prometheus/dev_pg_app001_filesystem.vcr.json",
                VCRMode::Replay,
            );

            let result = _get_batch_filesystem_data("dev-pg-app001.*", &client).await;

            assert!(
                result.is_ok(),
                "Failed to get batch filesystem data: {:?}",
                result.err()
            );

            let filesystem_map = result.unwrap();

            // Should have 3 hosts from the dev-pg-app001 cluster
            assert_eq!(filesystem_map.len(), 3);

            // Verify expected IPs are present
            assert!(filesystem_map.contains_key("127.3.12.151"));
            assert!(filesystem_map.contains_key("127.2.12.151"));
            assert!(filesystem_map.contains_key("127.1.12.151"));

            // Verify each entry has reasonable values
            for (ip, metrics) in &filesystem_map {
                assert!(
                    metrics.size_bytes > 200_000_000_000 && metrics.size_bytes < 220_000_000_000,
                    "Unexpected size_bytes for {}: {}",
                    ip,
                    metrics.size_bytes
                );
                assert!(
                    metrics.used_bytes > 80_000_000_000 && metrics.used_bytes < 90_000_000_000,
                    "Unexpected used_bytes for {}: {}",
                    ip,
                    metrics.used_bytes
                );
                assert!(
                    metrics.used_bytes < metrics.size_bytes,
                    "Used bytes should not exceed size bytes for {}",
                    ip
                );
            }
        }

        fn initialize_test_client(path: &str, mode: VCRMode) -> ClientWithMiddleware {
            let mut bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            bundle.push(path);

            let mw = VCRMiddleware::try_from(bundle.clone())
                .unwrap()
                .with_mode(mode)
                .with_search(VCRReplaySearch::SearchAll);

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
    use std::collections::HashMap;

    use crate::prometheus::FileSystemMetrics;

    /// Stub implementation of function when prometheus feature is disabled
    pub async fn get_batch_filesystem_data(
        _hostname: impl AsRef<str>,
    ) -> HashMap<String, FileSystemMetrics> {
        HashMap::new()
    }
}
