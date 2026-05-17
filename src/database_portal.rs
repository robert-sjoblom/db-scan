use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{config::get_config, errors, v2::node::Node};

const ONE_DAY_IN_SECONDS: u64 = 86_400;
const CACHE_PATH: &str = "/tmp/nodes_response.json";

#[derive(Debug, Serialize, Deserialize)]
struct NodesResponse {
    pub items: Vec<serde_json::Value>,
    pub count: i32,
}

fn parse_nodes(items: Vec<serde_json::Value>) -> Vec<Node> {
    items
        .into_iter()
        .filter_map(|v| match serde_json::from_value::<Node>(v.clone()) {
            Ok(node) => Some(node),
            Err(e) => {
                tracing::warn!(error = %e, raw = %v, "skipping node that failed to deserialize");
                None
            }
        })
        .collect()
}

#[instrument(skip_all, level = "INFO")]
pub(crate) async fn nodes() -> anyhow::Result<Vec<Node>> {
    // Check if cache exists and is less than 1 day old
    let use_cache = std::fs::metadata(CACHE_PATH)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.elapsed().ok())
        .is_some_and(|e| e.as_secs() < ONE_DAY_IN_SECONDS);

    if use_cache && let Ok(json) = std::fs::read_to_string(CACHE_PATH) {
        tracing::info!(source = "file", "reading nodes from cache");
        let nodes_response: NodesResponse = serde_json::from_str(&json)
            .map_err(errors::serde_err)
            .with_context(|| format!("cache_path: {CACHE_PATH}"))
            .context("attempting: deserialize cached nodes response")?;
        tracing::info!(
            node_count = nodes_response.count,
            source = "file",
            "loaded nodes from cache"
        );
        return Ok(parse_nodes(nodes_response.items));
    }

    let url = &get_config().database_portal_url;
    tracing::info!(source = "api", url = %url, "fetching nodes from API");
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("url: {url}"))
        .context("attempting: GET database portal API")?;

    let nodes_response: NodesResponse = response
        .json()
        .await
        .with_context(|| format!("url: {url}"))
        .context("attempting: deserialize database portal response body")?;

    // Write the response to a json file in /tmp
    let json = serde_json::to_string(&nodes_response)
        .map_err(errors::serde_err)
        .context("attempting: serialize nodes response for cache")?;
    std::fs::write(CACHE_PATH, &json)
        .with_context(|| format!("cache_path: {CACHE_PATH}"))
        .context("attempting: write nodes cache file")?;
    tracing::info!(file = CACHE_PATH, "cached nodes to file");

    tracing::info!(
        node_count = nodes_response.count,
        source = "api",
        "fetched nodes from API"
    );
    Ok(parse_nodes(nodes_response.items))
}
