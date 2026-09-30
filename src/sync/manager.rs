//! Sync manager for bidirectional sync with Google Reader API servers.

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::redundant_closure_for_method_calls)]

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use color_eyre::Result;
use tracing::{debug, info};

use crate::config::{Config, FeedConfig, FolderConfig};
use crate::feed::{CachedItem, FeedCache};
use crate::sync::{AuthToken, GReaderClient, StreamItem, StreamOptions};

/// Match a server item to the locally cached RSS item.
///
/// `FreshRSS` may expose a canonical URL that differs from the URL in the RSS
/// feed, so first compare every canonical/alternate URL and then fall back to
/// title plus publication time.
fn find_local_item<'a>(
    items: &'a [CachedItem],
    server_item: &StreamItem,
) -> Option<&'a CachedItem> {
    if let Some(item) = items
        .iter()
        .find(|item| item.sync_id.as_deref() == Some(server_item.id.as_str()))
    {
        return Some(item);
    }

    for link in server_item.links() {
        let id = CachedItem::generate_id(Some(link), server_item.title.as_deref().unwrap_or(""));
        if let Some(item) = items.iter().find(|item| item.id == id) {
            return Some(item);
        }
    }

    let title = server_item.title.as_deref()?.trim();
    let mut title_matches = items.iter().filter(|item| item.title.trim() == title);
    let first = title_matches.next()?;
    let Some(second) = title_matches.next() else {
        return Some(first);
    };

    // Duplicate titles are uncommon within one feed. If present, use the
    // closest publication time to disambiguate, otherwise retain the first.
    let published = server_item.published_at()?;
    std::iter::once(first)
        .chain(std::iter::once(second))
        .chain(title_matches)
        .filter_map(|item| {
            item.published
                .map(|local| ((local.timestamp() - published.timestamp()).abs(), item))
        })
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, item)| item)
}

/// Result of a sync operation.
#[derive(Debug, Default)]
pub struct SyncResult {
    /// Number of feeds imported from server.
    pub feeds_imported: usize,
    /// Number of feeds already present locally.
    pub feeds_existing: usize,
    /// Number of items marked as read locally (from server).
    pub items_marked_read: usize,
    /// Number of items marked as read on server (from local).
    pub items_synced_to_server: usize,
    /// Errors encountered (non-fatal).
    pub errors: Vec<String>,
}

/// Sync manager for bidirectional sync.
pub struct SyncManager {
    client: GReaderClient,
    auth: AuthToken,
}

impl SyncManager {
    /// Create a new sync manager.
    pub async fn connect(server: &str, username: &str, password: &str) -> Result<Self> {
        let client = GReaderClient::new(server);
        let auth = client.login(username, password).await?;
        Ok(Self { client, auth })
    }

    /// Get a reference to the client.
    pub fn client(&self) -> &GReaderClient {
        &self.client
    }

    /// Get a reference to the auth token.
    pub fn auth(&self) -> &AuthToken {
        &self.auth
    }

    /// Import subscriptions from server to local config.
    pub async fn import_subscriptions(&self, config: &mut Config) -> Result<SyncResult> {
        let mut result = SyncResult::default();

        let subs = self.client.subscriptions(&self.auth).await?;
        info!("Fetched {} subscriptions from server", subs.len());

        // Get existing feed URLs
        let existing_urls: HashSet<String> = config
            .folders
            .iter()
            .flat_map(|f| f.feeds.iter().map(|feed| feed.url.clone()))
            .chain(config.feeds.iter().map(|f| f.url.clone()))
            .collect();

        // Group subscriptions by category
        let mut by_category: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
        let mut root_feeds: Vec<(String, String, String)> = Vec::new();

        for sub in &subs {
            let feed_url = sub.url.clone();
            let feed_name = sub.title.clone();
            let sync_id = sub.id.clone();

            if existing_urls.contains(&feed_url) {
                // Update sync_id for existing feeds
                for folder in &mut config.folders {
                    for feed in &mut folder.feeds {
                        if feed.url == feed_url && feed.sync_id.is_none() {
                            feed.sync_id = Some(sync_id.clone());
                        }
                    }
                }
                for feed in &mut config.feeds {
                    if feed.url == feed_url && feed.sync_id.is_none() {
                        feed.sync_id = Some(sync_id.clone());
                    }
                }
                result.feeds_existing += 1;
                continue;
            }

            if let Some(cat) = sub.categories.first() {
                by_category
                    .entry(cat.label.clone())
                    .or_default()
                    .push((feed_url, feed_name, sync_id));
            } else {
                root_feeds.push((feed_url, feed_name, sync_id));
            }
        }

        // Add to folders
        for (category, feeds) in by_category {
            // Find or create folder
            let folder = config
                .folders
                .iter_mut()
                .find(|f| f.name.eq_ignore_ascii_case(&category));

            if let Some(folder) = folder {
                for (url, name, sync_id) in feeds {
                    folder.feeds.push(FeedConfig {
                        name,
                        url,
                        sync_id: Some(sync_id),
                    });
                    result.feeds_imported += 1;
                }
            } else {
                let new_feeds: Vec<FeedConfig> = feeds
                    .into_iter()
                    .map(|(url, name, sync_id)| {
                        result.feeds_imported += 1;
                        FeedConfig {
                            name,
                            url,
                            sync_id: Some(sync_id),
                        }
                    })
                    .collect();

                config.folders.push(FolderConfig {
                    name: category,
                    icon: None,
                    expanded: true,
                    feeds: new_feeds,
                });
            }
        }

        // Add root feeds
        for (url, name, sync_id) in root_feeds {
            config.feeds.push(FeedConfig {
                name,
                url,
                sync_id: Some(sync_id),
            });
            result.feeds_imported += 1;
        }

        info!(
            "Imported {} feeds, {} already existed",
            result.feeds_imported, result.feeds_existing
        );
        Ok(result)
    }

    /// Sync read states from server to local cache.
    pub async fn sync_read_states_from_server(&self, cache: &mut FeedCache) -> Result<SyncResult> {
        let mut result = SyncResult::default();
        let pending_states: HashMap<String, bool> =
            cache.pending_read_states()?.into_iter().collect();

        // Get subscriptions to map feed IDs to URLs
        let subs = self.client.subscriptions(&self.auth).await?;

        // For each subscription, fetch items and check read status
        for sub in &subs {
            // Fetch items with their read status
            let items = match self
                .client
                .stream_contents(&self.auth, &sub.id, Some(StreamOptions::with_count(100)))
                .await
            {
                Ok(items) => items,
                Err(e) => {
                    result
                        .errors
                        .push(format!("Failed to fetch {}: {}", sub.title, e));
                    continue;
                }
            };

            // FreshRSS may include the read category on every item returned by
            // an unfiltered stream. Its `xt=read` filtered stream is the
            // authoritative source for unread membership.
            let unread_items = match self
                .client
                .stream_contents(
                    &self.auth,
                    &sub.id,
                    Some(StreamOptions::unread_with_count(100)),
                )
                .await
            {
                Ok(items) => items,
                Err(error) => {
                    result.errors.push(format!(
                        "Failed to fetch unread status for {}: {error}",
                        sub.title
                    ));
                    continue;
                }
            };
            let unread_ids: HashSet<&str> = unread_items
                .items
                .iter()
                .map(|item| item.id.as_str())
                .collect();

            // Keep every unread item in the local 100-item window, then fill
            // remaining slots with the newest read items.
            let mut account_items: Vec<&StreamItem> = unread_items.items.iter().collect();
            let remaining = 100usize.saturating_sub(account_items.len());
            account_items.extend(
                items
                    .items
                    .iter()
                    .filter(|item| !unread_ids.contains(item.id.as_str()))
                    .take(remaining),
            );

            let existing = cache
                .get(&sub.url)
                .map(|feed| feed.items.clone())
                .unwrap_or_default();
            let mut synced_items = Vec::with_capacity(account_items.len());

            for item in account_items {
                let local_match = find_local_item(&existing, item);
                let is_read_on_server = !unread_ids.contains(item.id.as_str());

                if is_read_on_server && local_match.is_some_and(|local| !local.read) {
                    result.items_marked_read += 1;
                }

                let title = item.title.clone().unwrap_or_else(|| "Untitled".to_string());
                let link = local_match
                    .and_then(|local| local.link.clone())
                    .or_else(|| item.links().next().map(str::to_string));
                synced_items.push(CachedItem {
                    // Reader API IDs are stable and unique within the account.
                    // URLs are not: multiple articles may intentionally share
                    // one canonical URL, so hashing links can violate the
                    // database primary key.
                    id: item.id.clone(),
                    sync_id: Some(item.id.clone()),
                    title,
                    link,
                    published: local_match
                        .and_then(|local| local.published)
                        .or_else(|| item.published_at()),
                    summary: item.get_content().map(str::to_string),
                    // A pending local action wins until acknowledged. Without
                    // one, the account server is authoritative.
                    read: pending_states
                        .get(&item.id)
                        .copied()
                        .unwrap_or(is_read_on_server),
                    cached_at: local_match.map_or_else(Utc::now, |local| local.cached_at),
                });
            }

            debug!(
                "Account refresh for {}: {} articles, {} unread after local overlay",
                sub.title,
                synced_items.len(),
                synced_items.iter().filter(|item| !item.read).count()
            );
            cache.replace_synced_feed(&sub.url, &sub.title, synced_items);
        }

        info!(
            "Marked {} items as read from server",
            result.items_marked_read
        );
        Ok(result)
    }

    /// Sync local read states to server.
    pub async fn sync_read_states_to_server(&self, cache: &mut FeedCache) -> Result<SyncResult> {
        let mut result = SyncResult::default();

        // Send the durable status queue first. This makes local user actions
        // authoritative until the server has acknowledged them.
        let pending = cache.pending_read_states()?;
        for read in [true, false] {
            let ids: Vec<String> = pending
                .iter()
                .filter(|(_, state)| *state == read)
                .map(|(id, _)| id.clone())
                .collect();
            if ids.is_empty() {
                continue;
            }
            let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            let upload = if read {
                self.client.mark_read(&self.auth, &refs).await
            } else {
                self.client.mark_unread(&self.auth, &refs).await
            };
            match upload {
                Ok(()) => {
                    cache.acknowledge_read_states(&ids)?;
                    result.items_synced_to_server += ids.len();
                }
                Err(error) => result
                    .errors
                    .push(format!("Failed to upload queued read states: {error}")),
            }
        }

        info!("Synced {} items to server", result.items_synced_to_server);
        Ok(result)
    }

    /// Full bidirectional sync.
    pub async fn full_sync(
        &self,
        config: &mut Config,
        cache: &mut FeedCache,
    ) -> Result<SyncResult> {
        let mut result = SyncResult::default();

        // 1. Import subscriptions from server
        info!("Step 1: Importing subscriptions from server...");
        let import_result = self.import_subscriptions(config).await?;
        result.feeds_imported = import_result.feeds_imported;
        result.feeds_existing = import_result.feeds_existing;
        result.errors.extend(import_result.errors);

        // 2. Download articles while overlaying durable local status changes.
        // Reader API servers may briefly return stale state after a write, so
        // pending local changes must be applied before they are acknowledged.
        info!("Step 2: Syncing articles and read states from server...");
        let from_server = self.sync_read_states_from_server(cache).await?;
        result.items_marked_read = from_server.items_marked_read;
        result.errors.extend(from_server.errors);

        // 3. Upload and acknowledge the durable local status queue.
        info!("Step 3: Syncing read states to server...");
        let to_server = self.sync_read_states_to_server(cache).await?;
        result.items_synced_to_server = to_server.items_synced_to_server;
        result.errors.extend(to_server.errors);

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    use super::find_local_item;
    use crate::feed::CachedItem;
    use crate::sync::StreamItem;

    fn cached_item(title: &str, link: &str, published: i64) -> CachedItem {
        CachedItem {
            id: CachedItem::generate_id(Some(link), title),
            sync_id: None,
            title: title.to_string(),
            link: Some(link.to_string()),
            published: Utc.timestamp_opt(published, 0).single(),
            summary: None,
            read: true,
            cached_at: Utc::now(),
        }
    }

    #[test]
    fn matches_any_server_link() {
        let local = cached_item("Article", "https://example.com/from-feed", 100);
        let server: StreamItem = serde_json::from_value(json!({
            "id": "server-id",
            "title": "Article",
            "published": 100,
            "canonical": [{"href": "https://example.com/canonical"}],
            "alternate": [{"href": "https://example.com/from-feed"}]
        }))
        .unwrap();

        assert_eq!(find_local_item(&[local], &server).unwrap().title, "Article");
    }

    #[test]
    fn unique_title_matches_despite_different_timestamp() {
        let local = cached_item("Article", "https://feed.example/article", 100);
        let server: StreamItem = serde_json::from_value(json!({
            "id": "server-id",
            "title": "Article",
            "published": 999,
            "canonical": [{"href": "https://canonical.example/article"}]
        }))
        .unwrap();

        assert_eq!(find_local_item(&[local], &server).unwrap().title, "Article");
    }

    #[test]
    fn matches_authoritative_server_id_before_url() {
        let mut local = cached_item("Old title", "https://example.com/shared", 100);
        local.sync_id = Some("server-id".to_string());
        let server: StreamItem = serde_json::from_value(json!({
            "id": "server-id",
            "title": "New title",
            "published": 999,
            "canonical": [{"href": "https://example.com/different"}]
        }))
        .unwrap();

        assert_eq!(
            find_local_item(&[local], &server).unwrap().title,
            "Old title"
        );
    }
}
