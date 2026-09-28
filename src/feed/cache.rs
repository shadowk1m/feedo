//! Offline cache for feed articles.
//!
//! This module provides persistent storage for feed data,
//! allowing the app to work offline and preserve read states.

use std::{collections::HashMap, fs, path::PathBuf, sync::Mutex};

use chrono::{DateTime, Utc};
use color_eyre::Result;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::config::Config;

const MAX_ITEMS_PER_FEED: usize = 100;

/// Cached feed data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedFeed {
    /// Feed URL (used as key).
    pub url: String,

    /// Feed name.
    pub name: String,

    /// Cached items.
    pub items: Vec<CachedItem>,

    /// Last successful fetch time.
    pub last_fetched: Option<DateTime<Utc>>,

    /// Last fetch error (if any).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Cached item data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedItem {
    /// Unique ID (generated from link or title hash).
    pub id: String,

    /// Item ID assigned by a synchronization server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_id: Option<String>,

    /// Article title.
    pub title: String,

    /// Article URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,

    /// Publication date.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published: Option<DateTime<Utc>>,

    /// Summary or content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,

    /// Whether the item has been read.
    #[serde(default)]
    pub read: bool,

    /// When this item was first cached.
    pub cached_at: DateTime<Utc>,
}

impl CachedItem {
    /// Generate a unique ID for an item.
    #[must_use]
    pub fn generate_id(link: Option<&str>, title: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        if let Some(link) = link {
            link.hash(&mut hasher);
        } else {
            title.hash(&mut hasher);
        }
        format!("{:x}", hasher.finish())
    }
}

/// Feed cache manager.
#[derive(Debug)]
pub struct FeedCache {
    /// Cached feeds by URL.
    feeds: HashMap<String, CachedFeed>,

    /// Whether cache has been modified.
    dirty: bool,

    /// SQLite connection used for durable article and sync-status storage.
    connection: Mutex<Connection>,
}

impl Default for FeedCache {
    fn default() -> Self {
        let connection = Connection::open_in_memory().expect("open in-memory article database");
        Self::initialize_database(&connection).expect("initialize in-memory article database");
        Self {
            feeds: HashMap::new(),
            dirty: false,
            connection: Mutex::new(connection),
        }
    }
}

impl FeedCache {
    /// Load cache from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache file exists but cannot be read or parsed.
    pub fn load() -> Result<Self> {
        let path = Self::database_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        Self::initialize_database(&connection)?;
        let feeds = Self::load_feeds(&connection)?;

        debug!("Loaded {} feeds from cache", feeds.len());

        Ok(Self {
            feeds,
            dirty: false,
            connection: Mutex::new(connection),
        })
    }

    /// Save cache to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache file cannot be written.
    #[allow(clippy::significant_drop_tightening)]
    pub fn save(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }

        {
            let mut connection = self
                .connection
                .lock()
                .map_err(|_| color_eyre::eyre::eyre!("Article database lock poisoned"))?;
            let transaction = connection.transaction()?;
            transaction.execute("DELETE FROM articles", [])?;
            transaction.execute("DELETE FROM feeds", [])?;

            for feed in self.feeds.values() {
                transaction.execute(
                "INSERT INTO feeds (url, name, last_fetched, last_error) VALUES (?1, ?2, ?3, ?4)",
                params![
                    feed.url,
                    feed.name,
                    feed.last_fetched.map(|date| date.timestamp()),
                    feed.last_error,
                ],
                )?;
                for item in &feed.items {
                    transaction.execute(
                        "INSERT INTO articles
                     (feed_url, id, sync_id, title, link, published, summary, is_read, cached_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            feed.url,
                            item.id,
                            item.sync_id,
                            item.title,
                            item.link,
                            item.published.map(|date| date.timestamp()),
                            item.summary,
                            item.read,
                            item.cached_at.timestamp(),
                        ],
                    )?;
                }
            }
            transaction.commit()?;
        }

        self.dirty = false;
        debug!("Saved {} feeds to cache", self.feeds.len());

        Ok(())
    }

    /// Get the cache file path.
    fn database_path() -> Result<PathBuf> {
        Config::data_dir()
            .map(|dir| dir.join("feedo.sqlite3"))
            .ok_or_else(|| color_eyre::eyre::eyre!("Could not determine cache directory"))
    }

    fn initialize_database(connection: &Connection) -> Result<()> {
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS feeds (
                 url TEXT PRIMARY KEY,
                 name TEXT NOT NULL,
                 last_fetched INTEGER,
                 last_error TEXT
             );
             CREATE TABLE IF NOT EXISTS articles (
                 feed_url TEXT NOT NULL REFERENCES feeds(url) ON DELETE CASCADE,
                 id TEXT NOT NULL,
                 sync_id TEXT,
                 title TEXT NOT NULL,
                 link TEXT,
                 published INTEGER,
                 summary TEXT,
                 is_read INTEGER NOT NULL DEFAULT 0,
                 cached_at INTEGER NOT NULL,
                 PRIMARY KEY (feed_url, id)
             );
             CREATE UNIQUE INDEX IF NOT EXISTS articles_sync_id
                 ON articles(sync_id) WHERE sync_id IS NOT NULL;
             CREATE INDEX IF NOT EXISTS articles_feed_published
                 ON articles(feed_url, published DESC);
             CREATE TABLE IF NOT EXISTS pending_statuses (
                 sync_id TEXT PRIMARY KEY,
                 is_read INTEGER NOT NULL,
                 created_at INTEGER NOT NULL
             );",
        )?;
        Ok(())
    }

    fn load_feeds(connection: &Connection) -> Result<HashMap<String, CachedFeed>> {
        let mut feeds = HashMap::new();
        {
            let mut statement = connection
                .prepare("SELECT url, name, last_fetched, last_error FROM feeds ORDER BY name")?;
            let rows = statement.query_map([], |row| {
                let timestamp: Option<i64> = row.get(2)?;
                Ok(CachedFeed {
                    url: row.get(0)?,
                    name: row.get(1)?,
                    items: Vec::new(),
                    last_fetched: timestamp.and_then(|value| DateTime::from_timestamp(value, 0)),
                    last_error: row.get(3)?,
                })
            })?;
            for feed in rows {
                let feed = feed?;
                feeds.insert(feed.url.clone(), feed);
            }
        }

        let mut statement = connection.prepare(
            "SELECT feed_url, id, sync_id, title, link, published, summary, is_read, cached_at
             FROM articles ORDER BY feed_url, published DESC, cached_at DESC",
        )?;
        let rows = statement.query_map([], |row| {
            let published: Option<i64> = row.get(5)?;
            let cached_at: i64 = row.get(8)?;
            Ok((
                row.get::<_, String>(0)?,
                CachedItem {
                    id: row.get(1)?,
                    sync_id: row.get(2)?,
                    title: row.get(3)?,
                    link: row.get(4)?,
                    published: published.and_then(|value| DateTime::from_timestamp(value, 0)),
                    summary: row.get(6)?,
                    read: row.get(7)?,
                    cached_at: DateTime::from_timestamp(cached_at, 0).unwrap_or_else(Utc::now),
                },
            ))
        })?;
        for row in rows {
            let (feed_url, item) = row?;
            if let Some(feed) = feeds.get_mut(&feed_url) {
                feed.items.push(item);
            }
        }
        Ok(feeds)
    }

    /// Persist the latest local read state as a synchronization operation.
    pub fn queue_read_state(&self, sync_id: &str, read: bool) -> Result<()> {
        self.connection
            .lock()
            .map_err(|_| color_eyre::eyre::eyre!("Article database lock poisoned"))?
            .execute(
                "INSERT INTO pending_statuses (sync_id, is_read, created_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(sync_id) DO UPDATE SET
                 is_read = excluded.is_read,
                 created_at = excluded.created_at",
                params![sync_id, read, Utc::now().timestamp()],
            )?;
        Ok(())
    }

    /// Load all read-state changes that have not yet reached the server.
    #[allow(clippy::significant_drop_tightening)]
    pub fn pending_read_states(&self) -> Result<Vec<(String, bool)>> {
        let pending = {
            let connection = self
                .connection
                .lock()
                .map_err(|_| color_eyre::eyre::eyre!("Article database lock poisoned"))?;
            let mut statement = connection
                .prepare("SELECT sync_id, is_read FROM pending_statuses ORDER BY created_at")?;
            let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        Ok(pending)
    }

    /// Remove successfully uploaded read-state changes from the queue.
    pub fn acknowledge_read_states(&self, sync_ids: &[String]) -> Result<()> {
        for sync_id in sync_ids {
            self.connection
                .lock()
                .map_err(|_| color_eyre::eyre::eyre!("Article database lock poisoned"))?
                .execute(
                    "DELETE FROM pending_statuses WHERE sync_id = ?1",
                    params![sync_id],
                )?;
        }
        Ok(())
    }

    /// Get cached feed by URL.
    #[must_use]
    pub fn get(&self, url: &str) -> Option<&CachedFeed> {
        self.feeds.get(url)
    }

    /// Update cache for a feed.
    pub fn update_feed(
        &mut self,
        url: &str,
        name: &str,
        items: Vec<CachedItem>,
        error: Option<String>,
    ) {
        let now = Utc::now();

        let cached = self
            .feeds
            .entry(url.to_string())
            .or_insert_with(|| CachedFeed {
                url: url.to_string(),
                name: name.to_string(),
                items: Vec::new(),
                last_fetched: None,
                last_error: None,
            });

        cached.name = name.to_string();
        cached.last_error = error;

        if cached.last_error.is_none() {
            cached.last_fetched = Some(now);

            // Merge new items with history, preserving a read state once set.
            // Some public RSS feeds expose only 10-20 entries while sync
            // services retain considerably more history.
            let mut old_items: HashMap<String, CachedItem> = cached
                .items
                .drain(..)
                .map(|item| (item.id.clone(), item))
                .collect();
            let mut merged = Vec::with_capacity(MAX_ITEMS_PER_FEED);

            for mut item in items {
                if let Some(old) = old_items.remove(&item.id) {
                    item.read |= old.read;
                    item.cached_at = old.cached_at;
                    if item.sync_id.is_none() {
                        item.sync_id = old.sync_id;
                    }
                }
                merged.push(item);
            }

            // Keep older cached articles that are no longer present in the
            // latest RSS response, ordered newest first.
            let mut history: Vec<CachedItem> = old_items.into_values().collect();
            history.sort_by_key(|item| std::cmp::Reverse(item.published));
            merged.extend(history);
            merged.truncate(MAX_ITEMS_PER_FEED);
            cached.items = merged;
        }

        self.dirty = true;
    }

    /// Replace one synchronized feed with the authoritative account response.
    ///
    /// Read-state conflict resolution must happen before this call. Unlike a
    /// local RSS refresh, an account refresh does not retain stale articles or
    /// stale statuses that are absent from the server response.
    pub fn replace_synced_feed(&mut self, url: &str, name: &str, mut items: Vec<CachedItem>) {
        items.truncate(MAX_ITEMS_PER_FEED);
        self.feeds.insert(
            url.to_string(),
            CachedFeed {
                url: url.to_string(),
                name: name.to_string(),
                items,
                last_fetched: Some(Utc::now()),
                last_error: None,
            },
        );
        self.dirty = true;
    }

    /// Mark an item as read/unread.
    pub fn set_item_read(&mut self, feed_url: &str, item_id: &str, read: bool) -> bool {
        if let Some(feed) = self.feeds.get_mut(feed_url) {
            if let Some(item) = feed.items.iter_mut().find(|i| i.id == item_id) {
                if item.read != read {
                    item.read = read;
                    self.dirty = true;
                    return true;
                }
            }
        }
        false
    }

    /// Mark all items in a feed as read.
    pub fn mark_feed_read(&mut self, feed_url: &str) {
        if let Some(feed) = self.feeds.get_mut(feed_url) {
            for item in &mut feed.items {
                if !item.read {
                    item.read = true;
                    self.dirty = true;
                }
            }
        }
    }

    /// Remove a feed from cache.
    pub fn remove_feed(&mut self, url: &str) {
        if self.feeds.remove(url).is_some() {
            self.dirty = true;
        }
    }

    /// Get cache statistics.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        let total_feeds = self.feeds.len();
        let total_items: usize = self.feeds.values().map(|f| f.items.len()).sum();
        let unread_items: usize = self
            .feeds
            .values()
            .flat_map(|f| f.items.iter())
            .filter(|i| !i.read)
            .count();

        let oldest_fetch = self.feeds.values().filter_map(|f| f.last_fetched).min();

        CacheStats {
            total_feeds,
            total_items,
            unread_items,
            oldest_fetch,
        }
    }

    /// Prune old items beyond a limit per feed.
    pub fn prune(&mut self, max_items_per_feed: usize) {
        for feed in self.feeds.values_mut() {
            if feed.items.len() > max_items_per_feed {
                // Keep newest items, but always keep unread items
                feed.items.sort_by(|a, b| {
                    // Unread items first, then by date descending
                    match (a.read, b.read) {
                        (false, true) => std::cmp::Ordering::Less,
                        (true, false) => std::cmp::Ordering::Greater,
                        _ => b.cached_at.cmp(&a.cached_at),
                    }
                });

                let old_len = feed.items.len();
                feed.items.truncate(max_items_per_feed);

                if feed.items.len() < old_len {
                    self.dirty = true;
                    debug!(
                        "Pruned {} items from {}",
                        old_len - feed.items.len(),
                        feed.name
                    );
                }
            }
        }
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Total number of cached feeds.
    pub total_feeds: usize,
    /// Total number of cached items.
    pub total_items: usize,
    /// Number of unread items.
    pub unread_items: usize,
    /// Oldest fetch time.
    pub oldest_fetch: Option<DateTime<Utc>>,
}

impl Drop for FeedCache {
    fn drop(&mut self) {
        if self.dirty {
            if let Err(e) = self.save() {
                warn!("Failed to save cache on drop: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: i64, read: bool) -> CachedItem {
        CachedItem {
            id: id.to_string(),
            sync_id: Some(format!("server-{id}")),
            title: format!("Article {id}"),
            link: Some(format!("https://example.com/{id}")),
            published: DateTime::from_timestamp(id, 0),
            summary: None,
            read,
            cached_at: Utc::now(),
        }
    }

    #[test]
    fn test_generate_id() {
        let id1 = CachedItem::generate_id(Some("https://example.com/1"), "Title");
        let id2 = CachedItem::generate_id(Some("https://example.com/2"), "Title");
        let id3 = CachedItem::generate_id(None, "Title");

        assert_ne!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_cache_stats() {
        let cache = FeedCache::default();
        let stats = cache.stats();

        assert_eq!(stats.total_feeds, 0);
        assert_eq!(stats.total_items, 0);
    }

    #[test]
    fn update_feed_keeps_history_and_caps_it_at_one_hundred() {
        let mut cache = FeedCache::default();
        cache.update_feed(
            "https://example.com/feed",
            "Example",
            (0..100).map(|id| item(id, id == 99)).collect(),
            None,
        );
        cache.update_feed(
            "https://example.com/feed",
            "Example",
            (100..110).map(|id| item(id, false)).collect(),
            None,
        );

        let articles = &cache.get("https://example.com/feed").unwrap().items;
        assert_eq!(articles.len(), 100);
        assert!(
            articles
                .iter()
                .any(|article| article.id == "99" && article.read)
        );
    }

    #[test]
    fn pending_status_queue_keeps_latest_state_until_acknowledged() {
        let cache = FeedCache::default();
        cache.queue_read_state("server-1", true).unwrap();
        cache.queue_read_state("server-1", false).unwrap();

        assert_eq!(
            cache.pending_read_states().unwrap(),
            vec![("server-1".to_string(), false)]
        );

        cache
            .acknowledge_read_states(&["server-1".to_string()])
            .unwrap();
        assert!(cache.pending_read_states().unwrap().is_empty());
    }

    #[test]
    fn synchronized_feed_replaces_stale_local_read_state() {
        let mut cache = FeedCache::default();
        cache.update_feed(
            "https://example.com/feed",
            "Example",
            vec![item(1, true)],
            None,
        );
        cache.replace_synced_feed("https://example.com/feed", "Example", vec![item(1, false)]);

        assert!(!cache.get("https://example.com/feed").unwrap().items[0].read);
    }
}
