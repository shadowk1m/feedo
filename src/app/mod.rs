//! Main application module.
//!
//! Orchestrates all components and runs the main event loop.

use std::io::{self, stdout};

use color_eyre::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::prelude::*;
use tracing::info;

use crate::config::Config;
use crate::feed::{FeedCache, FeedItem, FeedManager};
use crate::sync::SyncResult;
use crate::ui::{FeedListItem, UiState};
use ratatui_themes::Theme;

/// Main application state.
pub struct App {
    /// Application configuration.
    pub config: Config,

    /// Feed manager.
    pub feeds: FeedManager,

    /// UI state.
    pub ui: UiState,

    /// Theme configuration.
    pub theme: Theme,

    /// Reused connection for automatic read-state uploads.
    sync_manager: Option<crate::sync::SyncManager>,

    /// Earliest time a failed automatic upload should be retried.
    read_sync_retry_at: Option<std::time::Instant>,

    /// Full account synchronization running independently of the UI loop.
    sync_task: Option<tokio::task::JoinHandle<std::result::Result<(Config, SyncResult), String>>>,
}

impl App {
    /// Create a new application instance.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration cannot be loaded or feeds cannot be initialized.
    pub fn new() -> Result<Self> {
        let config = Config::load()?;
        let theme = config.theme;
        let sync_enabled = config.sync.is_some();
        let feeds = FeedManager::new(&config)?;

        // Check if we have cached data (offline mode)
        let has_cached = feeds.feeds.iter().any(|f| !f.items.is_empty());

        if has_cached {
            info!("Loaded cached articles for offline reading");
        }

        // Don't refresh on startup - let the UI show first, then refresh in background
        // feeds.refresh_all().await;

        let ui = UiState {
            sync_enabled,
            pending_sync: sync_enabled,
            // Mark that we need to refresh feeds
            refreshing: !has_cached,
            ..Default::default()
        };

        let mut app = Self {
            config,
            feeds,
            ui,
            theme,
            sync_manager: None,
            read_sync_retry_at: None,
            sync_task: None,
        };

        // Build initial feed list
        app.rebuild_feed_list();
        app.select_first_feed();

        Ok(app)
    }

    /// Run the main application loop.
    ///
    /// # Errors
    ///
    /// Returns an error if terminal operations fail.
    pub async fn run(&mut self) -> Result<()> {
        // Setup terminal
        enable_raw_mode()?;
        let mut stdout = stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        // Main loop
        let result = self.main_loop(&mut terminal).await;

        // Save cache before exit
        self.feeds.save_cache();

        // Restore terminal
        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;

        result
    }

    async fn main_loop(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    ) -> Result<()> {
        use crossterm::event::poll;
        use std::time::Duration;

        // Track if we need initial refresh
        // Synced accounts get all articles from their account server. Direct
        // RSS fetching is reserved for local-only accounts.
        let mut needs_initial_refresh = self.ui.refreshing && !self.ui.sync_enabled;
        let mut update_check_done = false;

        loop {
            self.finish_sync_if_ready().await;

            // Render
            terminal.draw(|frame| self.render(frame))?;

            // Process pending update after draw (so "Updating..." is visible)
            if self.ui.pending_update {
                self.process_pending_update();
                // Redraw immediately after update completes
                terminal.draw(|frame| self.render(frame))?;
            }

            // Use poll with timeout to allow background work
            if poll(Duration::from_millis(100))? {
                match event::read()? {
                    Event::Key(key) => {
                        if key.kind == KeyEventKind::Press {
                            match self.handle_key(key.code).await {
                                crate::ui::input::KeyResult::Quit => {
                                    self.cancel_sync();
                                    break;
                                }
                                crate::ui::input::KeyResult::Continue => {}
                            }
                            // Mark current item read whenever content is visible
                            if self.ui.show_content {
                                self.mark_current_read();
                            }
                        }
                    }
                    Event::Mouse(mouse) => {
                        self.handle_mouse(mouse);
                    }
                    _ => {}
                }
            } else {
                // No input - do background work

                if !self.ui.syncing {
                    self.sync_pending_read_items().await;
                }

                // Initial refresh (one feed at a time to stay responsive)
                if needs_initial_refresh {
                    if let Some(idx) = self
                        .feeds
                        .feeds
                        .iter()
                        .position(|f| f.last_updated.is_none())
                    {
                        self.feeds.refresh_feed(idx).await;
                        self.rebuild_feed_list();
                    } else {
                        needs_initial_refresh = false;
                        self.ui.refreshing = false;
                        self.feeds.save_cache();
                    }
                }

                // A sync account is the source of truth for article history.
                // Run its initial refresh once the local feed refresh finishes.
                if !needs_initial_refresh && self.ui.pending_sync {
                    self.ui.pending_sync = false;
                    if let Err(error) = self.start_sync() {
                        self.ui.set_error(format!("Initial sync failed: {error}"));
                    }
                }

                // Check for updates in background (once)
                if !update_check_done && !needs_initial_refresh {
                    update_check_done = true;
                    if let crate::VersionCheck::UpdateAvailable { latest, .. } =
                        crate::check_for_updates_crates_io().await
                    {
                        self.ui.update_available = Some(latest);
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a pending update.
    fn process_pending_update(&mut self) {
        self.ui.pending_update = false;

        match crate::run_update(&self.ui.package_manager) {
            Ok(()) => {
                self.ui.update_status = Some("Update complete! Please restart feedo.".to_string());
                self.ui.update_available = None;
            }
            Err(e) => {
                self.ui.update_status = Some(format!("Update failed: {e}"));
            }
        }
        self.ui.mode = crate::ui::Mode::Normal;
    }

    /// Start a full account synchronization without blocking the TUI loop.
    pub fn start_sync(&mut self) -> Result<()> {
        if self.sync_task.is_some() {
            return Ok(());
        }

        self.feeds.cache.save()?;
        let mut config = self.config.clone();
        let sync = config
            .sync
            .clone()
            .ok_or_else(|| color_eyre::eyre::eyre!("No sync configured"))?;
        let (username, password) = sync
            .get_credentials()
            .ok_or_else(|| color_eyre::eyre::eyre!("No credentials stored"))?;

        self.ui.syncing = true;
        self.ui.set_status("⟳ Syncing with FreshRSS...");
        self.sync_task = Some(tokio::spawn(async move {
            let mut cache = FeedCache::load().map_err(|error| error.to_string())?;
            let manager = crate::sync::SyncManager::connect(&sync.server, &username, &password)
                .await
                .map_err(|error| error.to_string())?;
            let result = manager
                .full_sync(&mut config, &mut cache)
                .await
                .map_err(|error| error.to_string())?;
            config.save().map_err(|error| error.to_string())?;
            cache.save().map_err(|error| error.to_string())?;
            Ok((config, result))
        }));
        Ok(())
    }

    async fn finish_sync_if_ready(&mut self) {
        if !self
            .sync_task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            return;
        }

        let selected_url = self
            .ui
            .selected_feed
            .and_then(|index| self.feeds.feeds.get(index))
            .map(|feed| feed.url.clone());
        let task = self.sync_task.take().expect("finished sync task exists");
        self.ui.syncing = false;
        self.ui.refreshing = false;

        match task.await {
            Ok(Ok((config, result))) => {
                self.config = config;
                match FeedManager::new(&self.config) {
                    Ok(feeds) => {
                        self.feeds = feeds;
                        self.rebuild_feed_list();
                        if let Some(url) = selected_url {
                            self.restore_selected_feed(&url);
                        }
                        self.ui.read_this_session.clear();
                        self.ui.set_status(format!(
                            "✓ Sync complete: +{} feeds, {} statuses",
                            result.feeds_imported,
                            result.items_marked_read + result.items_synced_to_server
                        ));
                    }
                    Err(error) => self
                        .ui
                        .set_error(format!("Could not reload synchronized articles: {error}")),
                }
            }
            Ok(Err(error)) => self.ui.set_error(format!("Sync failed: {error}")),
            Err(error) if !error.is_cancelled() => {
                self.ui.set_error(format!("Sync task failed: {error}"));
            }
            Err(_) => {}
        }
    }

    fn restore_selected_feed(&mut self, url: &str) {
        if let Some(feed_index) = self.feeds.feeds.iter().position(|feed| feed.url == url) {
            self.ui.selected_feed = Some(feed_index);
            if let Some(list_index) = self
                .ui
                .feed_list
                .iter()
                .position(|item| *item == FeedListItem::Feed(feed_index))
            {
                self.ui.feed_list_index = list_index;
                self.sync_feed_list_state();
            }
            let item_count = self.visible_items().len();
            self.ui.selected_item = self.ui.selected_item.min(item_count.saturating_sub(1));
            self.sync_items_list_state();
        }
    }

    fn cancel_sync(&mut self) {
        if let Some(task) = self.sync_task.take() {
            task.abort();
        }
        self.ui.syncing = false;
    }

    /// Upload locally read items to the configured sync server in one batch.
    async fn sync_pending_read_items(&mut self) {
        if !self.ui.sync_enabled {
            return;
        }
        let pending = match self.feeds.cache.pending_read_states() {
            Ok(pending) if !pending.is_empty() => pending,
            Ok(_) => return,
            Err(error) => {
                self.ui
                    .set_error(format!("Could not load pending sync changes: {error}"));
                return;
            }
        };
        if self
            .read_sync_retry_at
            .is_some_and(|retry_at| std::time::Instant::now() < retry_at)
        {
            return;
        }

        if self.sync_manager.is_none() {
            let Some(sync) = self.config.sync.clone() else {
                return;
            };
            let Some((username, password)) = sync.get_credentials() else {
                return;
            };
            match crate::sync::SyncManager::connect(&sync.server, &username, &password).await {
                Ok(manager) => self.sync_manager = Some(manager),
                Err(error) => {
                    self.ui.set_error(format!("Read sync failed: {error}"));
                    self.read_sync_retry_at =
                        Some(std::time::Instant::now() + std::time::Duration::from_secs(30));
                    return;
                }
            }
        }

        let manager = self
            .sync_manager
            .as_ref()
            .expect("sync manager initialized");
        for read in [true, false] {
            let ids: Vec<String> = pending
                .iter()
                .filter(|(_, state)| *state == read)
                .map(|(id, _)| id.clone())
                .collect();
            if ids.is_empty() {
                continue;
            }
            let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
            let result = if read {
                manager.client().mark_read(manager.auth(), &id_refs).await
            } else {
                manager.client().mark_unread(manager.auth(), &id_refs).await
            };
            if let Err(error) = result {
                self.ui.set_error(format!("Read sync failed: {error}"));
                self.sync_manager = None;
                self.read_sync_retry_at =
                    Some(std::time::Instant::now() + std::time::Duration::from_secs(30));
                return;
            }
            if let Err(error) = self.feeds.cache.acknowledge_read_states(&ids) {
                self.ui
                    .set_error(format!("Could not finish read sync: {error}"));
                return;
            }
        }
        self.read_sync_retry_at = None;
    }

    /// Rebuild the flattened feed list for the UI.
    pub fn rebuild_feed_list(&mut self) {
        self.ui.feed_list.clear();

        // Add folders and their feeds
        for (folder_idx, folder) in self.feeds.folders.iter().enumerate() {
            self.ui.feed_list.push(FeedListItem::Folder(folder_idx));

            if folder.expanded {
                for &feed_idx in &folder.feed_indices {
                    self.ui.feed_list.push(FeedListItem::Feed(feed_idx));
                }
            }
        }

        // Add root-level feeds
        for feed_idx in self.feeds.root_feed_indices() {
            self.ui.feed_list.push(FeedListItem::Feed(feed_idx));
        }

        // Sync list state for scrolling
        self.sync_feed_list_state();
    }

    /// Sync `feed_list_state` selection with `feed_list_index`.
    pub fn sync_feed_list_state(&mut self) {
        self.ui
            .feed_list_state
            .select(Some(self.ui.feed_list_index));
    }

    /// Sync `items_list_state` selection with `selected_item`.
    pub fn sync_items_list_state(&mut self) {
        self.ui.items_list_state.select(Some(self.ui.selected_item));
    }

    /// Select the first feed in the list.
    pub fn select_first_feed(&mut self) {
        for (idx, item) in self.ui.feed_list.iter().enumerate() {
            if let FeedListItem::Feed(feed_idx) = item {
                self.ui.feed_list_index = idx;
                self.ui.selected_feed = Some(*feed_idx);
                break;
            }
        }
    }

    /// Get items from the currently selected feed.
    #[must_use]
    pub fn current_feed_items(&self) -> &[FeedItem] {
        self.ui
            .selected_feed
            .and_then(|idx| self.feeds.feeds.get(idx))
            .map_or(&[], |f| f.items.as_slice())
    }

    /// Get visible items from the currently selected feed, filtered by `hide_read`.
    ///
    /// Returns `(raw_index, &FeedItem)` pairs so callers can map back to the
    /// underlying `feed.items` vec for mutations.
    ///
    /// Items marked read during this session are kept visible until the feed is
    /// refreshed, so they don't disappear immediately after being read.
    #[must_use]
    pub fn visible_items(&self) -> Vec<(usize, &FeedItem)> {
        self.current_feed_items()
            .iter()
            .enumerate()
            .filter(|(_, item)| {
                !self.ui.hide_read || !item.read || self.ui.read_this_session.contains(&item.id)
            })
            .collect()
    }

    /// Get the currently selected item.
    #[must_use]
    pub fn selected_item(&self) -> Option<&FeedItem> {
        self.visible_items()
            .into_iter()
            .nth(self.ui.selected_item)
            .map(|(_, item)| item)
    }
}
