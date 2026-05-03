//! In-memory todo store. A real backend would swap this for SQL — the
//! observability surface stays exactly the same because the events are
//! emitted at the handler layer, not from inside the store.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

/// One todo row.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Todo {
    /// Stable id, e.g. `todo-7`.
    pub id: String,
    /// Free-form title.
    pub title: String,
    /// Bucket name (`groceries`, `work`, ...). Low-cardinality.
    pub list: String,
    /// Wall-clock creation time, ms since epoch.
    pub created_at_ms: u64,
    /// Set when the todo is marked completed; `None` while open.
    pub completed_at_ms: Option<u64>,
}

impl Todo {
    /// `true` when [`Self::completed_at_ms`] has been set.
    #[must_use]
    pub fn is_completed(&self) -> bool {
        self.completed_at_ms.is_some()
    }
}

/// Filter accepted by [`TodoStore::list`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TodoFilter {
    /// All rows.
    All,
    /// Only open rows.
    Active,
    /// Only completed rows.
    Completed,
}

impl TodoFilter {
    /// Stable label string used in metrics. Low-cardinality enum.
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Active => "active",
            Self::Completed => "completed",
        }
    }

    /// Parse `?filter=` query string.
    #[must_use]
    pub fn parse(s: Option<&str>) -> Self {
        match s.unwrap_or("all") {
            "active" => Self::Active,
            "completed" => Self::Completed,
            _ => Self::All,
        }
    }
}

/// Outcome of an update operation.
#[derive(Debug)]
pub enum UpdateOutcome {
    /// The row was missing.
    NotFound,
    /// The row was updated. Carries the post-update snapshot.
    Updated(Todo),
}

/// Outcome of a complete operation.
#[derive(Debug)]
pub enum CompleteOutcome {
    /// The row was missing.
    NotFound,
    /// Already completed — no state change.
    AlreadyCompleted(Todo),
    /// Newly completed. Carries the dwell time in ms since creation.
    Completed { todo: Todo, latency_ms: u64 },
}

/// Outcome of a delete operation.
#[derive(Debug)]
pub enum DeleteOutcome {
    /// The row was missing.
    NotFound,
    /// Successfully deleted. Carries the snapshot before deletion.
    Deleted(Todo),
}

/// Thread-safe in-memory todo store.
#[derive(Debug, Clone, Default)]
pub struct TodoStore {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    rows: Mutex<HashMap<String, Todo>>,
    next: AtomicU64,
}

impl TodoStore {
    /// Build an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a new todo. Returns the snapshot.
    pub fn create(&self, title: String, list: String) -> Option<Todo> {
        let id = self.inner.next.fetch_add(1, Ordering::Relaxed) + 1;
        let now = epoch_ms();
        let todo = Todo {
            id: format!("todo-{id}"),
            title,
            list,
            created_at_ms: now,
            completed_at_ms: None,
        };
        let mut g = self.inner.rows.lock().ok()?;
        g.insert(todo.id.clone(), todo.clone());
        Some(todo)
    }

    /// Snapshot the rows matching `filter`.
    pub fn list(&self, filter: TodoFilter) -> Option<Vec<Todo>> {
        let g = self.inner.rows.lock().ok()?;
        let mut out: Vec<Todo> = g
            .values()
            .filter(|t| match filter {
                TodoFilter::All => true,
                TodoFilter::Active => !t.is_completed(),
                TodoFilter::Completed => t.is_completed(),
            })
            .cloned()
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Some(out)
    }

    /// Update the title of an existing todo.
    pub fn update_title(&self, id: &str, new_title: String) -> Option<UpdateOutcome> {
        let mut g = self.inner.rows.lock().ok()?;
        let Some(t) = g.get_mut(id) else {
            return Some(UpdateOutcome::NotFound);
        };
        t.title = new_title;
        Some(UpdateOutcome::Updated(t.clone()))
    }

    /// Mark a todo completed.
    pub fn complete(&self, id: &str) -> Option<CompleteOutcome> {
        let mut g = self.inner.rows.lock().ok()?;
        let Some(t) = g.get_mut(id) else {
            return Some(CompleteOutcome::NotFound);
        };
        if t.is_completed() {
            return Some(CompleteOutcome::AlreadyCompleted(t.clone()));
        }
        let now = epoch_ms();
        t.completed_at_ms = Some(now);
        let latency_ms = now.saturating_sub(t.created_at_ms);
        Some(CompleteOutcome::Completed {
            todo: t.clone(),
            latency_ms,
        })
    }

    /// Delete a todo.
    pub fn delete(&self, id: &str) -> Option<DeleteOutcome> {
        let mut g = self.inner.rows.lock().ok()?;
        let Some(t) = g.remove(id) else {
            return Some(DeleteOutcome::NotFound);
        };
        Some(DeleteOutcome::Deleted(t))
    }
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
