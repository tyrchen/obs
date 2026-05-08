//! Axum handlers. Each handler:
//!
//! 1. Records `let started = Instant::now();`.
//! 2. Opens an `obs::scope!` so handler-internal emits inherit the route, method, and request id
//!    labels.
//! 3. Performs the store operation.
//! 4. Emits the matching domain event (e.g. `ObsTodoCreated`).
//! 5. Emits exactly one `ObsHttpRequestProcessed` before returning.

use std::time::Instant;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use obs_kit::{Severity, scope};
use serde::Deserialize;
use serde_json::json;

use crate::{
    AppState,
    schema::todomvc,
    store::{CompleteOutcome, DeleteOutcome, TodoFilter, UpdateOutcome},
};

const POST_TODOS: &str = "POST /todos";
const GET_TODOS: &str = "GET /todos";
const PATCH_TODO: &str = "PATCH /todos/:id";
const DELETE_TODO: &str = "DELETE /todos/:id";
const GET_HEALTHZ: &str = "GET /healthz";

#[derive(Debug, Deserialize)]
pub(crate) struct CreateBody {
    title: String,
    list: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PatchBody {
    title: Option<String>,
    completed: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListQuery {
    filter: Option<String>,
}

pub(crate) async fn create_todo(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateBody>,
) -> Response {
    let started = Instant::now();
    let req_id = request_id(&headers);
    let _scope = scope!(route = POST_TODOS, method = "POST", request_id = req_id);

    if body.title.trim().is_empty() {
        emit_request(POST_TODOS, "POST", StatusCode::BAD_REQUEST, started);
        return (StatusCode::BAD_REQUEST, "title required").into_response();
    }

    let Some(todo) = state.todos.create(body.title.clone(), body.list.clone()) else {
        emit_request(
            POST_TODOS,
            "POST",
            StatusCode::INTERNAL_SERVER_ERROR,
            started,
        );
        return (StatusCode::INTERNAL_SERVER_ERROR, "store unavailable").into_response();
    };

    todomvc::v1::ObsTodoCreated::builder()
        .todo_id(todo.id.clone())
        .title(todo.title.clone())
        .list(todo.list.clone())
        .emit();

    emit_request(POST_TODOS, "POST", StatusCode::CREATED, started);
    (StatusCode::CREATED, Json(todo)).into_response()
}

pub(crate) async fn list_todos(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Response {
    let started = Instant::now();
    let req_id = request_id(&headers);
    let filter = TodoFilter::parse(q.filter.as_deref());
    let _scope = scope!(
        route = GET_TODOS,
        method = "GET",
        request_id = req_id,
        filter = filter.as_label(),
    );

    let Some(rows) = state.todos.list(filter) else {
        emit_request(GET_TODOS, "GET", StatusCode::INTERNAL_SERVER_ERROR, started);
        return (StatusCode::INTERNAL_SERVER_ERROR, "store unavailable").into_response();
    };

    let count = u64::try_from(rows.len()).unwrap_or(u64::MAX);
    let latency_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);

    // List is bucketed per-list — use the first row's list as the
    // dominant label, falling back to "all" so filter combinations
    // still record cleanly. Real apps would emit one per list bucket.
    let list_label = rows
        .first()
        .map(|t| t.list.clone())
        .unwrap_or_else(|| "all".to_string());

    todomvc::v1::ObsTodoListQueried::builder()
        .list(list_label)
        .filter(filter.as_label().to_string())
        .result_count(count)
        .latency_us(latency_us)
        .emit();

    emit_request(GET_TODOS, "GET", StatusCode::OK, started);
    Json(json!({ "items": rows })).into_response()
}

pub(crate) async fn patch_todo(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<PatchBody>,
) -> Response {
    let started = Instant::now();
    let req_id = request_id(&headers);
    let _scope = scope!(
        route = PATCH_TODO,
        method = "PATCH",
        request_id = req_id,
        todo_id = id.clone(),
    );

    // `completed: true` short-circuits to the dedicated completion path
    // so we get a typed ObsTodoCompleted with dwell time.
    if body.completed == Some(true) {
        return handle_complete(&state, &id, started).await;
    }

    let Some(new_title) = body.title.clone() else {
        emit_request(PATCH_TODO, "PATCH", StatusCode::BAD_REQUEST, started);
        return (StatusCode::BAD_REQUEST, "no fields to update").into_response();
    };

    let Some(outcome) = state.todos.update_title(&id, new_title.clone()) else {
        emit_request(
            PATCH_TODO,
            "PATCH",
            StatusCode::INTERNAL_SERVER_ERROR,
            started,
        );
        return (StatusCode::INTERNAL_SERVER_ERROR, "store unavailable").into_response();
    };

    match outcome {
        UpdateOutcome::NotFound => {
            emit_request(PATCH_TODO, "PATCH", StatusCode::NOT_FOUND, started);
            (StatusCode::NOT_FOUND, "no such todo").into_response()
        }
        UpdateOutcome::Updated(t) => {
            todomvc::v1::ObsTodoUpdated::builder()
                .todo_id(t.id.clone())
                .field_changed("title".to_string())
                .new_value(new_title)
                .emit();
            emit_request(PATCH_TODO, "PATCH", StatusCode::OK, started);
            Json(t).into_response()
        }
    }
}

async fn handle_complete(state: &AppState, id: &str, started: Instant) -> Response {
    let Some(outcome) = state.todos.complete(id) else {
        emit_request(
            PATCH_TODO,
            "PATCH",
            StatusCode::INTERNAL_SERVER_ERROR,
            started,
        );
        return (StatusCode::INTERNAL_SERVER_ERROR, "store unavailable").into_response();
    };

    match outcome {
        CompleteOutcome::NotFound => {
            emit_request(PATCH_TODO, "PATCH", StatusCode::NOT_FOUND, started);
            (StatusCode::NOT_FOUND, "no such todo").into_response()
        }
        CompleteOutcome::AlreadyCompleted(t) => {
            // Idempotent — emit no-op log at WARN so dashboards
            // surface duplicate completions.
            todomvc::v1::ObsTodoUpdated::builder()
                .todo_id(t.id.clone())
                .field_changed("completed".to_string())
                .new_value("noop".to_string())
                .emit_at(Severity::Warn);
            emit_request(PATCH_TODO, "PATCH", StatusCode::OK, started);
            Json(t).into_response()
        }
        CompleteOutcome::Completed { todo, latency_ms } => {
            todomvc::v1::ObsTodoCompleted::builder()
                .todo_id(todo.id.clone())
                .list(todo.list.clone())
                .latency_ms_since_creation(latency_ms)
                .emit();
            emit_request(PATCH_TODO, "PATCH", StatusCode::OK, started);
            Json(todo).into_response()
        }
    }
}

pub(crate) async fn delete_todo(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let started = Instant::now();
    let req_id = request_id(&headers);
    let _scope = scope!(
        route = DELETE_TODO,
        method = "DELETE",
        request_id = req_id,
        todo_id = id.clone(),
    );

    let Some(outcome) = state.todos.delete(&id) else {
        emit_request(
            DELETE_TODO,
            "DELETE",
            StatusCode::INTERNAL_SERVER_ERROR,
            started,
        );
        return (StatusCode::INTERNAL_SERVER_ERROR, "store unavailable").into_response();
    };

    match outcome {
        DeleteOutcome::NotFound => {
            emit_request(DELETE_TODO, "DELETE", StatusCode::NOT_FOUND, started);
            (StatusCode::NOT_FOUND, "no such todo").into_response()
        }
        DeleteOutcome::Deleted(t) => {
            todomvc::v1::ObsTodoDeleted::builder()
                .todo_id(t.id.clone())
                .list(t.list.clone())
                .was_completed(t.is_completed())
                .emit();
            emit_request(DELETE_TODO, "DELETE", StatusCode::NO_CONTENT, started);
            StatusCode::NO_CONTENT.into_response()
        }
    }
}

pub(crate) async fn healthz(headers: HeaderMap) -> Response {
    let started = Instant::now();
    let req_id = request_id(&headers);
    let _scope = scope!(route = GET_HEALTHZ, method = "GET", request_id = req_id);
    emit_request(GET_HEALTHZ, "GET", StatusCode::OK, started);
    (StatusCode::OK, "ok\n").into_response()
}

fn emit_request(route: &str, method: &str, status: StatusCode, started: Instant) {
    let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let sev = severity_for(status);
    todomvc::v1::ObsHttpRequestProcessed::builder()
        .route(route.to_string())
        .method(method.to_string())
        .status(status.as_u16().to_string())
        .latency_ms(latency_ms)
        .emit_at(sev);
}

fn severity_for(status: StatusCode) -> Severity {
    if status.is_server_error() {
        Severity::Error
    } else if status.is_client_error() {
        Severity::Warn
    } else {
        Severity::Info
    }
}

fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(generate_request_id)
}

fn generate_request_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("req-{nanos:x}")
}
