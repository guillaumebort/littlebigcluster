use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use askama::Template;
use axum::extract::State;
use axum::response::Html;
use axum::routing::{get, post};
use axum::Form;
use lbc::NodeHandle;
use rusqlite::Connection;
use serde::Deserialize;

#[derive(Clone)]
pub struct App {
    pub node: NodeHandle,
    pub node_id: String,
    pub last_result: Arc<tokio::sync::Mutex<Option<SqlOutcome>>>,
}

#[derive(Debug)]
pub enum SqlOutcome {
    Query {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    Write {
        rows: usize,
    },
    Error(String),
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    is_leader: bool,
    members: Vec<MemberRow>,
    schema: Vec<SchemaRow>,
    result: Option<ResultView>,
}

struct MemberRow {
    mark: &'static str,
    id: String,
    leader_suffix: &'static str,
    row_style: String,
    addr: String,
    az: String,
    epoch: u64,
    schema_version: u32,
    schema_support: String,
}

struct SchemaRow {
    name: String,
    ddl: String,
}

struct ResultView {
    error: Option<String>,
    write_rows: Option<usize>,
    query_columns: Vec<String>,
    query_rows: Vec<Vec<String>>,
}

pub fn http_addr(grpc_addr: &str) -> Result<SocketAddr> {
    let grpc: SocketAddr = grpc_addr.parse().context("invalid --addr")?;
    Ok(SocketAddr::new(grpc.ip(), grpc.port() + 1000))
}

pub fn router(app: App) -> axum::Router {
    axum::Router::new()
        .route("/", get(index))
        .route("/sql", post(run_sql))
        .with_state(app)
}

async fn index(State(app): State<App>) -> Result<Html<String>, axum::http::StatusCode> {
    let result = app.last_result.lock().await.take();
    render(&app, result.as_ref())
        .await
        .map(Html)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)
}

#[derive(Deserialize)]
struct SqlForm {
    sql: String,
}

async fn run_sql(
    State(app): State<App>,
    Form(form): Form<SqlForm>,
) -> Result<Html<String>, axum::http::StatusCode> {
    let outcome = match exec_sql(&app.node, form.sql.trim()).await {
        Ok(o) => o,
        Err(e) => SqlOutcome::Error(e.to_string()),
    };
    let mut guard = app.last_result.lock().await;
    *guard = Some(outcome);
    render(&app, guard.as_ref())
        .await
        .map(Html)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)
}

async fn render(app: &App, result: Option<&SqlOutcome>) -> Result<String, askama::Error> {
    let view = app.node.subscribe_view().borrow().clone();
    let members = app.node.subscribe_members().borrow().clone();
    let schema = load_schema(&app.node).await.unwrap_or_else(|e| {
        vec![SchemaRow {
            name: "error".into(),
            ddl: e.to_string(),
        }]
    });

    IndexTemplate {
        is_leader: view.is_leader,
        members: members
            .iter()
            .map(|m| {
                let incompatible = m.schema_max > 0 && m.schema_version > m.schema_max;
                let is_leader = m.node_id == view.leader_id;
                let mut styles = Vec::new();
                if is_leader {
                    styles.push("font-weight:bold");
                }
                if incompatible {
                    styles.push("background:#fdd");
                }
                MemberRow {
                    mark: if m.node_id == app.node_id { "*" } else { "" },
                    id: m.node_id.clone(),
                    leader_suffix: if is_leader { " (leader)" } else { "" },
                    row_style: styles.join(";"),
                    addr: m.addr.clone(),
                    az: m.az.clone(),
                    epoch: m.epoch,
                    schema_version: m.schema_version,
                    schema_support: format!("{}–{}", m.schema_min, m.schema_max),
                }
            })
            .collect(),
        schema,
        result: result.map(|r| match r {
            SqlOutcome::Error(e) => ResultView {
                error: Some(e.clone()),
                write_rows: None,
                query_columns: vec![],
                query_rows: vec![],
            },
            SqlOutcome::Write { rows } => ResultView {
                error: None,
                write_rows: Some(*rows),
                query_columns: vec![],
                query_rows: vec![],
            },
            SqlOutcome::Query { columns, rows } => ResultView {
                error: None,
                write_rows: None,
                query_columns: columns.clone(),
                query_rows: rows.clone(),
            },
        }),
    }
    .render()
}

async fn load_schema(node: &NodeHandle) -> Result<Vec<SchemaRow>> {
    let db = node.db();
    let db = db.lock().await;
    db.store().with_read(|conn| {
        let mut stmt = conn.prepare(
            "SELECT name, sql FROM sqlite_master
             WHERE type = 'table'
               AND name NOT LIKE 'sqlite_%'
               AND name != '_lbc_meta'
             ORDER BY name",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(SchemaRow {
                name: row.get(0)?,
                ddl: row.get::<_, String>(1).unwrap_or_default(),
            });
        }
        Ok(out)
    })
}

async fn exec_sql(node: &NodeHandle, sql: &str) -> Result<SqlOutcome> {
    if sql.is_empty() {
        anyhow::bail!("empty query");
    }
    if is_read_only(sql) {
        let db = node.db();
        let db = db.lock().await;
        let sql = sql.to_string();
        db.store()
            .with_read(move |conn| query_rows(conn, &sql))
            .map(|(columns, rows)| SqlOutcome::Query { columns, rows })
    } else {
        let db = node.db();
        let db = db.lock().await;
        if !db.is_leader() {
            anyhow::bail!("not the leader");
        }
        let sql = sql.to_string();
        let durable = db.write(move |conn| {
            let n = conn.execute(&sql, [])?;
            Ok(n)
        })?;
        drop(db);
        let rows = durable.committed().await?;
        Ok(SqlOutcome::Write { rows })
    }
}

fn is_read_only(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("SELECT")
        || upper.starts_with("EXPLAIN")
        || upper.starts_with("PRAGMA")
        || (upper.starts_with("WITH")
            && !upper.contains(" INSERT ")
            && !upper.contains(" UPDATE ")
            && !upper.contains(" DELETE "))
}

fn query_rows(conn: &Connection, sql: &str) -> Result<(Vec<String>, Vec<Vec<String>>)> {
    let mut stmt = conn.prepare(sql).context("prepare")?;
    let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let values: Vec<String> = (0..row.as_ref().column_count())
            .map(|i| cell_to_string(row, i))
            .collect();
        out.push(values);
    }
    Ok((columns, out))
}

fn cell_to_string(row: &rusqlite::Row<'_>, i: usize) -> String {
    match row.get_ref(i) {
        Ok(v) => v
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|_| row.get::<_, String>(i).unwrap_or_else(|_| "?".into())),
        Err(_) => "?".into(),
    }
}
