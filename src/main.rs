use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use std::net::SocketAddr;
use tracing::Instrument;

#[derive(Clone)]
struct AppState {
    db: PgPool,
}
#[derive(Debug, Serialize, FromRow)]
struct User {
    id: i64,
    name: String,
    email: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct CreateUser {
    name: String,
    email: String,
}
#[tokio::main]

async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL)
        .init();
    tracing::info!("tracing system initialized");
    dotenvy::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = PgPool::connect(&database_url)
        .await
        .expect("Failed to connect to database");
    let state = AppState { db };
    let app = Router::new()
        .route("/health", get(health))
        .route("/users", get(get_users))
        .route("/users", post(create_user))
        .route("/users/{id}", get(get_user))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Server running on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[tracing::instrument]
async fn health() -> &'static str {
    tracing::info!("health check started");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    tracing::info!("health check finished");
    "OK"
}

#[tracing::instrument(skip(state))]
async fn get_users(State(state): State<AppState>) -> Result<Json<Vec<User>>, String> {
    let users = sqlx::query_as::<_, User>(
        r#"
        SELECT id, name, email, created_at
        FROM users
        ORDER BY id
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    Ok(Json(users))
}

#[tracing::instrument(skip(state))]
async fn get_user(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<User>, String> {
    let query_span = tracing::info_span!("database_query", db.system = "postgresql", user_id = id);
    let user = async {
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

        sqlx::query_as::<_, User>(
            r#"
        SELECT id, name, email, created_at
        FROM users
        WHERE id = $1
        "#,
        )
        .bind(id)
        .fetch_one(&state.db)
        .await
    }
    .instrument(query_span)
    .await
    .map_err(|error| error.to_string())?;

    tracing::info!("user fetched successfully");

    Ok(Json(user))
}

async fn create_user(
    State(state): State<AppState>,
    Json(payload): Json<CreateUser>,
) -> Result<Json<User>, String> {
    let user = sqlx::query_as::<_, User>(
        r#"
        INSERT INTO users (name, email)
        VALUES ($1, $2)
        RETURNING id, name, email, created_at
        "#,
    )
    .bind(payload.name)
    .bind(payload.email)
    .fetch_one(&state.db)
    .await
    .map_err(|error| error.to_string())?;

    Ok(Json(user))
}
