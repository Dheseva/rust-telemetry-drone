use axum::{
    Json, Router,
    extract::{MatchedPath, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use tower_http::trace::TraceLayer;
// use opentelemetry::global;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use std::net::SocketAddr;
use tracing::Instrument;
use tracing::field::Empty;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

#[derive(Debug)]
enum AppError {
    NotFound,
    Database(sqlx::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "User not found").into_response(),
            AppError::Database(error) => {
                tracing::error!(error = %error, "Database error");
                (StatusCode::INTERNAL_SERVER_ERROR, "Database error").into_response()
            }
        }
    }
}

#[tokio::main]

async fn main() {
    dotenvy::dotenv().ok();

    let tracer_provider = init_tracer();
    let tracer = tracer_provider.tracer("rust-telemetry");
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::FULL),
        )
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(
                    tracing_subscriber::filter::Targets::new()
                        .with_target("app", tracing::Level::TRACE)
                        .with_target("rust_telemetry", tracing::Level::TRACE),
                ),
        )
        .init();
    tracing::info!("tracing system initialized");
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
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<_>| {
                    let route = request
                        .extensions()
                        .get::<MatchedPath>()
                        .map(|path| path.as_str())
                        .unwrap_or("<unknown>");

                    tracing::info_span!(
                        "http_request",
                        http.method = %request.method(),
                        http.route = %route,
                        http.status_code = Empty,

                    )
                })
                .on_response(
                    |response: &axum::response::Response, _latency, span: &tracing::Span| {
                        span.record("http.status_code", response.status().as_u16());

                        tracing::info!(
                            parent: span,
                            status = %response.status(),
                            "HTTP request completed"
                        );
                    },
                ),
        )
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Server running on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    tracer_provider.shutdown().ok();
}

#[tracing::instrument(target = "app")]
async fn health() -> &'static str {
    tracing::info!("health check started");

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    tracing::info!("health check finished");
    "OK"
}

#[tracing::instrument(target = "app", skip(state))]
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

#[tracing::instrument(target = "app", skip(state))]
async fn get_user(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<User>, AppError> {
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
    .map_err(|error| {
        let context = tracing::Span::current().context();
        let span = context.span();
        let span_context = span.span_context();
        tracing::error!(
        error = %error,
        user_id = id,
        trace_id = %span_context.trace_id(),
        span_id = %span_context.span_id(),
        "database query failed");
        tracing::Span::current().set_status(opentelemetry::trace::Status::error(error.to_string()));
        if matches!(error, sqlx::Error::RowNotFound) {
            AppError::NotFound
        } else {
            AppError::Database(error)
        }
    })?;
    //tracing::info!("user fetched successfully");
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

fn init_tracer() -> SdkTracerProvider {
    use opentelemetry_otlp::WithExportConfig;

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint("http://127.0.0.1:4317")
        .build()
        .expect("Failed to create OTLP exporter");

    let resource = Resource::builder()
        .with_service_name("rust-telemetry")
        .build();

    SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build()
}
