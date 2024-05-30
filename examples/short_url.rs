use std::{thread::sleep, time::Duration};

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Redirect},
    routing::{get, post},
    Json, Router,
};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use tokio::net::TcpListener;
use tracing::{error, info, level_filters::LevelFilter, warn};
use tracing_subscriber::{fmt::Layer, layer::SubscriberExt, util::SubscriberInitExt, Layer as _};

#[derive(Debug, Serialize, Deserialize)]
struct Request {
    origin_url: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    shortten_url: String,
}

#[derive(Debug, Clone)]
struct AppState {
    pg_pool: PgPool,
}

#[derive(Debug, FromRow)]
struct RowData {
    #[sqlx(default)]
    url_id: String,
    #[sqlx(default)]
    url: String,
}

#[derive(Debug, thiserror::Error)]
enum MyError {
    #[error("error with database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("not found url with id: {0}")]
    NotFound(String),
}

impl IntoResponse for MyError {
    fn into_response(self) -> axum::response::Response {
        match &self {
            MyError::Database(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
            }
            MyError::NotFound(_) => (StatusCode::NOT_FOUND, self.to_string()).into_response(),
        }
    }
}

const HTTP_ADDR: &str = "localhost:3000";

impl AppState {
    async fn try_new(postgres_url: &str) -> Result<AppState> {
        let state = Self {
            pg_pool: PgPool::connect(postgres_url).await?,
        };
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS urls (
                url_id CHAR(6) PRIMARY KEY,
                url TEXT NOT NULL UNIQUE
            )
            "#,
        )
        .execute(&state.pg_pool)
        .await?;
        Ok(state)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let layer = Layer::new().with_filter(LevelFilter::INFO);
    tracing_subscriber::registry().with(layer).init();

    let state = AppState::try_new("postgres://postgres:example@10.0.0.82:5432/short-url").await?;

    let app = Router::new()
        .route("/", post(shortten_url_handler))
        .route("/:url_id", get(redirect_handler))
        .with_state(state);

    let addr = "0.0.0.0:3000";
    let listener = TcpListener::bind(addr).await?;

    info!("Server started on {}", addr);
    axum::serve(listener, app.into_make_service()).await?;

    Ok(())
}

async fn redirect_handler(
    State(state): State<AppState>,
    Path(url_id): Path<String>,
) -> Result<impl IntoResponse, MyError> {
    let sql = r#"
    SELECT * FROM urls
          WHERE url_id = $1
    "#;

    let result: std::result::Result<RowData, sqlx::Error> = sqlx::query_as(sql)
        .bind(&url_id)
        .fetch_one(&state.pg_pool)
        .await;

    match result {
        Ok(row) => Ok(Redirect::to(&row.url)),
        Err(e) => {
            warn!("Got an error while fetching url {}", e);
            Err(MyError::NotFound(url_id))
        }
    }
}

async fn shortten_url_handler(
    State(state): State<AppState>,
    Json(request): Json<Request>,
) -> Result<impl IntoResponse, MyError> {
    let result = shortten_url(&request.origin_url, &state).await;
    result.map(|url_id| {
        Json(Response {
            shortten_url: format!("http://{}/{}", HTTP_ADDR, url_id),
        })
    })
}

async fn shortten_url(url: &str, state: &AppState) -> Result<String, MyError> {
    let sql = r#"
    INSERT INTO urls(url_id, url)
          VALUES ($1, $2)
          ON CONFLICT (url) DO UPDATE SET url = EXCLUDED.url
          RETURNING url_id
    "#;

    loop {
        //let url_id = "111111";
        let url_id = nanoid!(6);
        let data: std::result::Result<RowData, sqlx::Error> = sqlx::query_as(sql)
            .bind(&url_id)
            .bind(url)
            .fetch_one(&state.pg_pool)
            .await;
        match data.map(|row| row.url_id) {
            Ok(id) => return Ok(id),
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                warn!("duplicate key {} will try again.", url_id);
                sleep(Duration::from_millis(200));
            }
            Err(e) => {
                error!("Got an error while insert url id and url {}", e);
                return Err(MyError::Database(e));
            }
        }
    }
}
