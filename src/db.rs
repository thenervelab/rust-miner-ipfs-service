use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{Row, SqlitePool};

pub async fn init(path: &str) -> Result<SqlitePool> {
    let pool = SqlitePool::connect(&format!("sqlite://{}", path)).await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS pinned_cids (
            cid TEXT PRIMARY KEY,
            pinned_at TIMESTAMP NOT NULL,
            last_seen_profile INTEGER NOT NULL
        );
    "#,
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS service_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            current_profile_cid TEXT,
            updated_at TIMESTAMP NOT NULL
        );
    "#,
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        INSERT OR IGNORE INTO service_state (id, current_profile_cid, updated_at)
        VALUES (1, NULL, datetime('now'))
    "#,
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cid TEXT,
            action TEXT NOT NULL, -- pin|unpin|fetch_profile
            error TEXT NOT NULL,
            occurred_at TIMESTAMP NOT NULL
        );
    "#,
    )
    .execute(&pool)
    .await?;

    Ok(pool)
}

pub async fn record_pin(pool: &SqlitePool, cid: &str, seen_now: bool) -> Result<()> {
    let now = DateTime::<Utc>::from(std::time::SystemTime::now());
    let seen = if seen_now { 1 } else { 0 };
    sqlx::query("INSERT OR REPLACE INTO pinned_cids (cid, pinned_at, last_seen_profile) VALUES (?1, COALESCE((SELECT pinned_at FROM pinned_cids WHERE cid=?1), ?2), ?3)")
        .bind(cid)
        .bind(now)
        .bind(seen)
        .execute(pool).await?;
    Ok(())
}

pub async fn mark_all_unseen(pool: &SqlitePool) -> Result<()> {
    sqlx::query("UPDATE pinned_cids SET last_seen_profile = 0")
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn mark_seen(pool: &SqlitePool, cid: &str) -> Result<()> {
    sqlx::query("UPDATE pinned_cids SET last_seen_profile = 1 WHERE cid=?1")
        .bind(cid)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn to_unpin(pool: &SqlitePool) -> Result<Vec<String>> {
    let rows = sqlx::query("SELECT cid FROM pinned_cids WHERE last_seen_profile = 0")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|r| r.get::<String, _>(0)).collect())
}

pub async fn delete_cid(pool: &SqlitePool, cid: &str) -> Result<()> {
    sqlx::query("DELETE FROM pinned_cids WHERE cid=?1")
        .bind(cid)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn set_profile(pool: &SqlitePool, cid: Option<&str>) -> Result<()> {
    sqlx::query(
        "UPDATE service_state SET current_profile_cid=?1, updated_at=datetime('now') WHERE id=1",
    )
    .bind(cid)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_profile(pool: &SqlitePool) -> Result<Option<String>> {
    let row = sqlx::query("SELECT current_profile_cid FROM service_state WHERE id=1")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<Option<String>, _>(0).ok().flatten())
}

pub async fn record_failure(
    pool: &SqlitePool,
    cid: Option<&str>,
    action: &str,
    error: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO failures (cid, action, error, occurred_at) VALUES (?1, ?2, ?3, datetime('now'))")
        .bind(cid)
        .bind(action)
        .bind(error)
        .execute(pool).await?;
    Ok(())
}

pub async fn show_state(pool: &SqlitePool) -> Result<()> {
    let prof = get_profile(pool).await?;
    println!("current_profile: {:?}", prof);
    let rows = sqlx::query("SELECT cid, pinned_at FROM pinned_cids ORDER BY pinned_at DESC")
        .fetch_all(pool)
        .await?;
    for r in rows {
        println!(
            "pinned: {} at {}",
            r.get::<String, _>(0),
            r.get::<String, _>(1)
        );
    }
    Ok(())
}
