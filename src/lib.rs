//! 指静脈 (vein) の Worker (Refs ippoan/rust-alc-api#680 / ippoan/rust-alc-api#683 / ippoan/rust-alc-api#691)。crates/alc-vein の 4 本の口
//! (`routes::tenant_router`、axum) を workers-rs (`http` / `axum` feature) に載せ、
//! repo だけを tokio-postgres の実装 ([`repo::WorkerVeinTemplatesRepository`]) に差し替える。
//! DB への経路 (staging = Container 内の PgBouncer、本番 = Supabase のプーラー) は
//! [`db::connect`] の 1 か所で出し分ける。monolith と同じく `/api` 付きでも受ける。
//! それとは別に、接続のロール名を返す `GET /internal/db-role` ([`db_role`]) を素にだけ持つ。
//!
//! **この Worker は JWT を検証せず、auth-worker が付け直した tenant ヘッダーを信頼する**
//! (`alc_core_wasm::require_tenant_header`)。本番の到達経路は auth-worker からの Service Binding
//! だけで、`workers_dev` / `preview_urls` / `routes` を持たない (wrangler.toml と
//! scripts/check-exposure.sh が保証する。ippoan/rust-alc-api#556 と同じ穴を開けないため)。
//! staging だけはテストから叩くため `workers_dev = true` で、その workers.dev は Cloudflare Access で
//! 保護する (Access を通らないリクエストは Worker に届かない。README 参照)。

mod db;
mod db_role;
mod repo;
mod tcp;
mod vein_db;

use std::sync::Arc;

use alc_core_wasm::require_tenant_header;
use alc_vein::{routes::tenant_router, VeinState};
use axum::body::Body;
use axum::http::{HeaderValue, Response, StatusCode};
use axum::{middleware, Router};
use tower_service::Service;
use worker::{
    console_error, event, Context, Date, Env, HttpRequest, Result, WorkerVersionMetadata,
};

use crate::repo::WorkerVeinTemplatesRepository;

pub use crate::vein_db::VeinDb;

/// `internal` (tenant ヘッダーを要求しない口、[`db_role`]) は**素にだけ** merge し、`/api` の nest には
/// 出さない (auth-worker の proxy が転送する `/api/vein/…` から届かないようにするため)。
///
/// **`internal` は `vein` より先に merge する。** axum の `merge` は fallback を後から merge した側の
/// もので置き換えるので、後に置くと、どの route にも当たらない path の fallback から tenant の layer が
/// 外れる (ヘッダー無しの未定義の path が 401 でなく 404 になる)。
fn router(state: VeinState, internal: Router) -> Router {
    let vein = tenant_router()
        .layer(middleware::from_fn(require_tenant_header))
        .with_state(state);
    Router::new()
        .merge(internal)
        .merge(vein.clone())
        .nest("/api", vein)
}

fn error_response(status: StatusCode, code: &str) -> Response<Body> {
    let mut resp = Response::new(Body::from(format!(r#"{{"error":"{code}"}}"#)));
    *resp.status_mut() = status;
    resp
}

#[event(fetch)]
async fn fetch(req: HttpRequest, env: Env, _ctx: Context) -> Result<Response<Body>> {
    let started = Date::now().as_millis();
    let client = match db::connect(&env).await {
        Ok(c) => c,
        Err(db::ConnectError::NotConfigured) => {
            console_error!("vein: {}", db::ConnectError::NotConfigured);
            return Ok(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_not_configured",
            ));
        }
        Err(e) => {
            console_error!("vein: {e}");
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ));
        }
    };
    let connect_ms = Date::now().as_millis() - started;
    let repo = Arc::new(WorkerVeinTemplatesRepository::new(client));
    let state = VeinState {
        templates: repo.clone(),
    };
    let internal = db_role::internal_router(repo.clone());
    let mut resp = match router(state, internal).call(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    // 測定用 (ippoan/rust-alc-api#683): 接続・DB (repo の全メソッドの合計)・それ以外 (照合など Worker の CPU)。
    // ローカル (wrangler dev) の Date.now は CPU 実行中も進むが、Cloudflare 上では
    // Spectre 対策で I/O まで止まるので app は 0 に寄る (本番の CPU 時間は dashboard で見る)
    let db_ms = repo.db_ms();
    let app_ms = (Date::now().as_millis() - started).saturating_sub(connect_ms + db_ms);
    let timing = format!("connect;dur={connect_ms}, db;dur={db_ms}, app;dur={app_ms}");
    if let Ok(v) = HeaderValue::from_str(&timing) {
        resp.headers_mut().insert("server-timing", v);
    }
    // どの版が応えたかを応答ヘッダーで分かるようにする (ippoan/rust-alc-api#697)。binding が取れなければ何も付けない
    if let Ok(meta) = env.get_binding::<WorkerVersionMetadata>("CF_VERSION_METADATA") {
        if let Ok(v) = HeaderValue::from_str(&meta.id()) {
            resp.headers_mut().insert("x-worker-version", v);
        }
        let tag = meta.tag();
        if !tag.is_empty() {
            if let Ok(v) = HeaderValue::from_str(&tag) {
                resp.headers_mut().insert("x-worker-tag", v);
            }
        }
    }
    Ok(resp)
}
