//! `GET /internal/db-role` — この Worker の DB 接続がどのロールで繋がっているかを返す
//! (Refs ippoan/auth-worker#605)。
//!
//! vein が触る表には FORCE ROW LEVEL SECURITY が無いものがあり、表の所有者で繋ぐと RLS が
//! 掛からない。実行用ロール ([`RUNTIME_ROLE`]、非所有者) で繋いでいるかを、auth-worker が
//! Service Binding で 1 回呼んで確かめるための口。
//!
//! - **引数を取らない。** path・query・header・body を読まず、流すのは `SELECT current_user` の
//!   1 文だけ (書き込み・`SET`・トランザクションなし)。
//! - **tenant ヘッダーを要求する layer の外・`/api` の nest の外に置く** (`lib.rs` の `router`)。
//!   auth-worker の proxy がブラウザ・端末から転送するのは `/api/vein` で始まる path だけなので、
//!   この path には proxy からは届かず、auth-worker のコードが binding で直接呼んだときだけ届く。
//!   **path を `/api/…` や `/vein/…` にしないこと。**
//! - 返すのはロール名と真偽だけ。接続文字列・ホスト・DB 名・版・エラーの詳細は、応答にも
//!   ログにも出さない。

use std::sync::Arc;

use alc_core_wasm::api_error::internal_error;
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderValue, Response};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use worker::console_error;

use crate::repo::WorkerVeinTemplatesRepository;

/// 本番の実行用ロール (表の所有者ではないので、FORCE の無い表でも RLS が掛かる)
const RUNTIME_ROLE: &str = "alc_api_rt";

pub fn internal_router(repo: Arc<WorkerVeinTemplatesRepository>) -> Router {
    Router::new()
        .route("/internal/db-role", get(db_role))
        .with_state(repo)
}

fn is_runtime_role(current_user: &str) -> bool {
    current_user == RUNTIME_ROLE
}

/// JSON の文字列リテラルにする (ロール名は引用符付きの識別子なら任意の文字を含みうる)。
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

async fn db_role(State(repo): State<Arc<WorkerVeinTemplatesRepository>>) -> Response<Body> {
    let Some(current_user) = repo.current_user().await else {
        // DB のエラー文は応答にもログにも出さない (固定の文言だけ)
        console_error!("vein db-role: query failed");
        return internal_error("vein db_role", "query failed").into_response();
    };
    let body = format!(
        r#"{{"current_user":{},"is_runtime_role":{}}}"#,
        json_string(&current_user),
        is_runtime_role(&current_user)
    );
    let mut resp = Response::new(Body::from(body));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    resp
}
