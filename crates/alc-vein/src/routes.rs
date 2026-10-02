//! 指静脈テンプレートの 4 本の口 (Refs ippoan/vein-match#20)。
//!
//! - `PUT /vein/templates/{employee_id}` `{"charas": [hex, ...]}` → 登録し直す (キオスクからも)
//! - `POST /vein/identify` `{"chara": hex}` → 1:N 照合 + 学習の書き戻し
//! - `GET /vein/templates` → オフライン照合用の全件 (`logic_version` つき)
//! - `DELETE /vein/templates/{employee_id}` → 登録の削除
//!
//! 認可は `alc-misc` の `employees::update_face` と同じ形で `TenantId` の Extension だけを
//! 見る。役割 (キオスクに DELETE を開かない等) は auth-worker の許可表が決める。

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{post, put},
    Extension, Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use alc_core_wasm::api_error::{internal_error, not_found, unprocessable, ApiError};
use alc_core_wasm::TenantId;

use crate::matcher::{self, CharaError, MAX_TEMPLATES};
use crate::repo::VeinTemplateItem;
use crate::VeinState;

pub fn tenant_router() -> Router<VeinState> {
    Router::new()
        .route("/vein/templates", axum::routing::get(list_templates))
        .route(
            "/vein/templates/{employee_id}",
            put(upsert_template).delete(delete_template),
        )
        .route("/vein/identify", post(identify))
}

#[derive(Deserialize)]
struct UpsertBody {
    charas: Vec<String>,
}

#[derive(Deserialize)]
struct IdentifyBody {
    chara: String,
}

fn chara_error(e: CharaError) -> ApiError {
    unprocessable(e.code(), e.message())
}

async fn upsert_template(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
    Json(body): Json<UpsertBody>,
) -> Result<Json<Value>, ApiError> {
    let charas = body
        .charas
        .iter()
        .map(|c| matcher::decode_chara(c))
        .collect::<Result<Vec<_>, _>>()
        .map_err(chara_error)?;
    let template = matcher::enroll_template(&charas).map_err(|_| {
        unprocessable(
            "invalid_chara_count",
            "登録の特徴量は 1〜6 件で送ってください",
        )
    })?;
    // 新規登録で上限を超えさせない (上書きは人数が増えないので通す)
    let (registered, enrolled) = state
        .templates
        .registration_count(tenant_id, employee_id)
        .await
        .map_err(|e| internal_error("vein registration_count", e))?;
    matcher::check_capacity(registered as usize + usize::from(!enrolled)).map_err(|_| {
        let message = format!("登録が上限の {MAX_TEMPLATES} 人に達しています");
        unprocessable("too_many_templates", &message)
    })?;
    let updated_at = state
        .templates
        .upsert(tenant_id, employee_id, &template)
        .await
        .map_err(|e| internal_error("vein upsert_template", e))?
        .ok_or_else(|| not_found("employee_not_found"))?;
    Ok(Json(
        json!({ "employee_id": employee_id, "updated_at": updated_at }),
    ))
}

async fn identify(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Json(body): Json<IdentifyBody>,
) -> Result<Json<Value>, ApiError> {
    let chara = matcher::decode_chara(&body.chara).map_err(chara_error)?;
    let rows = state
        .templates
        .list(tenant_id)
        .await
        .map_err(|e| internal_error("vein identify list", e))?;
    let templates: Vec<&str> = rows.iter().map(|r| r.template.as_str()).collect();
    let found =
        matcher::identify(&templates, &chara, chrono::Utc::now().timestamp()).map_err(|e| {
            let message = format!(
                "登録が {} 人あり、1:N 照合の上限 {MAX_TEMPLATES} 人を超えています",
                e.0
            );
            unprocessable("too_many_templates", &message)
        })?;
    for &i in &found.unreadable {
        let employee_id = rows[i].employee_id;
        tracing::warn!("vein identify: {employee_id} のテンプレートを読めず照合から外した");
    }
    let Some(hit) = found.hit else {
        return Ok(Json(json!({ "employee_id": null })));
    };
    let row = &rows[hit.index];
    // 学習は次の照合でまた起きるので、書き戻しの競合 (0 行) や失敗は捨ててよい
    let written = state
        .templates
        .update_learned(tenant_id, row.id, &hit.learned, row.updated_at)
        .await;
    // Ok(false) = 間に登録し直し等が入った競合で、書き戻しを捨てた
    let employee_id = row.employee_id;
    tracing::info!("vein identify: {employee_id} learned write-back {written:?}");
    Ok(Json(
        json!({ "employee_id": row.employee_id, "name": row.name }),
    ))
}

async fn list_templates(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    let rows = state
        .templates
        .list(tenant_id)
        .await
        .map_err(|e| internal_error("vein list_templates", e))?;
    let templates: Vec<VeinTemplateItem> = rows.into_iter().map(Into::into).collect();
    Ok(Json(json!({
        "logic_version": vein_match_search::VERSION,
        "templates": templates,
    })))
}

async fn delete_template(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let deleted = state
        .templates
        .delete(tenant_id, employee_id)
        .await
        .map_err(|e| internal_error("vein delete_template", e))?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(not_found("vein_template_not_found"))
    }
}

#[cfg(test)]
#[path = "routes_tests.rs"]
mod tests;
