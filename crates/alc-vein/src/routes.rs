//! 指静脈テンプレートの口 (Refs ippoan/vein-match#20)。
//!
//! - `PUT /vein/templates/{employee_id}` `{"charas": [hex, ...]}` → 登録し直す (キオスクからも)
//! - `PUT /vein/templates/{employee_id}/module` `{"record": base64}` → 端末のモジュールの記録全体を預かる
//! - `GET /vein/module-templates` → 承認済みのモジュールの記録の一覧 (端末が書き戻して、モジュール自身に照合させる)
//! - `POST /vein/identify` `{"chara": hex}` → 1:N 照合 + 学習の書き戻し
//! - `GET /vein/templates` → オフライン照合用の全件 (`logic_version` つき)
//! - `DELETE /vein/templates/{employee_id}` → 登録の削除 (承認待ちの却下・承認済みの削除もこれ。管理者だけ)
//! - `GET /vein/templates/pending` → 承認待ちの一覧 (管理者だけ)
//! - `GET /vein/templates/approved` → 承認済みの一覧 (管理者だけ。テンプレートは返さない)
//! - `POST /vein/templates/{employee_id}/approve` `{"updated_at": ...}` → 承認 (管理者だけ)
//!
//! モジュールの記録 (`template_format = 'module'`) はサーバーで作り直さず・照合もしない: 検査は base64 と長さだけで、
//! 中身は解釈しない (生体データなのでログにも出さない)。`chara` の行 (identify・`GET /vein/templates`) とは
//! 形式の列で分け、identify の候補・学習の書き戻し・旧一覧には載せない。
//!
//! 登録 (PUT) は経路によらず承認待ちで、照合 (identify) と `GET /vein/templates` は承認済みだけを使う
//! (なりすまし対策。Refs ippoan/alc-app#436)。承認待ち・承認済みの一覧と承認と削除は `AuthUser` (`X-User-Role`) の
//! `role == "admin"` を見る (`AuthUser` が無ければ 403)。他の口は `TenantId` の Extension だけを
//! 見て、役割 (キオスクに開く口) は auth-worker の許可表が決める。

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post, put},
    Extension, Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use alc_core_wasm::api_error::{internal_error, not_found, unprocessable, ApiError};
use alc_core_wasm::{AuthUser, TenantId};

use base64::{engine::general_purpose::STANDARD, Engine};

use crate::matcher::{self, CharaError, MAX_TEMPLATES};
use crate::repo::{ModuleRecordItem, VeinTemplateItem, FORMAT_CHARA, FORMAT_MODULE};
use crate::VeinState;

/// モジュールの記録 1 件の大きさの上限 (バイト。モジュールの READ_ENROLL の全体は実機で 8156 バイト)。
pub const MAX_MODULE_RECORD_BYTES: usize = 0x2000;

/// テナントのモジュールの記録の人数の上限 (端末のモジュールの本番の ID は 10〜100 の 91 個)。
pub const MAX_MODULE_RECORDS: usize = 91;

pub fn tenant_router() -> Router<VeinState> {
    Router::new()
        .route("/vein/templates", get(list_templates))
        .route("/vein/module-templates", get(list_module_templates))
        .route("/vein/templates/pending", get(list_pending))
        .route("/vein/templates/approved", get(list_approved))
        .route(
            "/vein/templates/{employee_id}/approve",
            post(approve_template),
        )
        .route(
            "/vein/templates/{employee_id}/module",
            put(upsert_module_record),
        )
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
struct ModuleRecordBody {
    record: String,
}

#[derive(Deserialize)]
struct ApproveBody {
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct IdentifyBody {
    chara: String,
}

fn chara_error(e: CharaError) -> ApiError {
    unprocessable(e.code(), e.message())
}

/// 管理者だけの口 (承認・一覧・削除) の認可。`AuthUser` が無い (端末の鍵など) か `role != "admin"` なら 403。
/// 返すのは承認した管理者の user_id。
fn require_admin(user: Option<Extension<AuthUser>>) -> Result<Uuid, ApiError> {
    match user {
        Some(Extension(u)) if u.role == "admin" => Ok(u.user_id),
        _ => Err((
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "forbidden",
                "message": "管理者だけが使えます",
            })),
        )),
    }
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
        .registration_count(tenant_id, employee_id, FORMAT_CHARA)
        .await
        .map_err(|e| internal_error("vein registration_count", e))?;
    matcher::check_capacity(registered as usize + usize::from(!enrolled)).map_err(|_| {
        let message = format!("登録が上限の {MAX_TEMPLATES} 人に達しています");
        unprocessable("too_many_templates", &message)
    })?;
    let updated_at = state
        .templates
        .upsert(tenant_id, employee_id, &template, FORMAT_CHARA)
        .await
        .map_err(|e| internal_error("vein upsert_template", e))?
        .ok_or_else(|| not_found("employee_not_found"))?;
    Ok(Json(
        json!({ "employee_id": employee_id, "updated_at": updated_at }),
    ))
}

/// 端末のモジュールの記録を base64 のまま預かる。検査は base64 として読めることと長さ (1〜[`MAX_MODULE_RECORD_BYTES`]
/// バイト) だけ。新規の登録 (その乗務員がまだ module の行を持たない) で [`MAX_MODULE_RECORDS`] 人を超えるなら 409。
async fn upsert_module_record(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
    Json(body): Json<ModuleRecordBody>,
) -> Result<Json<Value>, ApiError> {
    let len = STANDARD
        .decode(&body.record)
        .map_err(|_| unprocessable("invalid_module_record", "記録が base64 として読めません"))?
        .len();
    if len == 0 {
        return Err(unprocessable("invalid_module_record", "記録が空です"));
    }
    if len > MAX_MODULE_RECORD_BYTES {
        let message = format!("記録が大きすぎます ({MAX_MODULE_RECORD_BYTES} バイトまで)");
        return Err(unprocessable("module_record_too_long", &message));
    }
    let (registered, enrolled) = state
        .templates
        .registration_count(tenant_id, employee_id, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein module registration_count", e))?;
    if !enrolled && registered as usize >= MAX_MODULE_RECORDS {
        let message = format!("モジュールの記録が上限の {MAX_MODULE_RECORDS} 人に達しています");
        return Err((
            StatusCode::CONFLICT,
            Json(json!({ "error": "too_many_module_records", "message": message })),
        ));
    }
    let updated_at = state
        .templates
        .upsert(tenant_id, employee_id, &body.record, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein upsert_module_record", e))?
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
        .list(tenant_id, FORMAT_CHARA)
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
        .list(tenant_id, FORMAT_CHARA)
        .await
        .map_err(|e| internal_error("vein list_templates", e))?;
    let templates: Vec<VeinTemplateItem> = rows.into_iter().map(Into::into).collect();
    Ok(Json(json!({
        "logic_version": vein_match_search::VERSION,
        "templates": templates,
    })))
}

/// 承認済みのモジュールの記録の一覧 (端末のモジュールへ書き戻す元)。
async fn list_module_templates(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    let rows = state
        .templates
        .list(tenant_id, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein list_module_templates", e))?;
    let templates: Vec<ModuleRecordItem> = rows.into_iter().map(Into::into).collect();
    Ok(Json(json!({ "templates": templates })))
}

async fn list_pending(
    state: State<VeinState>,
    tenant: Extension<TenantId>,
    user: Option<Extension<AuthUser>>,
) -> Result<Json<Value>, ApiError> {
    list_by_approval(state, tenant, user, false).await
}

async fn list_approved(
    state: State<VeinState>,
    tenant: Extension<TenantId>,
    user: Option<Extension<AuthUser>>,
) -> Result<Json<Value>, ApiError> {
    list_by_approval(state, tenant, user, true).await
}

/// 承認待ち (`approved` = false) か承認済み (true) の一覧。管理者だけ。
async fn list_by_approval(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    user: Option<Extension<AuthUser>>,
    approved: bool,
) -> Result<Json<Value>, ApiError> {
    require_admin(user)?;
    let items = state
        .templates
        .list_by_approval(tenant_id, approved)
        .await
        .map_err(|e| internal_error("vein list_by_approval", e))?;
    Ok(Json(json!(items)))
}

async fn approve_template(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    user: Option<Extension<AuthUser>>,
    Path(employee_id): Path<Uuid>,
    Json(body): Json<ApproveBody>,
) -> Result<StatusCode, ApiError> {
    let approved_by = require_admin(user)?;
    let approved = state
        .templates
        .approve(tenant_id, employee_id, approved_by, body.updated_at)
        .await
        .map_err(|e| internal_error("vein approve_template", e))?;
    if approved {
        Ok(StatusCode::NO_CONTENT)
    } else {
        // 版が違う (登録し直された) か、承認待ちでない (承認済み・登録が無い)
        Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": "approve_conflict",
                "message": "承認待ちの登録が変わっています。一覧を読み直してください",
            })),
        ))
    }
}

async fn delete_template(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    user: Option<Extension<AuthUser>>,
    Path(employee_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_admin(user)?;
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
