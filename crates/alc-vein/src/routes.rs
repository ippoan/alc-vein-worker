//! 指静脈テンプレートの口 (Refs ippoan/vein-match#20)。
//!
//! - `PUT /vein/templates/{employee_id}` `{"charas": [hex, ...]}` → 登録し直す (キオスクからも)
//! - `PUT /vein/templates/{employee_id}/module` `{"record": base64}` → 端末のモジュールの記録全体を預かる
//! - `PUT /vein/templates/{employee_id}/module/devices` `{"record": base64}` → その端末で撮った記録を端末ごとに足す
//!   (`X-Device-Id` 必須。承認済みの module の行が在る乗務員だけ。承認は要らない。Refs ippoan/alc-app#558)
//! - `GET /vein/module-templates` → 承認済みのモジュールの記録の一覧 (端末が書き戻して、モジュール自身に照合させる)。
//!   `X-Device-Id` があれば、その端末向けに「その端末の 3 件 + 他の端末の最新の 3 件」へ混ぜて返す
//! - `GET /vein/module-templates/pending` → 承認待ちのモジュールの記録の一覧 (端末の token で。中身は返さない)
//! - `POST /vein/templates/{employee_id}/approve-by-employee` `{"approver_employee_id", "updated_at"}` → 運行管理者
//!   (社員) による承認 (端末の token で。承認者は端末の照合で決まり、ここで役割・登録・自己承認を検査する)
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
    http::{HeaderMap, StatusCode},
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
use crate::repo::{
    ApproveByEmployee, DeviceRecordRow, ModuleRecordItem, VeinTemplateItem, VeinTemplateRow,
    FORMAT_CHARA, FORMAT_MODULE,
};
use crate::VeinState;

/// モジュールの記録 1 件の大きさの上限 (バイト。モジュールの READ_ENROLL の全体は実機で 8156 バイト)。
pub const MAX_MODULE_RECORD_BYTES: usize = 0x2000;

/// テナントのモジュールの記録の人数の上限 (端末のモジュールの本番の ID は 10〜100 の 91 個)。
pub const MAX_MODULE_RECORDS: usize = 91;

/// 端末向けに混ぜる記録の、1 つの出どころから取る件数 (自分の 3 件 + 他の端末の 3 件 = 記録の上限の 6 件)。
const MIX_TAKE: usize = 3;

pub fn tenant_router() -> Router<VeinState> {
    Router::new()
        .route("/vein/templates", get(list_templates))
        .route("/vein/module-templates", get(list_module_templates))
        .route("/vein/module-templates/pending", get(list_pending_modules))
        .route("/vein/templates/pending", get(list_pending))
        .route("/vein/templates/approved", get(list_approved))
        .route(
            "/vein/templates/{employee_id}/approve",
            post(approve_template),
        )
        .route(
            "/vein/templates/{employee_id}/approve-by-employee",
            post(approve_by_employee),
        )
        .route(
            "/vein/templates/{employee_id}/module",
            put(upsert_module_record),
        )
        .route(
            "/vein/templates/{employee_id}/module/devices",
            put(upsert_device_record),
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
struct ApproveByEmployeeBody {
    approver_employee_id: Uuid,
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
        .upsert(tenant_id, employee_id, &template, FORMAT_CHARA, None)
        .await
        .map_err(|e| internal_error("vein upsert_template", e))?
        .ok_or_else(|| not_found("employee_not_found"))?;
    Ok(Json(
        json!({ "employee_id": employee_id, "updated_at": updated_at }),
    ))
}

/// 登録した端末の id を載せるヘッダ。auth-worker が端末の token で来た要求にだけ付ける (token の sub)。
const DEVICE_ID_HEADER: &str = "x-device-id";
const MAX_DEVICE_ID_LEN: usize = 64;

/// `X-Device-Id` を読む。無ければ `None`。1〜64 文字の `[A-Za-z0-9:_-]` 以外は 422 `invalid_device_id`。
fn registered_device_id(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    let Some(value) = headers.get(DEVICE_ID_HEADER) else {
        return Ok(None);
    };
    let valid = value.to_str().ok().filter(|s| {
        (1..=MAX_DEVICE_ID_LEN).contains(&s.len())
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'_' | b'-'))
    });
    valid.map(|s| Some(s.to_owned())).ok_or_else(|| {
        unprocessable(
            "invalid_device_id",
            "X-Device-Id は 1〜64 文字の英数字と : _ - だけで送ってください",
        )
    })
}

/// 記録の検査: base64 として読めて、長さが 1〜[`MAX_MODULE_RECORD_BYTES`] バイト。
fn validate_module_record(record: &str) -> Result<(), ApiError> {
    let len = STANDARD
        .decode(record)
        .map_err(|_| unprocessable("invalid_module_record", "記録が base64 として読めません"))?
        .len();
    if len == 0 {
        return Err(unprocessable("invalid_module_record", "記録が空です"));
    }
    if len > MAX_MODULE_RECORD_BYTES {
        let message = format!("記録が大きすぎます ({MAX_MODULE_RECORD_BYTES} バイトまで)");
        return Err(unprocessable("module_record_too_long", &message));
    }
    Ok(())
}

/// 端末のモジュールの記録を base64 のまま預かる。検査は base64 として読めることと長さ (1〜[`MAX_MODULE_RECORD_BYTES`]
/// バイト) だけ。新規の登録 (その乗務員がまだ module の行を持たない) で [`MAX_MODULE_RECORDS`] 人を超えるなら 409。
async fn upsert_module_record(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<ModuleRecordBody>,
) -> Result<Json<Value>, ApiError> {
    let device_id = registered_device_id(&headers)?;
    validate_module_record(&body.record)?;
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
        .upsert(
            tenant_id,
            employee_id,
            &body.record,
            FORMAT_MODULE,
            device_id.as_deref(),
        )
        .await
        .map_err(|e| internal_error("vein upsert_module_record", e))?
        .ok_or_else(|| not_found("employee_not_found"))?;
    Ok(Json(
        json!({ "employee_id": employee_id, "updated_at": updated_at }),
    ))
}

/// 追加の端末で撮った記録を、端末ごとに 1 行預かる (同じ端末なら差し替え)。`X-Device-Id` 必須 (422 `device_id_required`)。
/// 検査は [`upsert_module_record`] と同じ。承認済みの module の行が無い乗務員には足せない (409 `no_approved_module_record`)。
/// vein_templates の承認には触らず、承認も要らない (足したらすぐ [`list_module_templates`] で端末向けに混ざる)。
async fn upsert_device_record(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<ModuleRecordBody>,
) -> Result<Json<Value>, ApiError> {
    let Some(device_id) = registered_device_id(&headers)? else {
        return Err(unprocessable(
            "device_id_required",
            "X-Device-Id (端末の token) が要ります",
        ));
    };
    validate_module_record(&body.record)?;
    let updated_at = state
        .templates
        .upsert_device_record(tenant_id, employee_id, &device_id, &body.record)
        .await
        .map_err(|e| internal_error("vein upsert_device_record", e))?
        .ok_or_else(|| {
            (
                StatusCode::CONFLICT,
                Json(json!({
                    "error": "no_approved_module_record",
                    "message": "承認済みの module の記録が在る乗務員だけ、端末の記録を足せます",
                })),
            )
        })?;
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

/// 端末向けの記録の組み方のうち、混ぜる出どころ 1 つ (base64 の記録と、いつの記録か)。
struct MixSource<'a> {
    /// 撮った端末。元の行 (vein_templates) は登録した端末が分からないことがある。
    device_id: Option<&'a str>,
    /// 元の行 (vein_templates) か。端末ごとの記録 (vein_device_records) なら false。
    is_base: bool,
    record: &'a str,
    updated_at: DateTime<Utc>,
}

/// 元の行 `base` と、その乗務員の端末ごとの記録 `devices` から、端末 `requester` へ配る 1 件を組む
/// (Refs ippoan/alc-app#558)。
/// - 自分 = `requester` で撮った記録 (端末ごとの記録、無ければ元の行がその端末のもの)。
///   他 = 自分でない出どころのうち `requester` の端末でない最新 (`updated_at`) のもの。
/// - 自分と他が揃えば、自分の 3 件 + 他の 3 件に混ぜる (`updated_at` は 2 つの新しいほう)。
/// - 自分が無く端末ごとの記録が在れば、新しい順の 2 つを 3 件 + 3 件に混ぜる。
/// - 端末ごとの記録が無い・自分だけで他が無いときは `base` のまま。
/// - 読めない・混ぜられないときは warn を出して `base` のまま。
fn compose_for_device(
    base: VeinTemplateRow,
    devices: &[&DeviceRecordRow],
    requester: &str,
) -> ModuleRecordItem {
    let mut sources = vec![MixSource {
        device_id: base.registered_device_id.as_deref(),
        is_base: true,
        record: &base.template,
        updated_at: base.updated_at,
    }];
    sources.extend(devices.iter().map(|d| MixSource {
        device_id: Some(d.device_id.as_str()),
        is_base: false,
        record: &d.record,
        updated_at: d.updated_at,
    }));
    // 新しい順 (同時刻は元の行が先。sort は安定)
    sources.sort_by_key(|s| std::cmp::Reverse(s.updated_at));
    let from_requester = |s: &MixSource| s.device_id == Some(requester);
    let mine = sources
        .iter()
        .position(|s| !s.is_base && from_requester(s))
        .or_else(|| sources.iter().position(|s| s.is_base && from_requester(s)));
    let pair = match mine {
        Some(m) => sources
            .iter()
            .position(|s| !from_requester(s))
            .map(|o| (m, o)),
        None => (!devices.is_empty()).then_some((0, 1)),
    };
    let Some((a, b)) = pair else {
        return base.into();
    };
    let updated_at = sources[a].updated_at.max(sources[b].updated_at);
    match mix_records(sources[a].record, sources[b].record) {
        Some(record) => ModuleRecordItem {
            employee_id: base.employee_id,
            record,
            updated_at,
            registered_device_id: base.registered_device_id,
            composed: true,
        },
        None => {
            let employee_id = base.employee_id;
            tracing::warn!("vein module-templates: {employee_id} の記録を混ぜられず元のまま返す");
            base.into()
        }
    }
}

/// base64 の記録 2 つから先頭 [`MIX_TAKE`] 件ずつを取り、混ぜた記録を base64 で返す。読めなければ `None`。
fn mix_records(a: &str, b: &str) -> Option<String> {
    let a = STANDARD.decode(a).ok()?;
    let b = STANDARD.decode(b).ok()?;
    let mixed = vein_match_search::mix_templates(&[(&a, MIX_TAKE), (&b, MIX_TAKE)]).ok()?;
    Some(STANDARD.encode(mixed))
}

/// 承認済みのモジュールの記録の一覧 (端末のモジュールへ書き戻す元)。`X-Device-Id` があれば、その端末向けに
/// [`compose_for_device`] で組む (端末ごとの記録は 1 回の問い合わせで全員分)。無ければ (管理者のログイン) 元の行のまま。
async fn list_module_templates(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let requester = registered_device_id(&headers)?;
    let rows = state
        .templates
        .list(tenant_id, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein list_module_templates", e))?;
    let Some(requester) = requester else {
        let templates: Vec<ModuleRecordItem> = rows.into_iter().map(Into::into).collect();
        return Ok(Json(json!({ "templates": templates })));
    };
    let devices = state
        .templates
        .list_device_records(tenant_id)
        .await
        .map_err(|e| internal_error("vein list_device_records", e))?;
    let templates: Vec<ModuleRecordItem> = rows
        .into_iter()
        .map(|base| {
            let own: Vec<&DeviceRecordRow> = devices
                .iter()
                .filter(|d| d.employee_id == base.employee_id)
                .collect();
            compose_for_device(base, &own, &requester)
        })
        .collect();
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

/// 承認待ちのモジュールの記録の一覧 (端末の画面で承認する 1 人を選ぶ元)。管理者でなくてよい。
async fn list_pending_modules(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
) -> Result<Json<Value>, ApiError> {
    let templates = state
        .templates
        .list_pending_modules(tenant_id)
        .await
        .map_err(|e| internal_error("vein list_pending_modules", e))?;
    Ok(Json(json!({ "templates": templates })))
}

fn forbidden(code: &str, message: &str) -> ApiError {
    (
        StatusCode::FORBIDDEN,
        Json(json!({ "error": code, "message": message })),
    )
}

/// 運行管理者 (社員) による承認。端末の token で呼ばれ、利用者 (`AuthUser`) は見ない。承認者は端末の 1:N 照合で
/// 決まった社員で、ここでは承認者の役割 (manager か admin)・承認者自身の登録・自己承認でないことを検査する。
async fn approve_by_employee(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
    Json(body): Json<ApproveByEmployeeBody>,
) -> Result<StatusCode, ApiError> {
    if body.approver_employee_id == employee_id {
        return Err(forbidden(
            "self_approval",
            "自分の登録は自分で承認できません",
        ));
    }
    let outcome = state
        .templates
        .approve_by_employee(
            tenant_id,
            employee_id,
            body.approver_employee_id,
            body.updated_at,
        )
        .await
        .map_err(|e| internal_error("vein approve_by_employee", e))?;
    match outcome {
        ApproveByEmployee::Approved => Ok(StatusCode::NO_CONTENT),
        ApproveByEmployee::ApproverNotManager => Err(forbidden(
            "approver_not_manager",
            "承認できるのは運行管理者だけです",
        )),
        ApproveByEmployee::ApproverNotEnrolled => Err(forbidden(
            "approver_not_enrolled",
            "承認者の指静脈が承認済みで登録されていません",
        )),
        ApproveByEmployee::Conflict => Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": "approve_conflict",
                "message": "承認待ちの登録が変わっています。一覧を読み直してください",
            })),
        )),
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
