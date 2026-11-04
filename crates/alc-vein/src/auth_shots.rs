//! 指静脈の認証ログの口 (Refs ippoan/alc-app-s3#352)。認証のたびに端末がその場の撮影と認証の状態を送り、
//! ここが撮影を `chara_verify` にかけて結果を残す。本番の判定はモジュールのままで、ここは記録するだけ。
//! 閾値や計算を直したら、残した撮影で判定し直せる (判定の表に行を足す。前の判定は消さない)。
//!
//! - `POST /vein/auth-shots` (端末の鍵。`X-Device-Id` 必須) → 撮影を暗号化して保存し、本人の記録との 1:1 の判定も残す
//! - `POST /vein/auth-shots/rejudge` `{"since"?}` (開発者だけ) → 保存した撮影を復号して今の計算で判定し直す
//! - `GET /vein/auth-shots?limit=` (開発者だけ) → 新しい順に状態と最新の判定 (撮影は返さない)
//!
//! 撮影は生体データなので、worker が Secrets Store の鍵で暗号化してから DB に書く (DB は暗号文だけを持つ)。
//! 鍵は [`VeinState::auth_shot_key`] で、worker が [`needs_key`] の口のときだけ取る。

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    routing::post,
    Extension, Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use alc_core_wasm::api_error::{internal_error, not_found, unprocessable, ApiError};
use alc_core_wasm::TenantId;
use alc_worker_crypto::secret::{decrypt_secret, encrypt_secret};
use vein_match::{load, PLANE_LEN};
use vein_match_search::{chara_verify, template_to_charas, Library, TEMPLATE_MAGIC};

use crate::repo::{DeviceRecordRow, VeinTemplateRow, FORMAT_MODULE};
use crate::routes::{
    forbidden, parse_pick, registered_device_id, serve_for_device, MAX_MODULE_RECORD_BYTES,
};
use crate::VeinState;

/// 開発者のログインだけが開く口の印。auth-worker が開発者の Google ログインのときだけ付け直す
/// (端末の鍵の経路は新しいヘッダーを作るので、クライアントが送った同名のヘッダーは届かない)。
const DEVELOPER_HEADER: &str = "x-user-developer";

/// 判定し直しの 1 回の件数の上限。超えたら `more: true` を返し、残りは `next_since` から続ける。
pub const REJUDGE_LIMIT: usize = 500;
const LIST_DEFAULT: i64 = 50;
const LIST_MAX: i64 = 200;
const MAX_FIRMWARE_LEN: usize = 64;
const MAX_THRESHOLDS: usize = 8;

/// `chara_verify` の下限 (s0, s1, s2。vein-match-search の `chara_verify` が 1:1 の確認に渡すもの)。
/// 計算か閾値を直したら [`logic_version`] に出る文字列も変わるよう、1 か所にまとめる。
const CHARA_VERIFY_THRESHOLDS: &str = "55/55/75";

/// 判定した計算の版 (vein-match-search の版と閾値)。判定の行に残す。
pub fn logic_version() -> String {
    format!(
        "search-{} chara_verify {CHARA_VERIFY_THRESHOLDS}",
        vein_match_search::VERSION
    )
}

/// 撮影の暗号化・復号の鍵が要る口か (worker が Secrets Store から鍵を取るかどうかの判定)。
/// 他の口の待ち時間を増やさないよう、撮影を書く・判定し直す口のときだけ取る。
pub fn needs_key(method: &str, path: &str) -> bool {
    method == "POST"
        && (path.ends_with("/vein/auth-shots") || path.ends_with("/vein/auth-shots/rejudge"))
}

pub fn router() -> Router<VeinState> {
    Router::new()
        .route(
            "/vein/auth-shots",
            post(post_auth_shot).get(list_auth_shots),
        )
        .route("/vein/auth-shots/rejudge", post(rejudge))
}

/// 撮影の形。ファームの 1 本撮りの記録 (生のテンプレート) か、0xbdbd の特徴量。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShotKind {
    Record,
    Chara,
}

impl ShotKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::Chara => "chara",
        }
    }
}

/// 保存する撮影と認証の状態 (`vein_auth_shots` の 1 行)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAuthShot {
    pub id: Uuid,
    pub employee_id: Uuid,
    pub device_id: String,
    pub firmware: Option<String>,
    pub module_result: String,
    pub module_slot: Option<i32>,
    pub module_hit: String,
    pub module_score: Option<i32>,
    pub module_thresholds: Option<Vec<i32>>,
    pub shot_kind: &'static str,
    /// 撮影 (base64) を暗号化したもの
    pub shot_encrypted: String,
    pub captured_at: DateTime<Utc>,
}

/// 試した相手 1 人ぶんの結果 (相手の記録そのものは持たない)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candidate {
    pub employee_id: Uuid,
    pub matched: bool,
    /// `chara_verify` の info の s0, s1, s2
    pub scores: [u8; 3],
}

/// 判定 1 回ぶん (`vein_auth_judgments` の 1 行)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Judgment {
    pub logic_version: String,
    pub method: String,
    /// `match` / `again` / `nomatch` / `error`
    pub result: String,
    pub matched_employee_id: Option<Uuid>,
    pub candidates: Vec<Candidate>,
    pub error: Option<String>,
    pub elapsed_us: Option<i32>,
}

/// 保存した判定 (一覧に載せる最新の 1 件)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoredJudgment {
    #[serde(flatten)]
    pub judgment: Judgment,
    pub created_at: DateTime<Utc>,
}

/// `GET /vein/auth-shots` の 1 件 (撮影そのものは持たない)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuthShotStatus {
    pub id: Uuid,
    pub employee_id: Uuid,
    pub device_id: String,
    pub firmware: Option<String>,
    pub module_result: String,
    pub module_slot: Option<i32>,
    pub module_hit: String,
    pub module_score: Option<i32>,
    pub module_thresholds: Option<Vec<i32>>,
    pub shot_kind: String,
    pub captured_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    /// 最新の判定 (判定の行が無ければ null)
    pub judgment: Option<StoredJudgment>,
}

/// 判定し直しの対象 (暗号化されたままの撮影)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredShot {
    pub id: Uuid,
    pub employee_id: Uuid,
    pub device_id: String,
    pub shot_encrypted: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct ModuleBody {
    result: String,
    slot: Option<i32>,
    hit: String,
    score: Option<i32>,
    thresholds: Option<Vec<i32>>,
}

#[derive(Deserialize)]
struct ShotBody {
    employee_id: Uuid,
    /// base64。ファームの 1 本撮りの記録 (生のテンプレート) か 0xbdbd の特徴量
    shot: String,
    module: ModuleBody,
    firmware: Option<String>,
    captured_at: DateTime<Utc>,
    /// 端末へ配る記録の組み方 (`rotate` と同じ意味。`own` なら混ぜずに選ぶ)
    pick: Option<String>,
}

#[derive(Deserialize)]
struct RejudgeBody {
    since: Option<DateTime<Utc>>,
    pick: Option<String>,
}

#[derive(Deserialize)]
struct ListQuery {
    limit: Option<i64>,
}

/// 開発者のログインだけを通す。無ければ 403 `developer_only`。
fn require_developer(headers: &HeaderMap) -> Result<(), ApiError> {
    if headers.get(DEVELOPER_HEADER).is_some_and(|v| v == "1") {
        Ok(())
    } else {
        Err(forbidden("developer_only", "開発者だけが使えます"))
    }
}

/// 撮影の base64 を読み、形を決める。生のテンプレートの先頭 (`de ed de ed`) なら `Record`、`bd bd` なら `Chara`。
/// 読めない・大きすぎる・どちらでもないなら `None`。
fn classify_shot(b64: &str) -> Option<(ShotKind, Vec<u8>)> {
    let bytes = STANDARD
        .decode(b64)
        .ok()
        .filter(|b| b.len() <= MAX_MODULE_RECORD_BYTES)?;
    if bytes.starts_with(&TEMPLATE_MAGIC.to_le_bytes()) {
        Some((ShotKind::Record, bytes))
    } else if bytes.starts_with(&[0xbd, 0xbd]) {
        Some((ShotKind::Chara, bytes))
    } else {
        None
    }
}

/// モジュールの状態の検査。`CHECK` の値以外は 422 `invalid_module`。
fn validate_module(m: &ModuleBody, firmware: Option<&str>) -> Result<(), ApiError> {
    let ok = matches!(m.result.as_str(), "IDENTIFIED" | "NOMATCH")
        && matches!(m.hit.as_str(), "self" | "other" | "none")
        && m.thresholds.as_ref().map_or(0, Vec::len) <= MAX_THRESHOLDS
        && firmware.map_or(0, str::len) <= MAX_FIRMWARE_LEN;
    if ok {
        Ok(())
    } else {
        Err(unprocessable(
            "invalid_module",
            "module の result / hit / thresholds か firmware が正しくありません",
        ))
    }
}

/// `chara_verify` で 1:1 に確かめる。返すのは (s0, s1, s2, 一致したか)。読めなければ理由。
/// `chara_verify` は不一致でも読み込みの失敗でも info が全 0 で返るので、先に自分で読めることを確かめて
/// 区別する (記録は取り込める生のテンプレート、撮影は読み込める特徴量)。
fn verify(record_b64: &str, shot: &(ShotKind, Vec<u8>)) -> Result<([u8; 3], bool), &'static str> {
    let enroll = STANDARD
        .decode(record_b64)
        .ok()
        .filter(|e| e.starts_with(&TEMPLATE_MAGIC.to_le_bytes()))
        .ok_or("unreadable_record")?;
    let mut lib = Library::new(2).ok_or("library_unavailable")?;
    if lib.import_temp(1, &enroll) != 0 {
        return Err("unreadable_record");
    }
    let chara = match shot.0 {
        ShotKind::Chara => shot.1.clone(),
        ShotKind::Record => template_to_charas(&shot.1)
            .ok()
            .and_then(|c| c.into_iter().next())
            .ok_or("unreadable_shot")?
            .to_vec(),
    };
    load(&chara, chara.len() as u32, &mut [0u8; PLANE_LEN]).map_err(|_| "unreadable_shot")?;
    let r = chara_verify(&mut lib, &enroll, &chara, 0);
    let info = r.info.pack();
    Ok(([info[6], info[7], info[8]], r.output.is_some()))
}

/// 本人の記録 (その端末へ配る 1 件) との 1:1 の判定 (method = `self`)。記録が無い・読めない場合は `error`。
pub fn judge_self(
    employee_id: Uuid,
    record_b64: Option<&str>,
    shot: Option<&(ShotKind, Vec<u8>)>,
) -> Judgment {
    let started = Utc::now();
    let verified = match (record_b64, shot) {
        (Some(record), Some(shot)) => verify(record, shot),
        (None, _) => Err("no_approved_record"),
        (_, None) => Err("unreadable_shot"),
    };
    // Workers の時計は CPU の間は進まないので、0 は「測れなかった」として残さない
    let elapsed_us = (Utc::now() - started)
        .num_microseconds()
        .and_then(|us| i32::try_from(us).ok())
        .filter(|&us| us > 0);
    let (result, matched_employee_id, candidates, error) = match verified {
        Ok((scores, true)) => (
            "match",
            Some(employee_id),
            vec![Candidate {
                employee_id,
                matched: true,
                scores,
            }],
            None,
        ),
        Ok((scores, false)) => (
            "nomatch",
            None,
            vec![Candidate {
                employee_id,
                matched: false,
                scores,
            }],
            None,
        ),
        Err(reason) => ("error", None, vec![], Some(reason.to_owned())),
    };
    Judgment {
        logic_version: logic_version(),
        method: "self".to_owned(),
        result: result.to_owned(),
        matched_employee_id,
        candidates,
        error,
        elapsed_us,
    }
}

/// 乗務員 `employee_id` の、端末 `device_id` へ配る記録 (base64)。承認済みの module の行が無ければ `None`。
fn served_record(
    bases: &[VeinTemplateRow],
    devices: &[DeviceRecordRow],
    employee_id: Uuid,
    device_id: &str,
    pick_own: bool,
) -> Option<String> {
    let base = bases.iter().find(|r| r.employee_id == employee_id)?.clone();
    let own: Vec<&DeviceRecordRow> = devices
        .iter()
        .filter(|d| d.employee_id == employee_id)
        .collect();
    Some(serve_for_device(base, &own, device_id, pick_own).record)
}

/// 鍵が取れていない (worker が Secrets Store から取れなかった)。保存も判定し直しもしない。
fn key_missing() -> ApiError {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "encryption_key_missing" })),
    )
}

async fn post_auth_shot(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    headers: HeaderMap,
    Json(body): Json<ShotBody>,
) -> Result<Json<Value>, ApiError> {
    let Some(device_id) = registered_device_id(&headers)? else {
        return Err(unprocessable(
            "device_id_required",
            "X-Device-Id (端末の token) が要ります",
        ));
    };
    let pick_own = parse_pick(body.pick.as_deref())?;
    validate_module(&body.module, body.firmware.as_deref())?;
    let shot = classify_shot(&body.shot).ok_or_else(|| {
        unprocessable(
            "invalid_shot",
            "shot は base64 の生のテンプレートか 0xbdbd の特徴量で送ってください",
        )
    })?;
    let key = state.auth_shot_key.as_deref().ok_or_else(key_missing)?;
    let bases = state
        .templates
        .list(tenant_id, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein auth_shot list", e))?;
    let devices = state
        .templates
        .list_device_records(tenant_id)
        .await
        .map_err(|e| internal_error("vein auth_shot list_device_records", e))?;
    let record = served_record(&bases, &devices, body.employee_id, &device_id, pick_own);
    let judgment = judge_self(body.employee_id, record.as_deref(), Some(&shot));
    let record = NewAuthShot {
        id: Uuid::new_v4(),
        employee_id: body.employee_id,
        device_id,
        firmware: body.firmware,
        module_result: body.module.result,
        module_slot: body.module.slot,
        module_hit: body.module.hit,
        module_score: body.module.score,
        module_thresholds: body.module.thresholds,
        shot_kind: shot.0.as_str(),
        shot_encrypted: encrypt_secret(&body.shot, key)
            .map_err(|e| internal_error("vein auth_shot encrypt", e))?,
        captured_at: body.captured_at,
    };
    let saved = state
        .templates
        .insert_auth_shot(tenant_id, &record, &judgment)
        .await
        .map_err(|e| internal_error("vein insert_auth_shot", e))?;
    if !saved {
        return Err(not_found("employee_not_found"));
    }
    Ok(Json(json!({ "id": record.id, "judgment": judgment })))
}

/// 保存した撮影を今の計算で判定し直し、判定の表に行を足す (前の判定は消さない)。開発者だけ。
/// 対象は `since` 以降 (無ければ全部) の古い順 [`REJUDGE_LIMIT`] 件まで。超えたら `more: true` と
/// 続きの `next_since` (最後に判定した撮影の保存時刻。同じ時刻の撮影は次の回でもう一度判定する)。
async fn rejudge(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    headers: HeaderMap,
    Json(body): Json<RejudgeBody>,
) -> Result<Json<Value>, ApiError> {
    require_developer(&headers)?;
    let pick_own = parse_pick(body.pick.as_deref())?;
    let key = state.auth_shot_key.as_deref().ok_or_else(key_missing)?;
    let mut shots = state
        .templates
        .list_auth_shots(tenant_id, body.since, REJUDGE_LIMIT as i64 + 1)
        .await
        .map_err(|e| internal_error("vein rejudge list_auth_shots", e))?;
    let more = shots.len() > REJUDGE_LIMIT;
    shots.truncate(REJUDGE_LIMIT);
    let bases = state
        .templates
        .list(tenant_id, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein rejudge list", e))?;
    let devices = state
        .templates
        .list_device_records(tenant_id)
        .await
        .map_err(|e| internal_error("vein rejudge list_device_records", e))?;
    let judgments: Vec<(Uuid, Judgment)> = shots
        .iter()
        .map(|s| {
            let shot = decrypt_secret(&s.shot_encrypted, key)
                .ok()
                .and_then(|b64| classify_shot(&b64));
            let record = served_record(&bases, &devices, s.employee_id, &s.device_id, pick_own);
            (
                s.id,
                judge_self(s.employee_id, record.as_deref(), shot.as_ref()),
            )
        })
        .collect();
    state
        .templates
        .insert_judgments(tenant_id, &judgments)
        .await
        .map_err(|e| internal_error("vein rejudge insert_judgments", e))?;
    let mut out = json!({ "judged": judgments.len(), "logic_version": logic_version() });
    if more {
        out["more"] = json!(true);
        out["next_since"] = json!(shots.last().map(|s| s.created_at));
    }
    Ok(Json(out))
}

/// 新しい順に撮影の状態と最新の判定 (撮影そのものは返さない)。開発者だけ。`limit` は 1〜200 (既定 50)。
async fn list_auth_shots(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    require_developer(&headers)?;
    let limit = query.limit.unwrap_or(LIST_DEFAULT).clamp(1, LIST_MAX);
    let shots = state
        .templates
        .list_auth_shot_status(tenant_id, limit)
        .await
        .map_err(|e| internal_error("vein list_auth_shot_status", e))?;
    Ok(Json(json!({ "shots": shots })))
}
