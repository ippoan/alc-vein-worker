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

use std::collections::HashMap;

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
use vein_match_search::{chara_verify, rank_top, template_to_charas, Library, TEMPLATE_MAGIC};

use crate::matcher::MAX_TEMPLATES;
use crate::repo::{DeviceRecordRow, VeinTemplateRow, FORMAT_MODULE};
use crate::routes::{
    forbidden, parse_pick, registered_device_id, serve_for_device, MAX_MODULE_RECORD_BYTES,
};
use crate::VeinState;

/// 開発者のログインだけが開く口の印。auth-worker が開発者の Google ログインのときだけ付け直す
/// (端末の鍵の経路は新しいヘッダーを作るので、クライアントが送った同名のヘッダーは届かない)。
const DEVELOPER_HEADER: &str = "x-user-developer";

/// 判定し直しの 1 回の件数の上限。超えたら `more: true` を返し、残りは `next_since` から続ける。
/// 撮影 1 件に self と top10 の 2 回の判定がかかり (利用者 100 人で 1 件 100 ms 前後の見込み)、
/// 1 回の呼び出しの CPU 時間に収めるため小さめにする。
pub const REJUDGE_LIMIT: usize = 100;
const LIST_DEFAULT: i64 = 50;
const LIST_MAX: i64 = 200;
const MAX_FIRMWARE_LEN: usize = 64;
const MAX_THRESHOLDS: usize = 8;

/// `chara_verify` の下限 (s0, s1, s2。vein-match-search の `chara_verify` が 1:1 の確認に渡すもの)。
/// 計算か閾値を直したら [`logic_version`] に出る文字列も変わるよう、1 か所にまとめる。
const CHARA_VERIFY_THRESHOLDS: &str = "55/55/75";

/// 判定した計算の版 (vein-match-search の版と閾値)。method = `self` の判定の行に残す。
pub fn logic_version() -> String {
    format!(
        "search-{} chara_verify {CHARA_VERIFY_THRESHOLDS}",
        vein_match_search::VERSION
    )
}

/// 粗い比較で絞る人数 (method = `top10`)。
const TOP_N: usize = 10;

/// 保存した記録が無い古い行を今の記録で判定し直したときに、`logic_version` の後ろに付ける印。
const CURRENT_ENROLL_MARK: &str = " current-enroll";

/// method = `top10` の判定の版 (粗い比較の上位 [`TOP_N`] 人 + `chara_verify`)。
pub fn logic_version_top10() -> String {
    format!(
        "search-{} rank_top{TOP_N} chara_verify {CHARA_VERIFY_THRESHOLDS}",
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
    /// 端末へ配る記録の組み方 (`own` か、混ぜる側の NULL)。判定し直しで同じ組み方に戻す
    pub pick: Option<String>,
    pub shot_kind: &'static str,
    /// 撮影 (base64) を暗号化したもの
    pub shot_encrypted: String,
    /// 撮影したときに相手にした本人の記録 (base64) を、撮影と同じ鍵で暗号化したもの。
    /// 記録が無かったとき (`no_approved_record`) は `None`。判定し直しはこの記録を相手にする
    pub enroll_encrypted: Option<String>,
    pub captured_at: DateTime<Utc>,
}

/// 試した相手 1 人ぶんの結果 (相手の記録そのものは持たない)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candidate {
    pub employee_id: Uuid,
    pub matched: bool,
    /// `chara_verify` の info の s0, s1, s2
    pub scores: [u8; 3],
    /// 粗い比較の点数 (`rank_top`。小さいほど近い)。method = `top10` のときだけ
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coarse: Option<u32>,
    /// 粗い比較の順位 (1 から)。method = `top10` のときだけ
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<u32>,
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
    pub pick: Option<String>,
    pub shot_kind: String,
    pub captured_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    /// 撮影したときの記録を保存してあるか (記録そのものは返さない)
    pub has_enroll_snapshot: bool,
    /// method = `self` の最新の判定 (判定の行が無ければ null)
    pub judgment: Option<StoredJudgment>,
    /// method = `top10` の最新の判定 (無ければ null)
    pub judgment_top10: Option<StoredJudgment>,
}

/// 判定し直しの対象 (暗号化されたままの撮影)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredShot {
    pub id: Uuid,
    pub employee_id: Uuid,
    pub device_id: String,
    pub shot_encrypted: String,
    /// 撮影したときの本人の記録 (暗号化されたまま。古い行は `None`)
    pub enroll_encrypted: Option<String>,
    /// 保存時の `pick` (`own` なら混ぜずに選ぶ)
    pub pick: Option<String>,
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
    let enroll = decode_record(record_b64).ok_or("unreadable_record")?;
    let (chara, _) = shot_plane(shot)?;
    verify_chara(&enroll, &chara)
}

/// 記録 (base64) を取り込める生のテンプレートにする。取り込めなければ `None`。
fn decode_record(record_b64: &str) -> Option<Vec<u8>> {
    let enroll = STANDARD
        .decode(record_b64)
        .ok()
        .filter(|e| e.starts_with(&TEMPLATE_MAGIC.to_le_bytes()))?;
    let mut lib = Library::new(2)?;
    (lib.import_temp(1, &enroll) == 0).then_some(enroll)
}

/// 撮影から照合に使う特徴量 (0x448 バイト) と、その平面を取り出す。読めなければ `unreadable_shot`。
fn shot_plane(shot: &(ShotKind, Vec<u8>)) -> Result<(Vec<u8>, [u8; PLANE_LEN]), &'static str> {
    let chara = match shot.0 {
        ShotKind::Chara => shot.1.clone(),
        ShotKind::Record => template_to_charas(&shot.1)
            .ok()
            .and_then(|c| c.into_iter().next())
            .ok_or("unreadable_shot")?
            .to_vec(),
    };
    let mut plane = [0u8; PLANE_LEN];
    load(&chara, chara.len() as u32, &mut plane).map_err(|_| "unreadable_shot")?;
    Ok((chara, plane))
}

/// 取り込める記録 `enroll` と特徴量 `chara` を `chara_verify` で 1:1 に確かめる (毎回 `Library::new(2)`)。
fn verify_chara(enroll: &[u8], chara: &[u8]) -> Result<([u8; 3], bool), &'static str> {
    let mut lib = Library::new(2).ok_or("library_unavailable")?;
    let r = chara_verify(&mut lib, enroll, chara, 0);
    let info = r.info.pack();
    Ok(([info[6], info[7], info[8]], r.output.is_some()))
}

/// 本人の記録 (その端末へ配る 1 件) との 1:1 の判定 (method = `self`)。記録が無い・読めない場合は `error`
/// (`record_b64` が `Err` なら、その理由をそのまま error に残す)。
pub fn judge_self(
    employee_id: Uuid,
    record_b64: Result<&str, &'static str>,
    shot: Option<&(ShotKind, Vec<u8>)>,
) -> Judgment {
    let started = Utc::now();
    let verified = match (record_b64, shot) {
        (Ok(record), Some(shot)) => verify(record, shot),
        (Err(reason), _) => Err(reason),
        (_, None) => Err("unreadable_shot"),
    };
    let elapsed_us = elapsed_us(started, 0);
    let (result, matched_employee_id, candidates, error) = match verified {
        Ok((scores, true)) => (
            "match",
            Some(employee_id),
            vec![Candidate {
                employee_id,
                matched: true,
                scores,
                coarse: None,
                rank: None,
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
                coarse: None,
                rank: None,
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

/// 微秒で測った経過 (Workers の時計は CPU の間は進まないので、0 は「測れなかった」として残さない)
fn elapsed_us(started: DateTime<Utc>, extra_us: i64) -> Option<i32> {
    (Utc::now() - started)
        .num_microseconds()
        .and_then(|us| i32::try_from(us.saturating_add(extra_us)).ok())
        .filter(|&us| us > 0)
}

/// method = `top10` の相手: その端末へ配る記録 (承認済み・取り込めるもの) を `import_temp` で入れた `Library`。
/// 粗い比較 (`rank_top`) は `Library` を変えないので、同じ端末・同じ `pick` の撮影のあいだ使い回せる。
pub struct Gallery {
    lib: Library,
    /// 利用者 (1 始まりで `import_temp` した順) の乗務員と、取り込んだ生のテンプレート
    people: Vec<(Uuid, Vec<u8>)>,
    /// 取り込みに使った時間 (マイクロ秒。POST の判定の時間に足す)
    build_us: i64,
}

/// 相手の記録から [`Gallery`] を組む。読めない記録は外して警告を出す。件数が [`MAX_TEMPLATES`] を超えたら
/// `too_many_templates`、読める記録が 1 つも無ければ `no_approved_record`。
pub fn build_gallery(records: &[(Uuid, String)]) -> Result<Gallery, &'static str> {
    let started = Utc::now();
    if records.len() > MAX_TEMPLATES {
        return Err("too_many_templates");
    }
    let people: Vec<(Uuid, Vec<u8>)> = records
        .iter()
        .filter_map(|(id, b64)| decode_record(b64).map(|enroll| (*id, enroll)))
        .collect();
    let dropped = records.len() - people.len();
    if dropped > 0 {
        tracing::warn!("vein auth_shot top10: 読めない記録 {dropped} 件を外した");
    }
    if people.is_empty() {
        return Err("no_approved_record");
    }
    let mut lib = Library::new(people.len().max(2)).ok_or("library_unavailable")?;
    // 読めると確かめた記録だけなので、取り込みは通る
    for (i, (_, enroll)) in people.iter().enumerate() {
        lib.import_temp(i as u32 + 1, enroll);
    }
    let build_us = (Utc::now() - started).num_microseconds().unwrap_or(0);
    Ok(Gallery {
        lib,
        people,
        build_us,
    })
}

/// 粗い比較で上位 [`TOP_N`] 人に絞り、その全員を `chara_verify` にかける (method = `top10`)。
/// 1 人だけ通れば `match`、2 人以上通れば `again` (点数で選ばない)、0 人なら `nomatch`。
/// `include_build` なら [`Gallery`] を組んだ時間も判定の時間に足す。
pub fn judge_top10(
    gallery: &Result<Gallery, &'static str>,
    shot: Option<&(ShotKind, Vec<u8>)>,
    include_build: bool,
) -> Judgment {
    let started = Utc::now();
    let ranked = match (gallery, shot) {
        (Err(reason), _) => Err(*reason),
        (_, None) => Err("unreadable_shot"),
        (Ok(g), Some(shot)) => shot_plane(shot).map(|(chara, plane)| {
            let rows = rank_top(&g.lib, &plane, TOP_N);
            (g, chara, rows)
        }),
    };
    let build_us = match gallery {
        Ok(g) if include_build => g.build_us,
        _ => 0,
    };
    let (result, matched_employee_id, candidates, error) = match ranked {
        Err(reason) => ("error", None, vec![], Some(reason.to_owned())),
        Ok((g, chara, rows)) => {
            let candidates: Vec<Candidate> = rows
                .iter()
                .enumerate()
                .map(|(i, &(user, coarse))| {
                    let (employee_id, enroll) = &g.people[user as usize - 1];
                    // 取り込めると確かめた記録なので、読めない場合は起きない
                    let (scores, matched) = verify_chara(enroll, &chara).unwrap_or(([0; 3], false));
                    Candidate {
                        employee_id: *employee_id,
                        matched,
                        scores,
                        coarse: Some(coarse),
                        rank: Some(i as u32 + 1),
                    }
                })
                .collect();
            let hits: Vec<Uuid> = candidates
                .iter()
                .filter(|c| c.matched)
                .map(|c| c.employee_id)
                .collect();
            let (result, who) = match hits.as_slice() {
                [] => ("nomatch", None),
                [one] => ("match", Some(*one)),
                _ => ("again", None),
            };
            (result, who, candidates, None)
        }
    };
    Judgment {
        logic_version: logic_version_top10(),
        method: "top10".to_owned(),
        result: result.to_owned(),
        matched_employee_id,
        candidates,
        error,
        elapsed_us: elapsed_us(started, build_us),
    }
}

/// `records` のうち乗務員 `employee_id` の記録を `own` に差し替える (居なければ足す)。
/// 照合が通ると端末は撮影を本人の記録に入れるので、判定し直しは本人の記録だけを撮影したときのものにする。
fn replace_own(records: &mut Vec<(Uuid, String)>, employee_id: Uuid, own: String) {
    match records.iter_mut().find(|(id, _)| *id == employee_id) {
        Some(slot) => slot.1 = own,
        None => records.push((employee_id, own)),
    }
}

/// 端末 `device_id` へ配る、全員ぶんの記録 (承認済みの module の行がある乗務員。登録順)。
fn served_records(
    bases: &[VeinTemplateRow],
    devices: &[DeviceRecordRow],
    device_id: &str,
    pick_own: bool,
) -> Vec<(Uuid, String)> {
    bases
        .iter()
        .map(|b| {
            (
                b.employee_id,
                serve_one(b.clone(), devices, device_id, pick_own),
            )
        })
        .collect()
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
    Some(serve_one(base, devices, device_id, pick_own))
}

/// `base` の乗務員の、端末 `device_id` へ配る記録 (base64)。
fn serve_one(
    base: VeinTemplateRow,
    devices: &[DeviceRecordRow],
    device_id: &str,
    pick_own: bool,
) -> String {
    let own: Vec<&DeviceRecordRow> = devices
        .iter()
        .filter(|d| d.employee_id == base.employee_id)
        .collect();
    serve_for_device(base, &own, device_id, pick_own).record
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
    let served = served_record(&bases, &devices, body.employee_id, &device_id, pick_own);
    let judgment = judge_self(
        body.employee_id,
        served.as_deref().ok_or("no_approved_record"),
        Some(&shot),
    );
    let gallery = build_gallery(&served_records(&bases, &devices, &device_id, pick_own));
    let judgment_top10 = judge_top10(&gallery, Some(&shot), true);
    let record = NewAuthShot {
        id: Uuid::new_v4(),
        employee_id: body.employee_id,
        device_id,
        pick: body.pick,
        firmware: body.firmware,
        module_result: body.module.result,
        module_slot: body.module.slot,
        module_hit: body.module.hit,
        module_score: body.module.score,
        module_thresholds: body.module.thresholds,
        shot_kind: shot.0.as_str(),
        shot_encrypted: encrypt_secret(&body.shot, key)
            .map_err(|e| internal_error("vein auth_shot encrypt", e))?,
        enroll_encrypted: served
            .as_deref()
            .map(|r| encrypt_secret(r, key))
            .transpose()
            .map_err(|e| internal_error("vein auth_shot encrypt enroll", e))?,
        captured_at: body.captured_at,
    };
    let saved = state
        .templates
        .insert_auth_shot(
            tenant_id,
            &record,
            &[judgment.clone(), judgment_top10.clone()],
        )
        .await
        .map_err(|e| internal_error("vein insert_auth_shot", e))?;
    if !saved {
        return Err(not_found("employee_not_found"));
    }
    Ok(Json(json!({
        "id": record.id,
        "judgment": judgment,
        "judgment_top10": judgment_top10,
    })))
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
    // 保存した記録が無い古い行は、端末と pick が同じ撮影のあいだ今の相手の Library を使い回す
    // (粗い比較は Library を変えない)。保存した記録がある行は、本人の記録だけ差し替えて組む
    let mut galleries: HashMap<(String, bool), Result<Gallery, &'static str>> = HashMap::new();
    let mut judgments: Vec<(Uuid, Judgment)> = Vec::with_capacity(shots.len() * 2);
    for s in &shots {
        let shot = decrypt_secret(&s.shot_encrypted, key)
            .ok()
            .and_then(|b64| classify_shot(&b64));
        let pick_own = s.pick.is_some();
        // 撮影したときの本人の記録 (復号できなければ unreadable_enroll)
        let snapshot = s
            .enroll_encrypted
            .as_deref()
            .map(|enc| decrypt_secret(enc, key).map_err(|_| "unreadable_enroll"));
        let current = served_record(&bases, &devices, s.employee_id, &s.device_id, pick_own);
        let record = match &snapshot {
            Some(Ok(r)) => Ok(r.as_str()),
            Some(Err(reason)) => Err(*reason),
            None => current.as_deref().ok_or("no_approved_record"),
        };
        let mut self_judgment = judge_self(s.employee_id, record, shot.as_ref());
        let top10_judgment = match snapshot {
            Some(snapshot) => {
                let gallery = snapshot.and_then(|own| {
                    let mut records = served_records(&bases, &devices, &s.device_id, pick_own);
                    replace_own(&mut records, s.employee_id, own);
                    build_gallery(&records)
                });
                judge_top10(&gallery, shot.as_ref(), false)
            }
            None => {
                let gallery = galleries
                    .entry((s.device_id.clone(), pick_own))
                    .or_insert_with(|| {
                        build_gallery(&served_records(&bases, &devices, &s.device_id, pick_own))
                    });
                let mut j = judge_top10(gallery, shot.as_ref(), false);
                // 保存した記録が無い行は今の記録で判定したので、印を残す
                j.logic_version.push_str(CURRENT_ENROLL_MARK);
                self_judgment.logic_version.push_str(CURRENT_ENROLL_MARK);
                j
            }
        };
        judgments.push((s.id, self_judgment));
        judgments.push((s.id, top10_judgment));
    }
    state
        .templates
        .insert_judgments(tenant_id, &judgments)
        .await
        .map_err(|e| internal_error("vein rejudge insert_judgments", e))?;
    let mut out = json!({
        "judged": shots.len(),
        "logic_version": logic_version(),
        "logic_version_top10": logic_version_top10(),
        "methods": { "self": shots.len(), "top10": shots.len() },
    });
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
