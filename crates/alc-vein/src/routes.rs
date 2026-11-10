//! 指静脈テンプレートの口 (Refs ippoan/vein-match#20)。
//!
//! - `PUT /vein/templates/{employee_id}` `{"charas": [hex, ...]}` → 登録し直す (キオスクからも)
//! - `PUT /vein/templates/{employee_id}/module` `{"record": base64}` → 端末のモジュールの記録全体を預かる
//! - `PUT /vein/templates/{employee_id}/module/devices` `{"record": base64}` → その端末で撮った記録を端末ごとに足す
//!   (`X-Device-Id` 必須。承認済みの module の行が在る乗務員だけ。承認は要らない。Refs ippoan/alc-app#558)
//! - `GET /vein/module-templates` → 承認済みのモジュールの記録の一覧 (端末が書き戻して、モジュール自身に照合させる)。
//!   `X-Device-Id` があれば、その端末向けに「その端末の 3 件 + 他の端末の最新の 3 件」へ混ぜて返す。
//!   `?pick=own` なら混ぜずに 1 件を選ぶ (その端末で撮った記録、無ければ他の端末の最新。`source` = `own` / `other`)。
//!   `?employee_id=<uuid>` でその 1 人だけに絞る (端末が免許証で決まった人の版を確かめる)
//! - `GET /vein/module-templates/pending` → 承認待ちのモジュールの記録の一覧 (端末の token で。中身は返さない)
//! - `POST /vein/templates/{employee_id}/approve-by-employee` `{"approver_employee_id", "updated_at"}` → 運行管理者
//!   (社員) による承認 (端末の token で。承認者は端末の照合で決まり、ここで役割・登録・自己承認を検査する)
//! - `GET /vein/devices/approval-check?since=<RFC3339>` → 自動で承認された端末ごとの記録を、今の合格の線で点検する
//!   (開発者だけ。数と ID と日時だけを返し、記録は返さない)
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
    extract::{Path, Query, State},
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
use vein_match_search::MAXREG;

use crate::matcher::{self, CharaError, MAX_TEMPLATES};
use crate::repo::{
    ApproveByEmployee, DeviceApproval, DeviceRecordRow, ModuleRecordItem, VeinTemplateItem,
    VeinTemplateRow, FORMAT_CHARA, FORMAT_MODULE,
};
use crate::VeinState;

/// モジュールの記録 1 件の大きさの上限 (バイト。モジュールの READ_ENROLL の全体は実機で 8156 バイト)。
pub const MAX_MODULE_RECORD_BYTES: usize = 0x2000;

/// テナントのモジュールの記録の人数の上限。端末のモジュールの枠 (本番の ID は 10〜100 の 91 個) より多くてよい:
/// 端末は免許証で決まった人だけを枠に書き、埋まったら一番長く使っていない人の枠を使い回す
/// (Refs ippoan/alc-app-s3#352)。数は chara の行の 1:N の上限 [`MAX_TEMPLATES`] に揃える。
pub const MAX_MODULE_RECORDS: usize = MAX_TEMPLATES;

/// 端末ごとの記録の自動承認に要る、登録した記録の件のうち承認済みの記録と合う件の数 (6 件中 4 件。2026-11-07 オーナー決定)。
/// 件が 4 件に満たない記録は全部合っても届かない (承認待ちになる)。
pub const AUTO_APPROVE_MIN_MATCHES: usize = 4;

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
            "/vein/templates/{employee_id}/module/devices/rotate",
            post(rotate_device_record),
        )
        .route(
            "/vein/templates/{employee_id}",
            put(upsert_template).delete(delete_template),
        )
        .route("/vein/identify", post(identify))
        .route("/vein/devices/approval-check", get(approval_check))
        .merge(crate::auth_shots::router())
        .merge(crate::report::router())
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
struct RotateBody {
    /// 照合が通った直後にその端末で撮った記録 (先頭の 1 件だけを使う)
    record: String,
    /// 端末が今どちらの組み方で取っているか (`own` なら [`pick_for_device`]、無ければ [`compose_for_device`])
    pick: Option<String>,
}

#[derive(Deserialize)]
struct ApproveBody {
    updated_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct ApproveByEmployeeBody {
    approver_employee_id: Uuid,
    updated_at: DateTime<Utc>,
    /// 端末ごとの記録の承認待ちを承認するときの、その端末 (一覧の `device_id`)。無ければ乗務員の記録
    #[serde(default)]
    device_id: Option<String>,
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
pub(crate) fn require_admin(user: Option<Extension<AuthUser>>) -> Result<Uuid, ApiError> {
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
pub(crate) fn registered_device_id(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
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

/// 登録した記録に近い他の人 (警告用の `near`。見比べられなくても登録は止めないので、失敗は空にする)。
async fn near_of(state: &VeinState, tenant_id: Uuid, employee_id: Uuid, record: &str) -> Value {
    let others: Vec<(Uuid, String)> = state
        .templates
        .list(tenant_id, FORMAT_MODULE)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.employee_id != employee_id)
        .map(|r| (r.employee_id, r.template))
        .collect();
    Value::Array(crate::auth_shots::near_others(record, &others))
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
    if let Some(device_id) = device_id.as_deref() {
        if let Some(outcome) =
            register_on_device(&state, tenant_id, employee_id, device_id, &body.record).await?
        {
            let mut outcome = outcome;
            outcome["near"] = near_of(&state, tenant_id, employee_id, &body.record).await;
            return Ok(Json(outcome));
        }
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
    let near = near_of(&state, tenant_id, employee_id, &body.record).await;
    Ok(Json(
        json!({ "employee_id": employee_id, "updated_at": updated_at, "near": near }),
    ))
}

/// 承認済みの module の記録が在る乗務員の、端末での登録 (Refs ippoan/alc-app-s3#352、migration 210)。乗務員の記録
/// (vein_templates、1 人 1 件) には上書きせず、その端末の記録として預かる。自動で承認するのは次の両方のとき
/// ([`auto_approve_blocked`])。どちらかを欠けば、その端末の記録だけ承認待ちにする:
///
/// - 本人: 登録した記録の件のうち [`AUTO_APPROVE_MIN_MATCHES`] 件以上が、承認済みの記録 (乗務員の記録か、承認済みの
///   端末ごとの記録のどれか) と `chara_verify` で合う
/// - 他の人: 登録した記録の件が、他の人の承認済みの記録と合格の線 (`passes`) で 1 件も合わない ([`others_passing`])。
///   確かめられなかったときも承認待ちにする
///
/// 承認待ちにしたときは、応答の `auto_approve_blocked` に理由を返す (`too_few_matches` / `near_other` /
/// `others_check_failed`)。他の人の確かめは本人の側が届いたときだけ、登録 1 回につき 1 回。
///
/// 乗務員の記録を上書きすると全端末で承認待ちになり、端末の同期がモジュールの記録を消して指紋互換の部分を失う
/// (2026-11-06)。承認済みの記録が無い乗務員は `Ok(None)` (今までどおり乗務員の記録として承認待ちで登録する)
async fn register_on_device(
    state: &VeinState,
    tenant_id: Uuid,
    employee_id: Uuid,
    device_id: &str,
    record: &str,
) -> Result<Option<Value>, ApiError> {
    let bases = state
        .templates
        .list(tenant_id, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein register_on_device list", e))?;
    let Some(base) = bases.iter().find(|r| r.employee_id == employee_id) else {
        return Ok(None);
    };
    let devices = state
        .templates
        .list_device_records(tenant_id)
        .await
        .map_err(|e| internal_error("vein register_on_device list_device_records", e))?;
    let approved = approved_records(&base.template, devices.iter(), employee_id);
    let (matched, entries) =
        best_match(record, approved, crate::auth_shots::count_matching_entries);
    // 本人の側が届かなければ承認待ちと決まるので、他の人とは見比べない
    let others = if matched >= AUTO_APPROVE_MIN_MATCHES {
        others_passing(record, &bases, &devices, employee_id)
    } else {
        Ok(vec![])
    };
    let blocked = auto_approve_blocked(matched, &others);
    let approval = match blocked {
        None => DeviceApproval::AutoApproved,
        Some(_) => DeviceApproval::Pending,
    };
    let auto = approval == DeviceApproval::AutoApproved;
    // 照合の間に乗務員の記録が承認待ち・削除になった (書けなかった) なら None: 今までどおりの登録に任せる
    Ok(state
        .templates
        .upsert_device_record(tenant_id, employee_id, device_id, record, approval)
        .await
        .map_err(|e| internal_error("vein register_on_device upsert_device_record", e))?
        .map(|updated_at| {
            let mut out = json!({
                "employee_id": employee_id,
                "updated_at": updated_at,
                "device_record": true,
                "approved": auto,
                "auto_approved": auto,
                "matched": matched,
                "entries": entries,
            });
            if let Some(reason) = blocked {
                out["auto_approve_blocked"] = json!(reason);
            }
            match &others {
                Ok(people) if !people.is_empty() => {
                    let (count, most) = (people.len(), people[0].1);
                    // 文は先に組む (warn! が複数行になると、CI の計測で文字列の行が未実行に数えられる)
                    let why = format!("他の人 {count} 人に合う (最多 {most} 件)");
                    tracing::warn!("vein 自動承認を止めた: {why}");
                    out["others_people"] = json!(count);
                    out["others_matched"] = json!(most);
                }
                Ok(_) => {}
                Err(reason) => {
                    tracing::warn!("vein 自動承認を止めた: 他の人と見比べられない ({reason})");
                    out["others_error"] = json!(reason);
                }
            }
            out
        }))
}

/// 自動承認を止める理由。`None` なら自動で承認する。`matched` は本人の承認済みの記録と前の線
/// (`count_matching_entries`) で合った件の数、`others` は他の人との見比べ ([`others_passing`])。
/// 登録 ([`register_on_device`]) と点検 ([`approval_check`]) が同じ判定を使う。
fn auto_approve_blocked(
    matched: usize,
    others: &Result<Vec<(Uuid, usize)>, &'static str>,
) -> Option<&'static str> {
    match others {
        _ if matched < AUTO_APPROVE_MIN_MATCHES => Some("too_few_matches"),
        Err(_) => Some("others_check_failed"),
        Ok(people) if !people.is_empty() => Some("near_other"),
        Ok(_) => None,
    }
}

/// `record` の件が、他の人の承認済みの記録 (乗務員の記録 `bases` と端末ごとの記録 `devices` のうち `employee_id`
/// 以外) と合格の線で合った件の数 (人ごと)。確かめられなければ `Err` ([`crate::auth_shots::passing_others`])。
fn others_passing(
    record: &str,
    bases: &[VeinTemplateRow],
    devices: &[DeviceRecordRow],
    employee_id: Uuid,
) -> Result<Vec<(Uuid, usize)>, &'static str> {
    let others: Vec<(Uuid, String)> = bases
        .iter()
        .map(|b| (b.employee_id, b.template.clone()))
        .chain(devices.iter().map(|d| (d.employee_id, d.record.clone())))
        .filter(|(id, _)| *id != employee_id)
        .collect();
    crate::auth_shots::passing_others(record, &others)
}

/// 自動承認が見比べる相手: 乗務員の記録 `base` と、`devices` のうちその乗務員の端末ごとの記録。
/// 登録 ([`register_on_device`]) と点検 ([`approval_check`]) が同じ集め方を使う。
fn approved_records<'a>(
    base: &'a str,
    devices: impl Iterator<Item = &'a DeviceRecordRow>,
    employee_id: Uuid,
) -> impl Iterator<Item = &'a str> {
    std::iter::once(base).chain(
        devices
            .filter(move |d| d.employee_id == employee_id)
            .map(|d| d.record.as_str()),
    )
}

/// `record` の件が、相手 `approved` のどれかといちばん多く合った (合った件の数, 件の総数)。数え方は `count`
/// (自動承認は `count_matching_entries`、点検は加えて `count_passing_entries`)。比べられる相手が無ければ (0, 0)。
fn best_match<'a>(
    record: &str,
    approved: impl Iterator<Item = &'a str>,
    count: fn(&str, &str) -> Option<(usize, usize)>,
) -> (usize, usize) {
    approved
        .filter_map(|a| count(record, a))
        .max_by_key(|(m, _)| *m)
        .unwrap_or((0, 0))
}

/// 点検の 1 回で見る記録の数の上限。超えたら `more: true` と、続きの `next_since` を返す。
/// 1 件に、本人の記録との 2 通りの数え (前の線・今の線) と他の人との見比べがかかるので小さめにする。
pub const APPROVAL_CHECK_LIMIT: usize = 10;

/// 点検の `since` の既定 (今からこの時間だけ前)。
const APPROVAL_CHECK_DEFAULT_HOURS: i64 = 24;

#[derive(Deserialize)]
struct ApprovalCheckQuery {
    since: Option<DateTime<Utc>>,
}

/// 自動で承認された端末ごとの記録を、今の合格の線で点検する (開発者だけ。Refs ippoan/alc-app-s3#352)。
/// 自動承認 ([`register_on_device`]) の本人の側は `chara_verify` が通したかだけで数え、合格の条件 (`passes`) は他の人の側に使う。
/// 承認したときの数は保存していないので、`since` (既定は直近 24 時間) 以降に自動で承認された記録ごとに数え直す:
///
/// - `verified`: 前の線 (`count_matching_entries` = 自動承認が今使っている数え方) で合った件の数と、件の総数
/// - `passes`: 参考。本人の側を今の合格の線 (`count_passing_entries`) で数えた件の数と、件の総数 (自動承認の判定には使わない)
/// - `others`: 同じ記録が、他の人の承認済みの記録と今の合格の線で合った件の数 (人ごと。合う人だけ)。
///   確かめられなかったときは空で、`others_error` に理由
/// - `auto_approve_now`: 今 登録し直したら自動で承認されるか ([`auto_approve_blocked`] = 登録と同じ判定:
///   `verified` が [`AUTO_APPROVE_MIN_MATCHES`] 件以上で、`others` が空)
///
/// 相手は自動承認と同じ集め方 ([`approved_records`]) で、乗務員の記録と承認済みの端末ごとの記録。ただし今の DB の
/// 中身で数える: 点検する記録そのものと、その承認より後に預かった端末ごとの記録は外す。承認の後に登録し直された
/// 乗務員の記録・差し替えられた端末ごとの記録は、当時のものに戻せない。記録そのものは返さない。
async fn approval_check(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    headers: HeaderMap,
    Query(query): Query<ApprovalCheckQuery>,
) -> Result<Json<Value>, ApiError> {
    crate::auth_shots::require_developer(&headers)?;
    let since = query
        .since
        .unwrap_or_else(|| Utc::now() - chrono::Duration::hours(APPROVAL_CHECK_DEFAULT_HOURS));
    let mut rows = state
        .templates
        .list_auto_approved_device_records(tenant_id, since, APPROVAL_CHECK_LIMIT as i64 + 1)
        .await
        .map_err(|e| internal_error("vein approval_check list_auto_approved", e))?;
    let next_since = rows.get(APPROVAL_CHECK_LIMIT).map(|r| r.approved_at);
    rows.truncate(APPROVAL_CHECK_LIMIT);
    let bases = state
        .templates
        .list(tenant_id, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein approval_check list", e))?;
    let devices = state
        .templates
        .list_device_records(tenant_id)
        .await
        .map_err(|e| internal_error("vein approval_check list_device_records", e))?;
    let records: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut out = json!({
                "employee_id": row.employee_id,
                "device_id": row.device_id,
                "approved_at": row.approved_at,
            });
            let others = others_passing(&row.record, &bases, &devices, row.employee_id);
            out["others"] = others
                .iter()
                .flatten()
                .map(|(id, matched)| json!({ "employee_id": id, "matched": matched }))
                .collect();
            if let Err(reason) = &others {
                out["others_error"] = json!(reason);
            }
            let Some(base) = bases.iter().find(|b| b.employee_id == row.employee_id) else {
                out["error"] = json!("no_approved_record");
                return out;
            };
            let approved: Vec<&str> = approved_records(
                &base.template,
                devices
                    .iter()
                    .filter(|d| d.device_id != row.device_id && d.updated_at < row.approved_at),
                row.employee_id,
            )
            .collect();
            let (verified, entries) = best_match(
                &row.record,
                approved.iter().copied(),
                crate::auth_shots::count_matching_entries,
            );
            let (passed, passed_entries) = best_match(
                &row.record,
                approved.iter().copied(),
                crate::auth_shots::count_passing_entries,
            );
            out["compared"] = json!(approved.len());
            out["verified"] = json!({ "matched": verified, "entries": entries });
            out["passes"] = json!({ "matched": passed, "entries": passed_entries });
            out["auto_approve_now"] = json!(auto_approve_blocked(verified, &others).is_none());
            out
        })
        .collect();
    let mut out = json!({
        "since": since,
        "min_matches": AUTO_APPROVE_MIN_MATCHES,
        "logic_version": crate::auth_shots::logic_version(),
        "records": records,
    });
    if next_since.is_some() {
        out["more"] = json!(true);
        out["next_since"] = json!(next_since);
    }
    Ok(Json(out))
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
        .upsert_device_record(
            tenant_id,
            employee_id,
            &device_id,
            &body.record,
            DeviceApproval::Approved,
        )
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
    let near = near_of(&state, tenant_id, employee_id, &body.record).await;
    Ok(Json(
        json!({ "employee_id": employee_id, "updated_at": updated_at, "near": near }),
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
/// - ただし自分の端末ごとの記録が [`MAXREG`] 件揃っていれば、それ自体を混ぜずに返す (照合のたびに入れ替えた記録
///   [`rotate_device_record`] は、もう他の端末の件を含む。混ぜ直すと入れ替えた件を捨てる)。
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
    if let Some(m) = mine.filter(|&m| !sources[m].is_base) {
        if record_count(sources[m].record) == Some(MAXREG) {
            return ModuleRecordItem {
                employee_id: base.employee_id,
                record: sources[m].record.to_owned(),
                updated_at: sources[m].updated_at,
                registered_device_id: base.registered_device_id.clone(),
                composed: false,
                source: None,
            };
        }
    }
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
            source: None,
        },
        None => {
            let employee_id = base.employee_id;
            tracing::warn!("vein module-templates: {employee_id} の記録を混ぜられず元のまま返す");
            base.into()
        }
    }
}

/// `pick=own` の 1 件 (Refs ippoan/alc-app-s3#352): 混ぜずに、`requester` で撮った記録 (端末ごとの記録、無ければ
/// 元の行がその端末のもの) を選ぶ。無ければ出どころのうち最新 (`updated_at`) のもの。
fn pick_for_device(
    base: VeinTemplateRow,
    devices: &[&DeviceRecordRow],
    requester: &str,
) -> ModuleRecordItem {
    let own = devices
        .iter()
        .filter(|d| d.device_id == requester)
        .max_by_key(|d| d.updated_at);
    // 自分が無ければ、元の行と端末ごとの記録のうち最新 (同時刻は元の行。[`compose_for_device`] の並べ方と同じ)
    let newest = devices
        .iter()
        .max_by_key(|d| d.updated_at)
        .filter(|d| d.updated_at > base.updated_at);
    let (chosen, source) = match own {
        Some(d) => (Some(d), "own"),
        None if base.registered_device_id.as_deref() == Some(requester) => (None, "own"),
        None => (newest, "other"),
    };
    let mut item: ModuleRecordItem = match chosen {
        Some(d) => ModuleRecordItem {
            employee_id: base.employee_id,
            record: d.record.clone(),
            updated_at: d.updated_at,
            registered_device_id: base.registered_device_id,
            composed: false,
            source: None,
        },
        None => base.into(),
    };
    item.source = Some(source);
    item
}

/// base64 の記録 2 つから先頭 [`MIX_TAKE`] 件ずつを取り、混ぜた記録を base64 で返す。読めなければ `None`。
fn mix_records(a: &str, b: &str) -> Option<String> {
    let a = STANDARD.decode(a).ok()?;
    let b = STANDARD.decode(b).ok()?;
    let mixed = vein_match_search::mix_templates(&[(&a, MIX_TAKE), (&b, MIX_TAKE)]).ok()?;
    Some(STANDARD.encode(mixed))
}

#[derive(Debug, Deserialize)]
struct ModuleTemplatesQuery {
    /// `own` だけ受ける (混ぜずに 1 件を選ぶ。[`pick_for_device`])
    pick: Option<String>,
    /// この乗務員だけに絞る
    employee_id: Option<Uuid>,
}

/// 承認済みのモジュールの記録の一覧 (端末のモジュールへ書き戻す元)。`X-Device-Id` があれば、その端末向けに
/// [`compose_for_device`] で組む (`?pick=own` なら [`pick_for_device`] で選ぶ。端末ごとの記録は 1 回の問い合わせで
/// 全員分)。無ければ (管理者のログイン) 元の行のまま。`?employee_id=` はその 1 人に絞る (居なければ空の一覧)。
async fn list_module_templates(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Query(query): Query<ModuleTemplatesQuery>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let pick_own = parse_pick(query.pick.as_deref())?;
    let requester = registered_device_id(&headers)?;
    let mut rows = state
        .templates
        .list(tenant_id, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein list_module_templates", e))?;
    if let Some(employee_id) = query.employee_id {
        rows.retain(|r| r.employee_id == employee_id);
    }
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
            serve_for_device(base, &own, &requester, pick_own)
        })
        .collect();
    Ok(Json(json!({ "templates": templates })))
}

/// `pick` の値: 無し = 混ぜる、`own` = 混ぜずに選ぶ。他は 422 `invalid_pick`
pub(crate) fn parse_pick(pick: Option<&str>) -> Result<bool, ApiError> {
    match pick {
        None => Ok(false),
        Some("own") => Ok(true),
        Some(_) => Err(unprocessable(
            "invalid_pick",
            "pick は own だけ指定できます",
        )),
    }
}

/// 端末 `requester` へ配る 1 件 (`pick_own` なら [`pick_for_device`]、でなければ [`compose_for_device`])
pub(crate) fn serve_for_device(
    base: VeinTemplateRow,
    devices: &[&DeviceRecordRow],
    requester: &str,
    pick_own: bool,
) -> ModuleRecordItem {
    if pick_own {
        pick_for_device(base, devices, requester)
    } else {
        compose_for_device(base, devices, requester)
    }
}

/// 照合が通った端末の 1 本で、その端末へ配っている記録の一番古い 1 本を入れ替え、その端末の記録として預かる
///。記録の件は新しい順に並べる約束: 撮った 1 件を先頭に置き、配っている記録の
/// 先頭から [`MAXREG`] - 1 件を続ける (= 末尾の一番古い 1 件が落ちる。6 件に満たなければ落とさず増える)。
/// 通るたびに呼ばれ、その端末の記録はだんだんその端末で撮った件に入れ替わる。
/// `X-Device-Id` 必須 (422 `device_id_required`)。承認済みの module の行が無ければ 409 `no_approved_module_record`。
/// 撮った記録・配っている記録が読めなければ 422 `invalid_module_record`。返すのは組んだ記録と版
async fn rotate_device_record(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    Path(employee_id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<RotateBody>,
) -> Result<Json<Value>, ApiError> {
    let Some(requester) = registered_device_id(&headers)? else {
        return Err(unprocessable(
            "device_id_required",
            "X-Device-Id (端末の token) が要ります",
        ));
    };
    let pick_own = parse_pick(body.pick.as_deref())?;
    validate_module_record(&body.record)?;
    let no_record = || {
        (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "no_approved_module_record",
                "message": "承認済みの module の記録が在る乗務員だけ、端末の記録を入れ替えられます",
            })),
        )
    };
    let base = state
        .templates
        .list(tenant_id, FORMAT_MODULE)
        .await
        .map_err(|e| internal_error("vein rotate list", e))?
        .into_iter()
        .find(|r| r.employee_id == employee_id)
        .ok_or_else(no_record)?;
    let devices = state
        .templates
        .list_device_records(tenant_id)
        .await
        .map_err(|e| internal_error("vein rotate list_device_records", e))?;
    let own: Vec<&DeviceRecordRow> = devices
        .iter()
        .filter(|d| d.employee_id == employee_id)
        .collect();
    let served = serve_for_device(base, &own, &requester, pick_own);
    let record = rotate_records(&body.record, &served.record)
        .ok_or_else(|| unprocessable("invalid_module_record", "記録を組み直せません"))?;
    let updated_at = state
        .templates
        .upsert_device_record(
            tenant_id,
            employee_id,
            &requester,
            &record,
            DeviceApproval::Approved,
        )
        .await
        .map_err(|e| internal_error("vein rotate upsert_device_record", e))?
        .ok_or_else(no_record)?;
    Ok(Json(json!({
        "employee_id": employee_id,
        "record": record,
        "updated_at": updated_at,
    })))
}

/// 撮った記録 `fresh` の先頭 1 件 + 配っている記録 `served` の先頭 [`MAXREG`] - 1 件 (base64)。読めなければ `None`
fn rotate_records(fresh: &str, served: &str) -> Option<String> {
    let fresh = STANDARD.decode(fresh).ok()?;
    let served = STANDARD.decode(served).ok()?;
    let rotated = vein_match_search::mix_templates(&[(&fresh, 1), (&served, MAXREG - 1)]).ok()?;
    Some(STANDARD.encode(rotated))
}

/// base64 の記録の件数。読めなければ `None`
fn record_count(b64: &str) -> Option<usize> {
    let t = STANDARD.decode(b64).ok()?;
    Some(usize::from(
        vein_match_search::parse_template(&t).ok()?.0.count,
    ))
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

pub(crate) fn forbidden(code: &str, message: &str) -> ApiError {
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
            body.device_id.as_deref(),
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
