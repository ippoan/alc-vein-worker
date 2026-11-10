//! 毎朝 7:00 (日本時間) の「指静脈の日報」メール (Refs ippoan/alc-app-s3#352)。テナントの notify のグループ
//! [`REPORT_GROUP`] (「指静脈の日報」) のメンバーへ、前日 (日本時間 0:00〜24:00) の指静脈の点検を 1 人 1 通送る。
//! 血圧の日報 (ippoan/alc-measurements-worker の `report.rs`) と同じ形で、宛先は notify の既存の表
//! (`notify_groups`・`notify_recipient_groups`・`notify_recipients`) の使える宛先 (`enabled`・email あり)。
//! **警告が無い日も送る** (「警告はありません」)。指の管理は各テナントの責任なので、宛先はテナントのグループだけ。
//! グループが無い・メンバーが 0 のテナントは何もしない。画面に出したり未承認にしたりはしない (メールだけ)。
//!
//! 中身は 2 つ:
//!
//! 1. **前日の登録の見比べ** — 前日に登録・更新された module の記録 (初回の記録 `vein_templates` と端末ごとの記録
//!    `vein_device_records`。`updated_at` で判断。承認待ちも含む) ごとに、同じテナントの**他の人の承認済みの記録**と
//!    登録時の見比べ (`near`、[`crate::auth_shots::near_others`]) と同じ方法で比べ、通った人を載せる。
//! 2. **前日の照合の食い違い** — 前日の認証ログ (`vein_auth_shots`) のうち、静脈の層と指紋互換の層が別の枠を当てた回
//!    (`module_fv_slot` と `module_fp_slot` が両方 0 でなく違う)。認証ログは一部のテナントにしか無い。
//!
//! 定時実行 (wrangler.toml の `[triggers]`。本番だけ [`DAILY_CRON`]) が [`run`] を呼ぶ。テスト送信
//! (`POST /vein/daily-report-test`。admin だけ) は [`test_send`]: 同じ宛先へ前日の実際の日報を件名に「[テスト] 」を付けて送る。
//!
//! # 重さ
//!
//! 見比べは記録 1 件ごとに他の人の記録で検索の部品を組み直す ([`crate::auth_shots::near_others`])。CPU は
//! (見比べる記録の数) × (承認済みの人数) に比例するので、1 テナントで見比べる記録は [`COMPARE_MAX`] 件までにし、
//! 超えた分は本文に件数だけ書く。DB は 1 テナント 1 transaction で、見比べは transaction の外で回す。
//!
//! **ログは件数だけ** (氏名・id・メールアドレス・記録の中身を出さない)。

use std::sync::Arc;

use alc_core_wasm::api_error::{internal_error, ApiError};
use alc_core_wasm::TenantId;
use alc_worker_db::TxOutput;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use chrono::{DateTime, Datelike, Duration, FixedOffset, NaiveDate, TimeZone, Utc};
use futures_util::future::BoxFuture;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth_shots::near_others;
use crate::repo::VeinTemplatesRepository;
use crate::VeinState;

/// 宛先のグループの名前 (テナントごとに notify の画面 (nuxt-notify) で作る。`notify_groups` は (tenant_id, name) が UNIQUE。
/// nuxt-notify の同名の定数と文字列を合わせる)
pub const REPORT_GROUP: &str = "指静脈の日報";

/// テスト送信の件名の接頭辞
pub const TEST_SUBJECT_PREFIX: &str = "[テスト] ";

/// 日報の定時実行 (UTC。毎日 22:00 = 日本時間の翌朝 7:00)。wrangler.toml の本番の `[triggers]` と同じ文字列
pub const DAILY_CRON: &str = "0 22 * * *";

/// 1 テナントで見比べる記録の上限 (CPU を抑える。超えた分は本文に件数だけ書く)
pub const COMPARE_MAX: usize = 50;

pub mod sql {
    /// テナントの一覧 (`tenants` は RLS が無く、実行用ロールが読める)
    pub const TENANTS: &str = "SELECT id FROM tenants ORDER BY id";

    /// グループの使える宛先。グループ → (LEFT JOIN) メンバー → 宛先。使えない宛先 (`enabled` でない・email が NULL) は
    /// JOIN の条件で落とす。$1 グループ名 / $2 tenant_id → email (NULL の行が混ざる)。WHERE に tenant_id も明示する
    pub const RECIPIENTS: &str = "SELECT r.email \
         FROM notify_groups g \
         LEFT JOIN notify_recipient_groups m ON m.group_id = g.id \
         LEFT JOIN notify_recipients r ON r.id = m.recipient_id \
             AND r.tenant_id = $2 AND r.enabled AND r.email IS NOT NULL \
         WHERE g.name = $1 AND g.tenant_id = $2";

    /// 前日に登録・更新された module の記録 (初回の記録と端末ごとの記録。削除済みの乗務員を除く、古い順)。
    /// $1 tenant_id / $2 始め (以上) / $3 終わり (未満) → employee_id, name, record, 端末ごとの記録か, updated_at。
    pub const ENROLLED: &str = "SELECT v.employee_id, e.name, v.template, FALSE, v.updated_at \
         FROM vein_templates v \
         JOIN employees e ON e.id = v.employee_id AND e.tenant_id = v.tenant_id \
         WHERE v.tenant_id = $1 AND e.deleted_at IS NULL AND v.template_format = 'module' \
           AND v.updated_at >= $2 AND v.updated_at < $3 \
         UNION ALL \
         SELECT d.employee_id, e.name, d.record, TRUE, d.updated_at \
         FROM vein_device_records d \
         JOIN employees e ON e.id = d.employee_id AND e.tenant_id = d.tenant_id \
         WHERE d.tenant_id = $1 AND e.deleted_at IS NULL \
           AND d.updated_at >= $2 AND d.updated_at < $3 \
         ORDER BY 5, 1";

    /// 前日の認証ログの件数。$1 tenant_id / $2 始め / $3 終わり。
    pub const SHOT_COUNT: &str = "SELECT COUNT(*) FROM vein_auth_shots \
         WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3";

    /// 前日の認証ログのうち、静脈の層と指紋互換の層が別の枠を当てた回 (古い順)。
    /// $1 tenant_id / $2 始め / $3 終わり → created_at, name, fv_slot, fp_slot, fv_score, via, rule, module_hit。
    pub const MISMATCHES: &str =
        "SELECT s.created_at, e.name, s.module_fv_slot, s.module_fp_slot, \
                s.module_fv_score, s.module_via, s.module_rule, s.module_hit \
         FROM vein_auth_shots s \
         JOIN employees e ON e.id = s.employee_id AND e.tenant_id = s.tenant_id \
         WHERE s.tenant_id = $1 AND s.created_at >= $2 AND s.created_at < $3 \
           AND s.module_fv_slot IS NOT NULL AND s.module_fv_slot <> 0 \
           AND s.module_fp_slot IS NOT NULL AND s.module_fp_slot <> 0 \
           AND s.module_fv_slot <> s.module_fp_slot \
         ORDER BY s.created_at, s.id";
}

/// 前日に登録・更新された記録 1 件
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Enrolled {
    pub employee_id: Uuid,
    pub name: String,
    /// 端末ごとの記録なら true (初回の記録は false)
    pub per_device: bool,
    pub record: String,
}

/// 見比べる相手 (同じテナントの承認済みの module の記録)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Other {
    pub employee_id: Uuid,
    pub name: String,
    pub record: String,
}

/// 静脈の層と指紋互換の層が別の枠を当てた 1 回
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    pub at: DateTime<Utc>,
    pub name: String,
    pub fv_slot: i32,
    pub fp_slot: i32,
    pub fv_score: Option<i32>,
    pub via: Option<String>,
    pub rule: Option<String>,
    /// モジュールの判定での枠の持ち主 (`self` / `other` / `none`)
    pub hit: String,
}

/// 1 テナントの前日の材料 (DB から 1 transaction で引く)。宛先が 0 なら残りは空
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Facts {
    pub emails: Vec<String>,
    pub enrolled: Vec<Enrolled>,
    pub others: Vec<Other>,
    /// 前日の認証ログの件数 (0 = 認証ログが無い)
    pub shots: i64,
    pub mismatches: Vec<Mismatch>,
}

impl TxOutput for Facts {}

/// 登録の見比べで通った 1 組
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hit {
    pub name: String,
    pub per_device: bool,
    pub near_name: String,
    pub rule: String,
    pub scores: [u64; 3],
}

/// 見比べの結果
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Analysis {
    /// 見比べた記録の数
    pub compared: usize,
    /// 上限 ([`COMPARE_MAX`]) で見比べなかった記録の数
    pub omitted: usize,
    pub hits: Vec<Hit>,
}

/// `now` (UTC) が日本時間で何日かを見て、その前日の (始め, 終わり, 日付)。始め以上・終わり未満が前日の 0:00〜24:00
pub fn yesterday(now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>, NaiveDate) {
    let jst = jst();
    let today = now.with_timezone(&jst).date_naive();
    let day = today - Duration::days(1);
    let midnight = |d: NaiveDate| {
        jst.from_local_datetime(&d.and_hms_opt(0, 0, 0).expect("0:00"))
            .single()
            .expect("固定の offset に曖昧な時刻は無い")
            .with_timezone(&Utc)
    };
    (midnight(day), midnight(today), day)
}

fn jst() -> FixedOffset {
    FixedOffset::east_opt(9 * 3600).expect("+09:00")
}

/// 定時実行の時刻 (Cloudflare の `scheduled_time`。epoch ミリ秒) から
pub fn from_millis(ms: f64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms as i64)
}

/// 名前や値に改行が入っても本文の行が崩れないようにする
fn single_line(s: &str) -> String {
    s.replace(['\r', '\n'], " ")
}

/// 前日に登録・更新された記録を、他の人の承認済みの記録と見比べる (CPU を使う。transaction の外で呼ぶ)。
/// 同じ人の記録とは比べない。通った人は [`near_others`] の順 (粗い比較の近い順、1 件につき最大 3 人)
pub fn analyze(facts: &Facts) -> Analysis {
    let mut analysis = Analysis {
        omitted: facts.enrolled.len().saturating_sub(COMPARE_MAX),
        ..Default::default()
    };
    for e in facts.enrolled.iter().take(COMPARE_MAX) {
        analysis.compared += 1;
        let others: Vec<(Uuid, String)> = facts
            .others
            .iter()
            .filter(|o| o.employee_id != e.employee_id)
            .map(|o| (o.employee_id, o.record.clone()))
            .collect();
        for v in near_others(&e.record, &others) {
            if let Some(hit) = hit_of(e, &v, &facts.others) {
                analysis.hits.push(hit);
            }
        }
    }
    analysis
}

/// `near_others` の 1 件 (employee_id・rule・scores) を載せる形にする。相手の名前が引けない・形が違うときは載せない
fn hit_of(e: &Enrolled, v: &Value, others: &[Other]) -> Option<Hit> {
    let id: Uuid = v.get("employee_id")?.as_str()?.parse().ok()?;
    let near_name = others.iter().find(|o| o.employee_id == id)?.name.clone();
    let rule = v.get("rule")?.as_str()?.to_owned();
    let s = v.get("scores")?.as_array()?;
    let at = |i: usize| s.get(i).and_then(Value::as_u64).unwrap_or(0);
    Some(Hit {
        name: e.name.clone(),
        per_device: e.per_device,
        near_name,
        rule,
        scores: [at(0), at(1), at(2)],
    })
}

fn hit_label(hit: &str) -> &'static str {
    match hit {
        "self" => "本人の枠",
        "other" => "他人の枠",
        _ => "不一致",
    }
}

/// 件名と本文 (text)
pub fn compose(day: NaiveDate, facts: &Facts, analysis: &Analysis) -> (String, String) {
    let subject = format!("指静脈の日報 ({}/{})", day.month(), day.day());
    let warnings = analysis.hits.len() + facts.mismatches.len();
    let mut body = format!("{subject}\n");
    if warnings == 0 {
        body.push_str("警告はありません\n");
    } else {
        body.push_str(&format!("警告 {warnings} 件\n"));
    }

    body.push_str("\n【前日の登録の見比べ】\n");
    if analysis.compared == 0 {
        body.push_str("前日に登録・更新された記録はありません\n");
    } else {
        body.push_str(&format!(
            "前日に登録・更新された記録 {} 件を、他の人の承認済みの記録と見比べました。\n",
            analysis.compared
        ));
        if analysis.omitted > 0 {
            body.push_str(&format!(
                "(記録が多いので、ほかの {} 件は見比べていません)\n",
                analysis.omitted
            ));
        }
        if analysis.hits.is_empty() {
            body.push_str("近い人はいません\n");
        }
        for h in &analysis.hits {
            let kind = if h.per_device {
                "端末ごとの記録"
            } else {
                "初回の記録"
            };
            body.push_str(&format!(
                "・{}さんの登録 ({kind}) が {}さん に近い (規則 {}・点数 {}/{}/{})\n",
                single_line(&h.name),
                single_line(&h.near_name),
                single_line(&h.rule),
                h.scores[0],
                h.scores[1],
                h.scores[2]
            ));
        }
    }

    body.push_str("\n【前日の照合の食い違い】\n");
    if facts.shots == 0 {
        body.push_str("対象の記録なし (前日の認証ログがありません)\n");
    } else if facts.mismatches.is_empty() {
        body.push_str(&format!(
            "食い違いはありません (認証ログ {} 件)\n",
            facts.shots
        ));
    } else {
        body.push_str(&format!(
            "静脈の層と指紋互換の層が別の枠を当てた回が {} 件ありました (認証ログ {} 件中)。\n",
            facts.mismatches.len(),
            facts.shots
        ));
        for m in &facts.mismatches {
            let time = m.at.with_timezone(&jst()).format("%H:%M");
            let mut line = format!(
                "・{time} {}さん: 静脈の層は枠 {}、指紋互換の層は枠 {} (モジュールの判定: {})",
                single_line(&m.name),
                m.fv_slot,
                m.fp_slot,
                hit_label(&m.hit)
            );
            if let Some(score) = m.fv_score {
                line.push_str(&format!("、静脈の点数 {score}"));
            }
            if let (Some(via), Some(rule)) = (&m.via, &m.rule) {
                line.push_str(&format!(
                    "、通した層 {}・規則 {}",
                    single_line(via),
                    single_line(rule)
                ));
            }
            body.push_str(&line);
            body.push('\n');
        }
    }
    (subject, body)
}

/// 送信の段の失敗。持つのは段の名前だけ (宛先・本文・ランタイムのエラー文を持たない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailError(pub &'static str);

/// 1 人宛の 1 通 (差出人は送り手 [`Mailer`] が持つ)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    pub to: String,
    pub subject: String,
    pub body: String,
}

/// メールの送り手 (Worker では Cloudflare の `send_email` の binding `ALERT_EMAIL`、テストでは偽物)。
/// axum の state と handler が要るので `Send + Sync` で `Send` な future を返す
pub trait Mailer: Send + Sync {
    fn send<'a>(&'a self, mail: &'a Mail) -> BoxFuture<'a, Result<(), MailError>>;
}

pub type SharedMailer = Arc<dyn Mailer>;

/// ログの出し先 (固定の語と件数だけを渡す)
pub type Log<'a> = &'a (dyn Fn(&str) + Send + Sync);

/// 宛先ごとに同じ件名・本文の 1 通ずつ送る。失敗はログ 1 行 (段の名前だけ) で、ほかの宛先へは続ける。
/// リトライしない。返すのは (送れた数, 失敗した数)
pub async fn deliver(
    mailer: &SharedMailer,
    emails: Vec<String>,
    subject: &str,
    body: &str,
    log: Log<'_>,
) -> (usize, usize) {
    let (mut sent, mut failed) = (0, 0);
    for to in emails {
        let mail = Mail {
            to,
            subject: subject.to_owned(),
            body: body.to_owned(),
        };
        match mailer.send(&mail).await {
            Ok(()) => sent += 1,
            Err(e) => {
                failed += 1;
                log(&format!("vein: report send failed: {}", e.0));
            }
        }
    }
    (sent, failed)
}

/// 1 回の実行の件数 (ログに出すのは件数だけ)
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReportSummary {
    /// テナントの数
    pub tenants: usize,
    /// 宛先が無く、送らなかったテナント
    pub skipped: usize,
    /// 引きに失敗したテナント
    pub lookup_failed: usize,
    /// 送れた通数
    pub sent: usize,
    /// 送れなかった通数
    pub failed: usize,
}

impl ReportSummary {
    /// ログの 1 行 (件数だけ)
    pub fn line(&self) -> String {
        format!(
            "vein: report: tenants {} skipped {} lookup_failed {} sent {} failed {}",
            self.tenants, self.skipped, self.lookup_failed, self.sent, self.failed
        )
    }
}

/// 前日の日報を、宛先のあるテナントごとに送る。送り手が無い・テナントの一覧が取れなければ `None` (理由はログ 1 行)。
/// 1 テナントの失敗で残りを止めない (リトライしない)
pub async fn run(
    repo: &dyn VeinTemplatesRepository,
    mailer: Option<SharedMailer>,
    log: Log<'_>,
    now: DateTime<Utc>,
) -> Option<ReportSummary> {
    let Some(mailer) = mailer else {
        log("vein: report skipped: no mailer");
        return None;
    };
    let Ok(tenants) = repo.report_tenants().await else {
        log("vein: report list failed");
        return None;
    };
    let (from, to, day) = yesterday(now);
    let mut summary = ReportSummary {
        tenants: tenants.len(),
        ..Default::default()
    };
    for tenant_id in tenants {
        let Ok(facts) = repo.report_facts(tenant_id, from, to).await else {
            log("vein: report lookup failed");
            summary.lookup_failed += 1;
            continue;
        };
        if facts.emails.is_empty() {
            summary.skipped += 1;
            continue;
        }
        let analysis = analyze(&facts);
        let (subject, body) = compose(day, &facts, &analysis);
        let (sent, failed) = deliver(&mailer, facts.emails, &subject, &body, log).await;
        summary.sent += sent;
        summary.failed += failed;
    }
    Some(summary)
}

/// `POST /vein/daily-report-test` — グループ [`REPORT_GROUP`] の使える宛先へ、前日の実際の日報を件名に「[テスト] 」を付けて
/// 今すぐ送る。admin 以外 403 `forbidden`・送り手が無い 503 `mail_unavailable`・宛先が 0 (グループが無いも含む)
/// 409 `no_recipients`・引きの失敗 500・送った 200 `{"sent", "failed"}` (宛先ごとの成否・アドレスは返さない)
async fn test_send(
    State(state): State<VeinState>,
    Extension(TenantId(tenant_id)): Extension<TenantId>,
    user: Option<Extension<alc_core_wasm::AuthUser>>,
) -> Result<Json<Value>, ApiError> {
    crate::routes::require_admin(user)?;
    let Some(mailer) = state.mailer.clone() else {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE, "mail_unavailable"));
    };
    let (from, to, day) = yesterday(Utc::now());
    let facts = state
        .templates
        .report_facts(tenant_id, from, to)
        .await
        .map_err(|e| internal_error("vein report test", e))?;
    if facts.emails.is_empty() {
        return Err(error(StatusCode::CONFLICT, "no_recipients"));
    }
    let analysis = analyze(&facts);
    let (subject, body) = compose(day, &facts, &analysis);
    let subject = format!("{TEST_SUBJECT_PREFIX}{subject}");
    let log = |m: &str| tracing::error!("{m}");
    let (sent, failed) = deliver(&mailer, facts.emails, &subject, &body, &log).await;
    Ok(Json(json!({ "sent": sent, "failed": failed })))
}

fn error(status: StatusCode, word: &'static str) -> ApiError {
    (status, Json(json!({ "error": word })))
}

/// テスト送信の口 ([`crate::routes::tenant_router`] に merge する)
pub fn router() -> Router<VeinState> {
    Router::new().route("/vein/daily-report-test", post(test_send))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matcher::{self, synth};

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn record_of(seed: u64) -> String {
        let charas: Vec<_> = (0..3).map(|_| synth::chara(seed)).collect();
        matcher::enroll_template(&charas).unwrap()
    }

    fn other(name: &str, seed: u64) -> Other {
        Other {
            employee_id: Uuid::new_v4(),
            name: name.into(),
            record: record_of(seed),
        }
    }

    fn enrolled(name: &str, seed: u64, per_device: bool) -> Enrolled {
        Enrolled {
            employee_id: Uuid::new_v4(),
            name: name.into(),
            per_device,
            record: record_of(seed),
        }
    }

    fn mismatch(name: &str, hit: &str) -> Mismatch {
        Mismatch {
            at: at("2026-11-09T00:12:00Z"),
            name: name.into(),
            fv_slot: 12,
            fp_slot: 34,
            fv_score: None,
            via: None,
            rule: None,
            hit: hit.into(),
        }
    }

    /// 本文に含まれるか (assert のメッセージ引数を別の行に持たない形にする。CI の llvm-cov はその行を未実行に数える)
    fn has(body: &str, needle: &str) -> bool {
        body.contains(needle)
    }

    fn day() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 11, 9).unwrap()
    }

    #[test]
    fn yesterday_is_the_previous_japan_day() {
        // 毎朝 7:00 (日本時間) = 前日の 22:00 UTC。11/10 7:00 に動けば 11/9 の 0:00〜24:00
        let (from, to, d) = yesterday(at("2026-11-09T22:00:00Z"));
        assert_eq!(from, at("2026-11-08T15:00:00Z"));
        assert_eq!(to, at("2026-11-09T15:00:00Z"));
        assert_eq!(d, day());
        // 日本時間の日付で決める
        assert_eq!(
            yesterday(at("2026-11-09T14:59:59Z")).2.to_string(),
            "2026-11-08"
        );
        assert_eq!(
            yesterday(at("2026-12-31T22:00:00Z")).2.to_string(),
            "2026-12-31"
        );
    }

    #[test]
    fn from_millis_reads_the_scheduled_time() {
        assert_eq!(
            from_millis(1_790_114_400_000.0),
            Some(at("2026-09-22T22:00:00Z"))
        );
        assert_eq!(from_millis(f64::MAX), None);
    }

    #[test]
    fn quiet_day_still_says_so() {
        let facts = Facts::default();
        let (subject, body) = compose(day(), &facts, &analyze(&facts));
        assert_eq!(subject, "指静脈の日報 (11/9)");
        assert_eq!(
            body,
            "指静脈の日報 (11/9)\n警告はありません\n\n\
             【前日の登録の見比べ】\n前日に登録・更新された記録はありません\n\n\
             【前日の照合の食い違い】\n対象の記録なし (前日の認証ログがありません)\n"
        );
    }

    #[test]
    fn compared_without_hits_and_shots_without_mismatch() {
        let facts = Facts {
            enrolled: vec![enrolled("山田", 5, false)],
            others: vec![other("鈴木", 99)],
            shots: 7,
            ..Default::default()
        };
        let analysis = analyze(&facts);
        assert_eq!((analysis.compared, analysis.omitted), (1, 0));
        let (_, body) = compose(day(), &facts, &analysis);
        assert!(has(&body, "警告はありません"));
        assert!(has(
            &body,
            "記録 1 件を、他の人の承認済みの記録と見比べました。\n近い人はいません\n"
        ));
        assert!(has(&body, "食い違いはありません (認証ログ 7 件)"));
    }

    #[test]
    fn a_near_person_is_reported_but_never_the_same_person() {
        let near = other("鈴木 花子", 7);
        let mut me = enrolled("山田\n太郎", 7, true);
        // 自分自身の承認済みの記録 (同じ指) は比べない
        let mine = Other {
            employee_id: me.employee_id,
            name: "山田 太郎".into(),
            record: me.record.clone(),
        };
        let facts = Facts {
            enrolled: vec![me.clone()],
            others: vec![mine.clone(), near.clone()],
            ..Default::default()
        };
        let analysis = analyze(&facts);
        assert_eq!(analysis.hits.len(), 1, "{analysis:?}");
        let hit = &analysis.hits[0];
        assert_eq!(hit.near_name, "鈴木 花子");
        assert!(hit.per_device);
        let (_, body) = compose(day(), &facts, &analysis);
        assert!(has(&body, "警告 1 件"));
        assert!(has(
            &body,
            "・山田 太郎さんの登録 (端末ごとの記録) が 鈴木 花子さん に近い (規則 "
        ));
        // 初回の記録の言い方
        me.per_device = false;
        let facts = Facts {
            enrolled: vec![me],
            others: vec![near],
            ..Default::default()
        };
        let (_, body) = compose(day(), &facts, &analyze(&facts));
        assert!(has(&body, "(初回の記録)"));
    }

    #[test]
    fn too_many_records_are_capped_and_counted() {
        let facts = Facts {
            enrolled: (0..COMPARE_MAX + 3)
                .map(|_| enrolled("山田", 5, false))
                .collect(),
            ..Default::default()
        };
        let analysis = analyze(&facts);
        assert_eq!((analysis.compared, analysis.omitted), (COMPARE_MAX, 3));
        let (_, body) = compose(day(), &facts, &analysis);
        assert!(has(
            &body,
            "(記録が多いので、ほかの 3 件は見比べていません)"
        ));
    }

    #[test]
    fn hit_of_skips_malformed_near_entries() {
        let e = enrolled("山田", 5, false);
        let o = other("鈴木", 7);
        let id = o.employee_id.to_string();
        let others = [o];
        assert!(hit_of(&e, &json!({}), &others).is_none());
        assert!(hit_of(&e, &json!({"employee_id": "x"}), &others).is_none());
        // 相手が引けない
        let stranger = Uuid::new_v4().to_string();
        assert!(hit_of(
            &e,
            &json!({"employee_id": stranger, "rule": "A", "scores": []}),
            &others
        )
        .is_none());
        assert!(hit_of(&e, &json!({"employee_id": id}), &others).is_none());
        assert!(hit_of(&e, &json!({"employee_id": id, "rule": "A"}), &others).is_none());
        // 点数が足りなくても落ちない
        let hit = hit_of(
            &e,
            &json!({"employee_id": id, "rule": "B", "scores": [1, 2]}),
            &others,
        )
        .unwrap();
        assert_eq!(hit.scores, [1, 2, 0]);
    }

    #[test]
    fn mismatches_are_listed_in_japan_time() {
        let mut full = mismatch("山田\n太郎", "self");
        full.fv_score = Some(800);
        full.via = Some("fp".into());
        full.rule = Some("C".into());
        let facts = Facts {
            shots: 30,
            mismatches: vec![full, mismatch("鈴木", "other"), mismatch("佐藤", "none")],
            ..Default::default()
        };
        let (_, body) = compose(day(), &facts, &analyze(&facts));
        assert!(has(&body, "警告 3 件"));
        assert!(has(
            &body,
            "別の枠を当てた回が 3 件ありました (認証ログ 30 件中)"
        ));
        assert!(has(&body, "・09:12 山田 太郎さん: 静脈の層は枠 12、指紋互換の層は枠 34 (モジュールの判定: 本人の枠)、静脈の点数 800、通した層 fp・規則 C\n"));
        assert!(has(&body, "(モジュールの判定: 他人の枠)\n"));
        assert!(has(&body, "(モジュールの判定: 不一致)\n"));
    }

    #[test]
    fn summary_line_has_counts_only() {
        let s = ReportSummary {
            tenants: 3,
            skipped: 1,
            lookup_failed: 1,
            sent: 4,
            failed: 2,
        };
        assert_eq!(
            s.line(),
            "vein: report: tenants 3 skipped 1 lookup_failed 1 sent 4 failed 2"
        );
    }
}
