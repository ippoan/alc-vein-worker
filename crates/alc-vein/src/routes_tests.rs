//! `routes` の口を DB なしで確かめる (repo は fake に差し替える)。
//! SQL の側 (RLS・upsert・updated_at の競合) は crates/alc-vein/tests/sql_db.rs が組み込みの PostgreSQL で見る。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Extension;
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

use alc_core_wasm::{AuthUser, DbError, TenantId};

use super::tenant_router;
use super::{MAX_MODULE_RECORDS, MAX_MODULE_RECORD_BYTES};
use crate::matcher::{self, synth, MAX_TEMPLATES};
use crate::repo::{
    ApprovalItem, ApproveByEmployee, DeviceRecordRow, PendingModuleItem, VeinTemplateRow,
    VeinTemplatesRepository, FORMAT_MODULE,
};
use crate::VeinState;

/// fake の repo。`employees` に居る乗務員だけ登録でき、`fail` で全メソッドが DB エラー、
/// `conflict` で書き戻しが競合 (0 行) になる。登録は承認待ち (`pending`) で入り、`list` は承認済みだけを返す。
#[derive(Default)]
struct FakeRepo {
    employees: Mutex<Vec<(Uuid, String)>>,
    rows: Mutex<Vec<VeinTemplateRow>>,
    /// 承認待ちの乗務員 (`rows` のうち `list` に出さないもの)。
    pending: Mutex<Vec<Uuid>>,
    /// `rows` のうち形式が module の乗務員 (それ以外は chara)。
    modules: Mutex<Vec<Uuid>>,
    /// 承認した管理者の user_id (承認した順)。
    approved_by: Mutex<Vec<Uuid>>,
    /// role に manager か admin を持つ乗務員。
    managers: Mutex<Vec<Uuid>>,
    /// 運行管理者として承認された (対象, 承認者)。
    approved_by_employee: Mutex<Vec<(Uuid, Uuid)>>,
    /// 端末ごとの記録 (`vein_device_records`)。
    device_records: Mutex<Vec<DeviceRecordRow>>,
    fail: AtomicBool,
    /// upsert だけを DB エラーにする (人数の数えは通す)。
    fail_upsert: AtomicBool,
    conflict: AtomicBool,
    /// list_device_records だけを DB エラーにする。
    fail_device_list: AtomicBool,
    /// update_learned の呼び出し (id, 読んだ updated_at)。
    learned_calls: Mutex<Vec<(Uuid, DateTime<Utc>)>>,
}

impl FakeRepo {
    fn check(&self) -> Result<(), DbError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(DbError::Other("pool timed out".into()));
        }
        Ok(())
    }

    fn add_employee(&self, name: &str) -> Uuid {
        let id = Uuid::new_v4();
        self.employees.lock().unwrap().push((id, name.to_string()));
        id
    }

    fn template_of(&self, employee_id: Uuid) -> String {
        let rows = self.rows.lock().unwrap();
        rows.iter()
            .find(|r| r.employee_id == employee_id)
            .unwrap()
            .template
            .clone()
    }
}

#[async_trait]
impl VeinTemplatesRepository for FakeRepo {
    async fn upsert(
        &self,
        _tenant_id: Uuid,
        employee_id: Uuid,
        template: &str,
        format: &str,
        registered_device_id: Option<&str>,
    ) -> Result<Option<DateTime<Utc>>, DbError> {
        self.check()?;
        if self.fail_upsert.load(Ordering::SeqCst) {
            return Err(DbError::Other("pool timed out".into()));
        }
        let employees = self.employees.lock().unwrap();
        let Some((_, name)) = employees.iter().find(|(id, _)| *id == employee_id) else {
            return Ok(None);
        };
        let now = Utc::now();
        let mut rows = self.rows.lock().unwrap();
        rows.retain(|r| r.employee_id != employee_id);
        rows.push(VeinTemplateRow {
            id: Uuid::new_v4(),
            employee_id,
            name: name.clone(),
            template: template.to_string(),
            updated_at: now,
            registered_device_id: registered_device_id.map(str::to_owned),
        });
        // 形式も上書きする
        let mut modules = self.modules.lock().unwrap();
        modules.retain(|e| *e != employee_id);
        if format == FORMAT_MODULE {
            modules.push(employee_id);
        }
        // 登録は上書きも承認待ち
        let mut pending = self.pending.lock().unwrap();
        pending.retain(|e| *e != employee_id);
        pending.push(employee_id);
        Ok(Some(now))
    }

    async fn list(&self, _tenant_id: Uuid, format: &str) -> Result<Vec<VeinTemplateRow>, DbError> {
        self.check()?;
        let pending = self.pending.lock().unwrap();
        let modules = self.modules.lock().unwrap();
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .iter()
            .filter(|r| {
                !pending.contains(&r.employee_id)
                    && modules.contains(&r.employee_id) == (format == FORMAT_MODULE)
            })
            .cloned()
            .collect())
    }

    async fn list_by_approval(
        &self,
        _tenant_id: Uuid,
        approved: bool,
    ) -> Result<Vec<ApprovalItem>, DbError> {
        self.check()?;
        let pending = self.pending.lock().unwrap();
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .iter()
            .filter(|r| pending.contains(&r.employee_id) != approved)
            .map(|r| ApprovalItem {
                employee_id: r.employee_id,
                updated_at: r.updated_at,
            })
            .collect())
    }

    async fn approve(
        &self,
        _tenant_id: Uuid,
        employee_id: Uuid,
        approved_by: Uuid,
        updated_at: DateTime<Utc>,
    ) -> Result<bool, DbError> {
        self.check()?;
        let mut pending = self.pending.lock().unwrap();
        let rows = self.rows.lock().unwrap();
        let version_matches = rows
            .iter()
            .any(|r| r.employee_id == employee_id && r.updated_at == updated_at);
        if !version_matches || !pending.contains(&employee_id) {
            return Ok(false);
        }
        pending.retain(|e| *e != employee_id);
        self.approved_by.lock().unwrap().push(approved_by);
        Ok(true)
    }

    async fn list_pending_modules(
        &self,
        _tenant_id: Uuid,
    ) -> Result<Vec<PendingModuleItem>, DbError> {
        self.check()?;
        let pending = self.pending.lock().unwrap();
        let modules = self.modules.lock().unwrap();
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .iter()
            .filter(|r| pending.contains(&r.employee_id) && modules.contains(&r.employee_id))
            .map(|r| PendingModuleItem {
                employee_id: r.employee_id,
                name: r.name.clone(),
                updated_at: r.updated_at,
                registered_device_id: r.registered_device_id.clone(),
            })
            .collect())
    }

    async fn approve_by_employee(
        &self,
        _tenant_id: Uuid,
        employee_id: Uuid,
        approver_employee_id: Uuid,
        updated_at: DateTime<Utc>,
    ) -> Result<ApproveByEmployee, DbError> {
        self.check()?;
        if !self
            .managers
            .lock()
            .unwrap()
            .contains(&approver_employee_id)
        {
            return Ok(ApproveByEmployee::ApproverNotManager);
        }
        let mut pending = self.pending.lock().unwrap();
        let modules = self.modules.lock().unwrap();
        let rows = self.rows.lock().unwrap();
        let enrolled = rows.iter().any(|r| r.employee_id == approver_employee_id)
            && modules.contains(&approver_employee_id)
            && !pending.contains(&approver_employee_id);
        if !enrolled {
            return Ok(ApproveByEmployee::ApproverNotEnrolled);
        }
        let version_matches = rows
            .iter()
            .any(|r| r.employee_id == employee_id && r.updated_at == updated_at);
        if !version_matches || !pending.contains(&employee_id) {
            return Ok(ApproveByEmployee::Conflict);
        }
        pending.retain(|e| *e != employee_id);
        self.approved_by_employee
            .lock()
            .unwrap()
            .push((employee_id, approver_employee_id));
        Ok(ApproveByEmployee::Approved)
    }

    async fn registration_count(
        &self,
        _tenant_id: Uuid,
        employee_id: Uuid,
        format: &str,
    ) -> Result<(i64, bool), DbError> {
        self.check()?;
        let modules = self.modules.lock().unwrap();
        let rows = self.rows.lock().unwrap();
        let of_format: Vec<_> = rows
            .iter()
            .filter(|r| modules.contains(&r.employee_id) == (format == FORMAT_MODULE))
            .collect();
        let enrolled = of_format.iter().any(|r| r.employee_id == employee_id);
        Ok((of_format.len() as i64, enrolled))
    }

    async fn update_learned(
        &self,
        _tenant_id: Uuid,
        id: Uuid,
        template: &str,
        read_updated_at: DateTime<Utc>,
    ) -> Result<bool, DbError> {
        self.learned_calls
            .lock()
            .unwrap()
            .push((id, read_updated_at));
        if self.conflict.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .iter_mut()
            .find(|r| r.id == id && r.updated_at == read_updated_at)
            .unwrap();
        row.template = template.to_string();
        row.updated_at = Utc::now();
        Ok(true)
    }

    async fn upsert_device_record(
        &self,
        _tenant_id: Uuid,
        employee_id: Uuid,
        device_id: &str,
        record: &str,
    ) -> Result<Option<DateTime<Utc>>, DbError> {
        self.check()?;
        let approved_module = self.modules.lock().unwrap().contains(&employee_id)
            && !self.pending.lock().unwrap().contains(&employee_id)
            && self
                .rows
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.employee_id == employee_id);
        if !approved_module {
            return Ok(None);
        }
        let now = Utc::now();
        let mut devices = self.device_records.lock().unwrap();
        devices.retain(|d| !(d.employee_id == employee_id && d.device_id == device_id));
        devices.push(DeviceRecordRow {
            employee_id,
            device_id: device_id.to_string(),
            record: record.to_string(),
            updated_at: now,
        });
        Ok(Some(now))
    }

    async fn list_device_records(&self, _tenant_id: Uuid) -> Result<Vec<DeviceRecordRow>, DbError> {
        self.check()?;
        if self.fail_device_list.load(Ordering::SeqCst) {
            return Err(DbError::Other("pool timed out".into()));
        }
        Ok(self.device_records.lock().unwrap().clone())
    }

    async fn delete(&self, _tenant_id: Uuid, employee_id: Uuid) -> Result<bool, DbError> {
        self.check()?;
        self.device_records
            .lock()
            .unwrap()
            .retain(|d| d.employee_id != employee_id);
        let mut rows = self.rows.lock().unwrap();
        let before = rows.len();
        rows.retain(|r| r.employee_id != employee_id);
        Ok(rows.len() < before)
    }
}

/// 承認する管理者の user_id (`call_as(Some("admin"), ..)` が付ける)。
const ADMIN_ID: Uuid = Uuid::from_u128(0xAD);

/// 端末の鍵 (`AuthUser` を持たない) として叩く。
async fn call(repo: &Arc<FakeRepo>, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    call_as(repo, None, method, uri, body).await
}

/// `role` が `Some` なら、tenant_header が `X-User-*` から復元する `AuthUser` を付けて叩く。
async fn call_as(
    repo: &Arc<FakeRepo>,
    role: Option<&str>,
    method: &str,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    call_full(repo, role, &[], method, uri, body).await
}

/// `call_as` に、追加のリクエストヘッダ (`X-Device-Id` など) を付けられるようにしたもの。
async fn call_full(
    repo: &Arc<FakeRepo>,
    role: Option<&str>,
    extra_headers: &[(&str, &[u8])],
    method: &str,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let state = VeinState {
        templates: repo.clone(),
    };
    let mut app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(Uuid::nil())));
    if let Some(role) = role {
        app = app.layer(Extension(AuthUser {
            user_id: ADMIN_ID,
            email: "admin@example.com".to_string(),
            name: String::new(),
            tenant_id: Uuid::nil(),
            tenant_slug: None,
            role: role.to_string(),
        }));
    }
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    for (k, v) in extra_headers {
        req = req.header(*k, *v);
    }
    let req = req.body(Body::from(body.to_string())).unwrap();
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

fn hex(seed: u64) -> String {
    synth::hex(&synth::chara(seed))
}

/// 乗務員を作って種 `seed` の指を登録する (承認待ちのまま)。`updated_at` も返す。
async fn enroll_pending(repo: &Arc<FakeRepo>, name: &str, seed: u64) -> (Uuid, Value) {
    let id = repo.add_employee(name);
    let body = json!({ "charas": [hex(seed), hex(seed)] });
    let (status, v) = call(repo, "PUT", &format!("/vein/templates/{id}"), body).await;
    assert_eq!(status, StatusCode::OK);
    (id, v["updated_at"].clone())
}

/// 管理者として承認する。
async fn approve(repo: &Arc<FakeRepo>, id: Uuid, updated_at: &Value) -> StatusCode {
    let uri = format!("/vein/templates/{id}/approve");
    let body = json!({ "updated_at": updated_at });
    call_as(repo, Some("admin"), "POST", &uri, body).await.0
}

/// 乗務員を作って種 `seed` の指を登録し、管理者が承認する (照合に載る)。
async fn enroll(repo: &Arc<FakeRepo>, name: &str, seed: u64) -> Uuid {
    let (id, updated_at) = enroll_pending(repo, name, seed).await;
    assert_eq!(approve(repo, id, &updated_at).await, StatusCode::NO_CONTENT);
    id
}

#[tokio::test]
async fn put_enrolls_a_template_that_reads_back() {
    let repo = Arc::new(FakeRepo::default());
    let id = repo.add_employee("山田");
    let body = json!({ "charas": [hex(1), hex(1)] });
    let (status, v) = call(&repo, "PUT", &format!("/vein/templates/{id}"), body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["employee_id"], id.to_string());
    assert!(v["updated_at"].is_string());
    let mut lib = vein_match_search::Library::new(2).unwrap();
    assert_eq!(lib.import_temp_b64(1, &repo.template_of(id)), 0);
}

#[tokio::test]
async fn put_rejects_unsupported_charas_with_422() {
    let repo = Arc::new(FakeRepo::default());
    let id = repo.add_employee("山田");
    let uri = format!("/vein/templates/{id}");
    for (chara, code) in [
        ("9911AABB", "unsupported_chara_format"),
        ("", "unsupported_chara_format"),
        ("BDBD0", "invalid_chara_hex"),
    ] {
        let (status, v) = call(&repo, "PUT", &uri, json!({ "charas": [hex(1), chara] })).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{chara:?}");
        assert_eq!(v["error"], code);
    }
    let (status, v) = call(&repo, "PUT", &uri, json!({ "charas": [] })).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "invalid_chara_count");
    assert!(repo.rows.lock().unwrap().is_empty());
}

#[tokio::test]
async fn put_unknown_employee_is_404_and_db_error_is_500() {
    let repo = Arc::new(FakeRepo::default());
    let body = json!({ "charas": [hex(1)] });
    let uri = format!("/vein/templates/{}", Uuid::new_v4());
    let (status, v) = call(&repo, "PUT", &uri, body.clone()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(v["error"], "employee_not_found");
    repo.fail_upsert.store(true, Ordering::SeqCst);
    let (status, _) = call(&repo, "PUT", &uri, body.clone()).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = call(&repo, "PUT", &uri, body).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn identify_with_no_templates_returns_null() {
    let repo = Arc::new(FakeRepo::default());
    let (status, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v, json!({ "employee_id": null }));
}

#[tokio::test]
async fn identify_hits_returns_name_and_writes_back_learned_template() {
    let repo = Arc::new(FakeRepo::default());
    let id = enroll(&repo, "山田", 1).await;
    let before = repo.rows.lock().unwrap()[0].clone();
    let (status, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v, json!({ "employee_id": id, "name": "山田" }));
    // 読んだ updated_at を条件に書き戻し、テンプレートが学習後のものに変わる
    assert_eq!(
        *repo.learned_calls.lock().unwrap(),
        vec![(before.id, before.updated_at)]
    );
    assert_ne!(repo.template_of(id), before.template);
}

#[tokio::test]
async fn identify_picks_the_right_one_of_two_and_misses_unknown() {
    let repo = Arc::new(FakeRepo::default());
    enroll(&repo, "山田", 1).await;
    let suzuki = enroll(&repo, "鈴木", 2).await;
    let (_, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(2) })).await;
    assert_eq!(v, json!({ "employee_id": suzuki, "name": "鈴木" }));
    let (status, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(3) })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v, json!({ "employee_id": null }));
}

#[tokio::test]
async fn identify_discards_learned_template_on_conflict() {
    let repo = Arc::new(FakeRepo::default());
    let id = enroll(&repo, "山田", 1).await;
    let before = repo.template_of(id);
    repo.conflict.store(true, Ordering::SeqCst);
    let (status, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    // 照合の結果は返し、学習後のテンプレートは捨てる
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["employee_id"], id.to_string());
    assert_eq!(repo.learned_calls.lock().unwrap().len(), 1);
    assert_eq!(repo.template_of(id), before);
}

#[tokio::test]
async fn identify_skips_unreadable_rows() {
    let repo = Arc::new(FakeRepo::default());
    let broken = enroll(&repo, "壊れ", 5).await;
    repo.rows.lock().unwrap()[0].template = "not-a-template".to_string();
    let id = enroll(&repo, "山田", 1).await;
    let (_, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    assert_eq!(v["employee_id"], id.to_string());
    assert_ne!(v["employee_id"], broken.to_string());
}

#[tokio::test]
async fn identify_rejects_bad_chara_and_over_500_and_db_error() {
    let repo = Arc::new(FakeRepo::default());
    let (status, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": "ABCD" })).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "unsupported_chara_format");
    assert!(v["message"].as_str().unwrap().contains("未対応の形式"));

    let id = enroll(&repo, "山田", 1).await;
    let row = repo.rows.lock().unwrap()[0].clone();
    *repo.rows.lock().unwrap() = vec![row; MAX_TEMPLATES + 1];
    let (status, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "too_many_templates");
    assert!(v["message"].as_str().unwrap().contains("501"));
    assert!(!id.is_nil());

    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn get_lists_templates_with_logic_version() {
    let repo = Arc::new(FakeRepo::default());
    let id = enroll(&repo, "山田", 1).await;
    let (status, v) = call(&repo, "GET", "/vein/templates", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["logic_version"], vein_match_search::VERSION);
    assert_eq!(v["templates"][0]["employee_id"], id.to_string());
    assert_eq!(v["templates"][0]["template"], repo.template_of(id));
    assert!(v["templates"][0]["updated_at"].is_string());
    assert!(v["templates"][0].get("name").is_none());
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = call(&repo, "GET", "/vein/templates", Value::Null).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn delete_removes_then_404_and_db_error_is_500() {
    let repo = Arc::new(FakeRepo::default());
    let id = enroll(&repo, "山田", 1).await;
    let uri = format!("/vein/templates/{id}");
    let admin = Some("admin");
    let (status, _) = call_as(&repo, admin, "DELETE", &uri, Value::Null).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, v) = call_as(&repo, admin, "DELETE", &uri, Value::Null).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(v["error"], "vein_template_not_found");
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = call_as(&repo, admin, "DELETE", &uri, Value::Null).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn delete_is_admin_only() {
    let repo = Arc::new(FakeRepo::default());
    let id = enroll(&repo, "山田", 1).await;
    let uri = format!("/vein/templates/{id}");
    // AuthUser が無い (端末の鍵) / admin 以外の role は 403 で、消えない
    for role in [None, Some("user"), Some("Admin"), Some("")] {
        let (status, v) = call_as(&repo, role, "DELETE", &uri, Value::Null).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{role:?}");
        assert_eq!(v["error"], "forbidden");
    }
    let (_, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    assert_eq!(v, json!({ "employee_id": id, "name": "山田" }));
}

/// 登録済みを `n` 人ぶん (テンプレートの中身は PUT の判定に関係しないので空) 並べる。
fn fill_rows(repo: &FakeRepo, n: usize) {
    let mut rows = repo.rows.lock().unwrap();
    for _ in 0..n {
        rows.push(VeinTemplateRow {
            id: Uuid::new_v4(),
            employee_id: Uuid::new_v4(),
            name: "既存".to_string(),
            template: String::new(),
            updated_at: Utc::now(),
            registered_device_id: None,
        });
    }
}

#[tokio::test]
async fn put_new_employee_at_500_is_rejected_but_overwrite_passes() {
    let repo = Arc::new(FakeRepo::default());
    let body = json!({ "charas": [hex(1)] });

    // 499 人 + 新規 → 500 人目は通る
    fill_rows(&repo, MAX_TEMPLATES - 1);
    let id500 = repo.add_employee("500 人目");
    let (status, _) = call(
        &repo,
        "PUT",
        &format!("/vein/templates/{id500}"),
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(repo.rows.lock().unwrap().len(), MAX_TEMPLATES);

    // 500 人 + 新規 → 422 (全員の照合を止めないよう、ここで断る)
    let id501 = repo.add_employee("501 人目");
    let (status, v) = call(
        &repo,
        "PUT",
        &format!("/vein/templates/{id501}"),
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "too_many_templates");
    assert!(v["message"].as_str().unwrap().contains("500"));
    assert_eq!(repo.rows.lock().unwrap().len(), MAX_TEMPLATES);

    // 500 人 + 既存の上書き → 人数が増えないので通る
    let (status, _) = call(&repo, "PUT", &format!("/vein/templates/{id500}"), body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(repo.rows.lock().unwrap().len(), MAX_TEMPLATES);
}

#[tokio::test]
async fn put_is_pending_until_an_admin_approves() {
    let repo = Arc::new(FakeRepo::default());
    let (id, updated_at) = enroll_pending(&repo, "山田", 1).await;

    // 承認待ちは照合にも GET にも出ない
    let (_, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    assert_eq!(v, json!({ "employee_id": null }));
    let (_, v) = call(&repo, "GET", "/vein/templates", Value::Null).await;
    assert_eq!(v["templates"], json!([]));

    // 管理者の一覧には出る (テンプレートは返さない)
    let (status, v) = call_as(
        &repo,
        Some("admin"),
        "GET",
        "/vein/templates/pending",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v, json!([{ "employee_id": id, "updated_at": updated_at }]));

    // 承認すると照合にも GET にも出て、一覧から消え、承認した管理者が残る
    assert_eq!(
        approve(&repo, id, &updated_at).await,
        StatusCode::NO_CONTENT
    );
    let (_, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    assert_eq!(v, json!({ "employee_id": id, "name": "山田" }));
    let (_, v) = call(&repo, "GET", "/vein/templates", Value::Null).await;
    assert_eq!(v["templates"][0]["employee_id"], id.to_string());
    let (_, v) = call_as(
        &repo,
        Some("admin"),
        "GET",
        "/vein/templates/pending",
        Value::Null,
    )
    .await;
    assert_eq!(v, json!([]));
    assert_eq!(*repo.approved_by.lock().unwrap(), vec![ADMIN_ID]);
}

#[tokio::test]
async fn re_registering_returns_to_pending() {
    let repo = Arc::new(FakeRepo::default());
    let id = enroll(&repo, "山田", 1).await;
    // 承認済みの乗務員の上書きも承認待ちに戻り、照合から外れる
    let body = json!({ "charas": [hex(2), hex(2)] });
    let (status, v) = call(&repo, "PUT", &format!("/vein/templates/{id}"), body).await;
    assert_eq!(status, StatusCode::OK);
    let (_, found) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(2) })).await;
    assert_eq!(found, json!({ "employee_id": null }));
    let (_, pending) = call_as(
        &repo,
        Some("admin"),
        "GET",
        "/vein/templates/pending",
        Value::Null,
    )
    .await;
    assert_eq!(
        pending,
        json!([{ "employee_id": id, "updated_at": v["updated_at"] }])
    );
}

#[tokio::test]
async fn approve_conflicts_on_stale_version_or_already_approved() {
    let repo = Arc::new(FakeRepo::default());
    let (id, first) = enroll_pending(&repo, "山田", 1).await;

    // 登録し直された後に、古い updated_at で承認しようとすると 409 (別の指を承認しない)
    let body = json!({ "charas": [hex(2), hex(2)] });
    let (_, v) = call(&repo, "PUT", &format!("/vein/templates/{id}"), body).await;
    assert_eq!(approve(&repo, id, &first).await, StatusCode::CONFLICT);
    assert_eq!(
        approve(&repo, id, &v["updated_at"]).await,
        StatusCode::NO_CONTENT
    );

    // 承認済みの再承認と、登録の無い乗務員は 409
    assert_eq!(
        approve(&repo, id, &v["updated_at"]).await,
        StatusCode::CONFLICT
    );
    let none = Uuid::new_v4();
    assert_eq!(
        approve(&repo, none, &v["updated_at"]).await,
        StatusCode::CONFLICT
    );
    let uri = format!("/vein/templates/{id}/approve");
    let (_, e) = call_as(
        &repo,
        Some("admin"),
        "POST",
        &uri,
        json!({ "updated_at": v["updated_at"] }),
    )
    .await;
    assert_eq!(e["error"], "approve_conflict");
}

#[tokio::test]
async fn pending_and_approve_are_admin_only() {
    let repo = Arc::new(FakeRepo::default());
    let (id, updated_at) = enroll_pending(&repo, "山田", 1).await;
    let approve_uri = format!("/vein/templates/{id}/approve");
    let body = json!({ "updated_at": updated_at });
    // AuthUser が無い (端末の鍵) / admin 以外の role は 403
    for role in [None, Some("user"), Some("Admin"), Some("")] {
        let (status, v) = call_as(&repo, role, "GET", "/vein/templates/pending", Value::Null).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{role:?}");
        assert_eq!(v["error"], "forbidden");
        let (status, _) = call_as(&repo, role, "POST", &approve_uri, body.clone()).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{role:?}");
    }
    // 403 では承認されない
    let (_, v) = call(&repo, "GET", "/vein/templates", Value::Null).await;
    assert_eq!(v["templates"], json!([]));
    assert!(repo.approved_by.lock().unwrap().is_empty());
}

/// 管理者として `GET /vein/templates/{which}` (pending / approved) を叩く。
async fn list_as_admin(repo: &Arc<FakeRepo>, which: &str) -> (StatusCode, Value) {
    let uri = format!("/vein/templates/{which}");
    call_as(repo, Some("admin"), "GET", &uri, Value::Null).await
}

#[tokio::test]
async fn approved_lists_only_approved_without_templates() {
    let repo = Arc::new(FakeRepo::default());
    let (pending_id, pending_at) = enroll_pending(&repo, "山田", 1).await;
    let (approved_id, approved_at) = enroll_pending(&repo, "鈴木", 2).await;
    assert_eq!(
        approve(&repo, approved_id, &approved_at).await,
        StatusCode::NO_CONTENT
    );

    // 承認済みだけが、employee_id と updated_at の 2 つだけで出る (テンプレートは返さない)
    let (status, v) = list_as_admin(&repo, "approved").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        v,
        json!([{ "employee_id": approved_id, "updated_at": approved_at }])
    );
    // pending は今までどおり承認待ちだけ
    let (status, v) = list_as_admin(&repo, "pending").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        v,
        json!([{ "employee_id": pending_id, "updated_at": pending_at }])
    );

    // 承認済みを消すと一覧から消える
    let uri = format!("/vein/templates/{approved_id}");
    let (status, _) = call_as(&repo, Some("admin"), "DELETE", &uri, Value::Null).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(list_as_admin(&repo, "approved").await.1, json!([]));
}

#[tokio::test]
async fn approved_is_admin_only_and_db_error_is_500() {
    let repo = Arc::new(FakeRepo::default());
    let id = enroll(&repo, "山田", 1).await;
    for role in [None, Some("user"), Some("Admin"), Some("")] {
        let (status, v) =
            call_as(&repo, role, "GET", "/vein/templates/approved", Value::Null).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{role:?}");
        assert_eq!(v["error"], "forbidden");
    }
    assert_eq!(
        list_as_admin(&repo, "approved").await.1[0]["employee_id"],
        id.to_string()
    );
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = list_as_admin(&repo, "approved").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn pending_and_approve_db_error_is_500() {
    let repo = Arc::new(FakeRepo::default());
    let (id, updated_at) = enroll_pending(&repo, "山田", 1).await;
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = call_as(
        &repo,
        Some("admin"),
        "GET",
        "/vein/templates/pending",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        approve(&repo, id, &updated_at).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

/// 長さ `n` バイトの記録 (中身は何でもよい) の base64。
fn record(n: usize) -> String {
    STANDARD.encode(vec![0xA5u8; n])
}

/// `PUT /vein/templates/{id}/module` を端末の鍵で叩く。
async fn put_module(repo: &Arc<FakeRepo>, id: Uuid, record: &str) -> (StatusCode, Value) {
    let uri = format!("/vein/templates/{id}/module");
    call(repo, "PUT", &uri, json!({ "record": record })).await
}

#[tokio::test]
async fn put_module_stores_the_record_as_is_and_is_pending() {
    let repo = Arc::new(FakeRepo::default());
    let id = repo.add_employee("山田");
    let rec = record(MAX_MODULE_RECORD_BYTES);
    let (status, v) = put_module(&repo, id, &rec).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["employee_id"], id.to_string());
    assert!(v["updated_at"].is_string());
    // base64 のまま・module の行として入り、承認待ち (一覧に出ない)
    assert_eq!(repo.template_of(id), rec);
    assert_eq!(*repo.modules.lock().unwrap(), vec![id]);
    let (_, v) = call(&repo, "GET", "/vein/module-templates", Value::Null).await;
    assert_eq!(v["templates"], json!([]));
}

/// `X-Device-Id` を付けて `PUT /vein/templates/{id}/module` を叩く。
async fn put_module_from(repo: &Arc<FakeRepo>, id: Uuid, device: &[u8]) -> (StatusCode, Value) {
    let uri = format!("/vein/templates/{id}/module");
    let body = json!({ "record": record(4) });
    call_full(repo, None, &[("x-device-id", device)], "PUT", &uri, body).await
}

fn stored_device(repo: &FakeRepo, id: Uuid) -> Option<String> {
    let rows = repo.rows.lock().unwrap();
    rows.iter()
        .find(|r| r.employee_id == id)
        .unwrap()
        .registered_device_id
        .clone()
}

#[tokio::test]
async fn put_module_saves_x_device_id_and_get_returns_it() {
    let repo = Arc::new(FakeRepo::default());
    let id = repo.add_employee("山田");
    let other = repo.add_employee("鈴木");
    let max = "a".repeat(64);
    // ヘッダ無し = NULL。ヘッダ有りは保存され、登録し直すと上書き (ヘッダ無しなら NULL に戻る)
    assert_eq!(put_module(&repo, id, &record(4)).await.0, StatusCode::OK);
    assert_eq!(stored_device(&repo, id), None);
    for device in ["alarm:0123456789abcdef", "dev-1_X", "a", max.as_str()] {
        let (status, _) = put_module_from(&repo, id, device.as_bytes()).await;
        assert_eq!(status, StatusCode::OK, "{device}");
        assert_eq!(stored_device(&repo, id).as_deref(), Some(device));
    }
    assert_eq!(put_module(&repo, id, &record(4)).await.0, StatusCode::OK);
    assert_eq!(stored_device(&repo, id), None);
    // chara の PUT は端末を持たない
    let (status, _) = call(
        &repo,
        "PUT",
        &format!("/vein/templates/{other}"),
        json!({ "charas": [hex(1)] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored_device(&repo, other), None);

    // 承認済みの一覧は registered_device_id を null か文字列で返す
    assert_eq!(
        put_module_from(&repo, id, b"alarm:0123456789abcdef")
            .await
            .0,
        StatusCode::OK
    );
    repo.pending.lock().unwrap().clear();
    let (_, v) = call(&repo, "GET", "/vein/module-templates", Value::Null).await;
    assert_eq!(
        v["templates"][0]["registered_device_id"],
        "alarm:0123456789abcdef"
    );
    assert_eq!(put_module(&repo, id, &record(4)).await.0, StatusCode::OK);
    repo.pending.lock().unwrap().clear();
    let (_, v) = call(&repo, "GET", "/vein/module-templates", Value::Null).await;
    assert!(v["templates"][0]["registered_device_id"].is_null());
}

#[tokio::test]
async fn put_module_rejects_invalid_x_device_id() {
    let repo = Arc::new(FakeRepo::default());
    let id = repo.add_employee("山田");
    let too_long = "a".repeat(65);
    let bad: [&[u8]; 6] = [
        b"",
        too_long.as_bytes(),
        b"a b",
        b"a/b",
        "端末".as_bytes(),
        &[0xff, 0xfe],
    ];
    for device in bad {
        let (status, v) = put_module_from(&repo, id, device).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{device:?}");
        assert_eq!(v["error"], "invalid_device_id");
    }
    assert!(repo.rows.lock().unwrap().is_empty());
}

#[tokio::test]
async fn put_module_rejects_bad_base64_empty_and_too_long() {
    let repo = Arc::new(FakeRepo::default());
    let id = repo.add_employee("山田");
    for rec in ["not base64!", ""] {
        let (status, v) = put_module(&repo, id, rec).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{rec:?}");
        assert_eq!(v["error"], "invalid_module_record");
    }
    let (status, v) = put_module(&repo, id, &record(MAX_MODULE_RECORD_BYTES + 1)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "module_record_too_long");
    assert!(repo.rows.lock().unwrap().is_empty());
}

#[tokio::test]
async fn put_module_unknown_employee_is_404_and_db_error_is_500() {
    let repo = Arc::new(FakeRepo::default());
    let id = Uuid::new_v4();
    let (status, v) = put_module(&repo, id, &record(8)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(v["error"], "employee_not_found");
    repo.fail_upsert.store(true, Ordering::SeqCst);
    assert_eq!(
        put_module(&repo, id, &record(8)).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    repo.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        put_module(&repo, id, &record(8)).await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn put_module_new_employee_at_500_is_409_but_overwrite_and_chara_rows_pass() {
    let repo = Arc::new(FakeRepo::default());
    // chara の行は module の人数に数えない
    fill_rows(&repo, MAX_TEMPLATES);
    let mut last = Uuid::nil();
    for i in 0..MAX_MODULE_RECORDS {
        last = repo.add_employee(&format!("m{i}"));
        assert_eq!(put_module(&repo, last, &record(8)).await.0, StatusCode::OK);
    }
    let over = repo.add_employee("501 人目");
    let (status, v) = put_module(&repo, over, &record(8)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(v["error"], "too_many_module_records");
    assert!(v["message"].as_str().unwrap().contains("500"));
    // 既に module の行がある乗務員の上書きは通る
    assert_eq!(put_module(&repo, last, &record(9)).await.0, StatusCode::OK);
}

#[tokio::test]
async fn module_rows_stay_out_of_identify_and_the_chara_list() {
    let repo = Arc::new(FakeRepo::default());
    let chara_id = enroll(&repo, "山田", 1).await;
    let id = repo.add_employee("鈴木");
    let (_, put) = put_module(&repo, id, &record(64)).await;
    assert_eq!(
        approve(&repo, id, &put["updated_at"]).await,
        StatusCode::NO_CONTENT
    );

    // 承認済みの module 行は module の一覧にだけ出る (形式を問わない承認・一覧は両方を扱う)
    let (status, v) = call(&repo, "GET", "/vein/module-templates", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        v,
        json!({ "templates": [{
            "employee_id": id,
            "record": record(64),
            "updated_at": put["updated_at"],
            "registered_device_id": null,
            "composed": false,
        }] })
    );
    let (_, v) = call(&repo, "GET", "/vein/templates", Value::Null).await;
    assert_eq!(v["templates"].as_array().unwrap().len(), 1);
    assert_eq!(v["templates"][0]["employee_id"], chara_id.to_string());
    let (_, v) = call(&repo, "POST", "/vein/identify", json!({ "chara": hex(1) })).await;
    assert_eq!(v["employee_id"], chara_id.to_string());
    assert_eq!(repo.learned_calls.lock().unwrap().len(), 1);

    // chara で登録し直すと chara に戻り、module の一覧から外れる
    let body = json!({ "charas": [hex(2), hex(2)] });
    assert_eq!(
        call(&repo, "PUT", &format!("/vein/templates/{id}"), body)
            .await
            .0,
        StatusCode::OK
    );
    assert!(repo.modules.lock().unwrap().is_empty());
}

#[tokio::test]
async fn list_module_templates_db_error_is_500() {
    let repo = Arc::new(FakeRepo::default());
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = call(&repo, "GET", "/vein/module-templates", Value::Null).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

/// module の記録を登録して `updated_at` を返す (承認待ちのまま)。
async fn pending_module(repo: &Arc<FakeRepo>, id: Uuid) -> Value {
    let (status, v) = put_module(repo, id, &record(8)).await;
    assert_eq!(status, StatusCode::OK);
    v["updated_at"].clone()
}

/// 運行管理者 (社員) の承認を端末の token (`AuthUser` 無し) で叩く。
async fn approve_by_employee(
    repo: &Arc<FakeRepo>,
    id: Uuid,
    approver: Uuid,
    updated_at: &Value,
) -> (StatusCode, Value) {
    let uri = format!("/vein/templates/{id}/approve-by-employee");
    let body = json!({ "approver_employee_id": approver, "updated_at": updated_at });
    call(repo, "POST", &uri, body).await
}

/// 承認済みの module の記録を持つ運行管理者を作る。
async fn approved_manager(repo: &Arc<FakeRepo>, name: &str) -> Uuid {
    let id = repo.add_employee(name);
    let updated_at = pending_module(repo, id).await;
    assert_eq!(approve(repo, id, &updated_at).await, StatusCode::NO_CONTENT);
    repo.managers.lock().unwrap().push(id);
    id
}

#[tokio::test]
async fn pending_modules_lists_only_pending_module_rows_without_records() {
    let repo = Arc::new(FakeRepo::default());
    let target = repo.add_employee("山田");
    let updated_at = pending_module(&repo, target).await;
    // chara の承認待ち・承認済みの module は載らない
    enroll_pending(&repo, "鈴木", 1).await;
    approved_manager(&repo, "佐藤").await;
    let (status, v) = call(&repo, "GET", "/vein/module-templates/pending", Value::Null).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        v,
        json!({ "templates": [{
            "employee_id": target,
            "name": "山田",
            "updated_at": updated_at,
            "registered_device_id": null,
        }] })
    );
}

#[tokio::test]
async fn pending_modules_db_error_is_500() {
    let repo = Arc::new(FakeRepo::default());
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = call(&repo, "GET", "/vein/module-templates/pending", Value::Null).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn approve_by_employee_approves_and_records_the_approver() {
    let repo = Arc::new(FakeRepo::default());
    let manager = approved_manager(&repo, "管理者").await;
    let target = repo.add_employee("山田");
    let updated_at = pending_module(&repo, target).await;
    let (status, _) = approve_by_employee(&repo, target, manager, &updated_at).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        *repo.approved_by_employee.lock().unwrap(),
        vec![(target, manager)]
    );
    // 承認済みなので module の一覧に載り、もう一度は 409
    let (_, v) = call(&repo, "GET", "/vein/module-templates", Value::Null).await;
    assert_eq!(v["templates"].as_array().unwrap().len(), 2);
    let (status, v) = approve_by_employee(&repo, target, manager, &updated_at).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(v["error"], "approve_conflict");
}

#[tokio::test]
async fn approve_by_employee_conflicts_on_stale_version() {
    let repo = Arc::new(FakeRepo::default());
    let manager = approved_manager(&repo, "管理者").await;
    let target = repo.add_employee("山田");
    let stale = pending_module(&repo, target).await;
    let _fresh = pending_module(&repo, target).await;
    let (status, v) = approve_by_employee(&repo, target, manager, &stale).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(v["error"], "approve_conflict");
    assert!(repo.approved_by_employee.lock().unwrap().is_empty());
}

#[tokio::test]
async fn approve_by_employee_rejects_non_manager_unenrolled_and_self() {
    let repo = Arc::new(FakeRepo::default());
    let target = repo.add_employee("山田");
    let updated_at = pending_module(&repo, target).await;
    // 運行管理者でない (承認済みの記録は持つ)
    let driver = repo.add_employee("乗務員");
    let driver_at = pending_module(&repo, driver).await;
    assert_eq!(
        approve(&repo, driver, &driver_at).await,
        StatusCode::NO_CONTENT
    );
    let (status, v) = approve_by_employee(&repo, target, driver, &updated_at).await;
    assert_eq!(
        (status, v["error"].as_str()),
        (StatusCode::FORBIDDEN, Some("approver_not_manager"))
    );
    // 運行管理者だが、自分の記録が承認待ち / 無い
    let waiting = repo.add_employee("待ち");
    pending_module(&repo, waiting).await;
    let none = repo.add_employee("未登録");
    for approver in [waiting, none] {
        repo.managers.lock().unwrap().push(approver);
        let (status, v) = approve_by_employee(&repo, target, approver, &updated_at).await;
        assert_eq!(
            (status, v["error"].as_str()),
            (StatusCode::FORBIDDEN, Some("approver_not_enrolled"))
        );
    }
    // 自己承認
    let manager = approved_manager(&repo, "管理者").await;
    let own = json!("2026-01-01T00:00:00Z");
    let (status, v) = approve_by_employee(&repo, manager, manager, &own).await;
    assert_eq!(
        (status, v["error"].as_str()),
        (StatusCode::FORBIDDEN, Some("self_approval"))
    );
    assert!(repo.approved_by_employee.lock().unwrap().is_empty());
}

#[tokio::test]
async fn approve_by_employee_db_error_is_500() {
    let repo = Arc::new(FakeRepo::default());
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = approve_by_employee(
        &repo,
        Uuid::new_v4(),
        Uuid::new_v4(),
        &json!("2026-01-01T00:00:00Z"),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

// --- 端末ごとの記録 (Refs ippoan/alc-app#558) ---

/// 同じ種の特徴量 `n` 件から組んだ mode 1 のテンプレート (base64)。
fn template_of_seed(seed: u64, n: usize) -> String {
    matcher::enroll_template(&vec![synth::chara(seed); n]).unwrap()
}

/// base64 のテンプレートの件数。
fn count_of(b64: &str) -> u8 {
    let t = STANDARD.decode(b64).unwrap();
    vein_match_search::parse_template(&t).unwrap().0.count
}

/// 期待値: `a` の 3 件の後ろに `b` の 3 件を並べた記録 (base64)。
fn mixed_of(a: &str, b: &str) -> String {
    let (a, b) = (STANDARD.decode(a).unwrap(), STANDARD.decode(b).unwrap());
    STANDARD.encode(vein_match_search::mix_templates(&[(&a, 3), (&b, 3)]).unwrap())
}

/// 承認済みの module の行を作る (`device` はその行の登録した端末)。
fn approved_module(repo: &FakeRepo, name: &str, record: &str, device: Option<&str>) -> Uuid {
    let id = repo.add_employee(name);
    repo.rows.lock().unwrap().push(VeinTemplateRow {
        id: Uuid::new_v4(),
        employee_id: id,
        name: name.to_string(),
        template: record.to_string(),
        updated_at: ts(100),
        registered_device_id: device.map(str::to_owned),
    });
    repo.modules.lock().unwrap().push(id);
    id
}

/// 基準の時刻から `n` 秒後。
fn ts(n: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + n, 0).unwrap()
}

fn add_device_record(repo: &FakeRepo, id: Uuid, device: &str, record: &str, at: i64) {
    repo.device_records.lock().unwrap().push(DeviceRecordRow {
        employee_id: id,
        device_id: device.to_string(),
        record: record.to_string(),
        updated_at: ts(at),
    });
}

async fn put_device(
    repo: &Arc<FakeRepo>,
    id: Uuid,
    device: Option<&[u8]>,
    record: &str,
) -> (StatusCode, Value) {
    let uri = format!("/vein/templates/{id}/module/devices");
    let headers: Vec<(&str, &[u8])> = device.map(|d| ("x-device-id", d)).into_iter().collect();
    call_full(
        repo,
        None,
        &headers,
        "PUT",
        &uri,
        json!({ "record": record }),
    )
    .await
}

/// `X-Device-Id` つきで `GET /vein/module-templates`。
async fn get_modules_as(repo: &Arc<FakeRepo>, device: &[u8]) -> (StatusCode, Value) {
    let h: [(&str, &[u8]); 1] = [("x-device-id", device)];
    call_full(repo, None, &h, "GET", "/vein/module-templates", Value::Null).await
}

#[tokio::test]
async fn put_device_record_requires_device_id_and_a_valid_record() {
    let repo = Arc::new(FakeRepo::default());
    let id = approved_module(&repo, "山田", &record(4), Some("kiosk-1"));
    let rec = record(4);
    let (status, v) = put_device(&repo, id, None, &rec).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "device_id_required");
    let (status, v) = put_device(&repo, id, Some(b"a b"), &rec).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "invalid_device_id");
    for (bad, code) in [
        ("!!!".to_string(), "invalid_module_record"),
        (String::new(), "invalid_module_record"),
        (
            record(MAX_MODULE_RECORD_BYTES + 1),
            "module_record_too_long",
        ),
    ] {
        let (status, v) = put_device(&repo, id, Some(b"kiosk-2"), &bad).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(v["error"], code);
    }
    assert!(repo.device_records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn put_device_record_needs_an_approved_module_row() {
    let repo = Arc::new(FakeRepo::default());
    let rec = record(4);
    // 登録が無い・chara の行・承認待ちの module の行はどれも 409
    let nobody = Uuid::new_v4();
    let (status, v) = put_device(&repo, nobody, Some(b"kiosk-2"), &rec).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(v["error"], "no_approved_module_record");
    let chara = enroll(&repo, "鈴木", 1).await;
    assert_eq!(
        put_device(&repo, chara, Some(b"kiosk-2"), &rec).await.0,
        StatusCode::CONFLICT
    );
    let pending = repo.add_employee("佐藤");
    assert_eq!(put_module(&repo, pending, &rec).await.0, StatusCode::OK);
    assert_eq!(
        put_device(&repo, pending, Some(b"kiosk-2"), &rec).await.0,
        StatusCode::CONFLICT
    );
    assert!(repo.device_records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn put_device_record_upserts_per_device_without_touching_approval() {
    let repo = Arc::new(FakeRepo::default());
    let id = approved_module(&repo, "山田", &record(4), Some("kiosk-1"));
    let (status, v) = put_device(&repo, id, Some(b"kiosk-2"), "AAEC").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["employee_id"], id.to_string());
    assert!(v["updated_at"].is_string());
    // 同じ端末は差し替え、別の端末は別の行
    assert_eq!(
        put_device(&repo, id, Some(b"kiosk-2"), "AAED").await.0,
        StatusCode::OK
    );
    assert_eq!(
        put_device(&repo, id, Some(b"kiosk-3"), "AAEC").await.0,
        StatusCode::OK
    );
    let devices = repo.device_records.lock().unwrap().clone();
    assert_eq!(devices.len(), 2);
    assert_eq!(
        devices
            .iter()
            .find(|d| d.device_id == "kiosk-2")
            .unwrap()
            .record,
        "AAED"
    );
    // 元の行は承認済みのまま・record もそのまま
    assert!(!repo.pending.lock().unwrap().contains(&id));
    assert_eq!(repo.template_of(id), record(4));
}

#[tokio::test]
async fn put_device_record_db_error_is_500() {
    let repo = Arc::new(FakeRepo::default());
    let id = approved_module(&repo, "山田", &record(4), None);
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = put_device(&repo, id, Some(b"kiosk-2"), &record(4)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn module_list_without_device_id_or_device_records_is_unchanged() {
    let repo = Arc::new(FakeRepo::default());
    let base = template_of_seed(1, 3);
    let id = approved_module(&repo, "山田", &base, Some("kiosk-1"));
    let before = json!({ "templates": [{
        "employee_id": id, "record": base, "updated_at": ts(100),
        "registered_device_id": "kiosk-1", "composed": false,
    }] });
    // ヘッダ無し (管理者のログイン) はそのまま。端末ごとの記録が無い乗務員もそのまま
    let (_, v) = call(&repo, "GET", "/vein/module-templates", Value::Null).await;
    assert_eq!(v, before);
    assert_eq!(get_modules_as(&repo, b"kiosk-1").await.1, before);
    assert_eq!(get_modules_as(&repo, b"kiosk-2").await.1, before);
    // ヘッダ無しは端末ごとの記録が在っても混ぜない
    add_device_record(&repo, id, "kiosk-2", &template_of_seed(2, 3), 200);
    let (_, v) = call(&repo, "GET", "/vein/module-templates", Value::Null).await;
    assert_eq!(v, before);
}

#[tokio::test]
async fn module_list_rejects_invalid_device_id_and_db_errors() {
    let repo = Arc::new(FakeRepo::default());
    let (status, v) = get_modules_as(&repo, b"a b").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "invalid_device_id");
    repo.fail_device_list.store(true, Ordering::SeqCst);
    assert_eq!(
        get_modules_as(&repo, b"kiosk-1").await.0,
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn module_list_mixes_own_and_the_newest_other_for_the_requester() {
    let repo = Arc::new(FakeRepo::default());
    // 元の行 = kiosk-1 の 3 件。kiosk-2 (古い) と kiosk-3 (新しい) の端末ごとの記録
    let base = template_of_seed(1, 3);
    let id = approved_module(&repo, "山田", &base, Some("kiosk-1"));
    add_device_record(&repo, id, "kiosk-2", &template_of_seed(2, 3), 200);
    add_device_record(&repo, id, "kiosk-3", &template_of_seed(3, 3), 300);
    // 別の乗務員 (端末ごとの記録が無い) はそのまま
    let alone = approved_module(&repo, "鈴木", &template_of_seed(9, 3), Some("kiosk-1"));

    // kiosk-1 向け: 自分 = 元の行の 3 件、他 = 最新の kiosk-3 の 3 件
    let (status, v) = get_modules_as(&repo, b"kiosk-1").await;
    assert_eq!(status, StatusCode::OK);
    let item = &v["templates"][0];
    assert_eq!(item["employee_id"], id.to_string());
    assert_eq!(item["composed"], true);
    assert_eq!(item["registered_device_id"], "kiosk-1");
    assert_eq!(item["updated_at"], json!(ts(300)));
    let mixed = item["record"].as_str().unwrap();
    assert_eq!(count_of(mixed), 6);
    let expect = mixed_of(&base, &template_of_seed(3, 3));
    assert_eq!(mixed, expect);
    assert_eq!(v["templates"][1]["employee_id"], alone.to_string());
    assert_eq!(v["templates"][1]["composed"], false);

    // kiosk-2 向け: 自分 = kiosk-2 の端末ごとの記録、他 = 自分でない最新 = kiosk-3
    let (_, v) = get_modules_as(&repo, b"kiosk-2").await;
    let item = &v["templates"][0];
    assert_eq!(item["composed"], true);
    assert_eq!(item["updated_at"], json!(ts(300)));
    assert_eq!(
        item["record"],
        mixed_of(&template_of_seed(2, 3), &template_of_seed(3, 3))
    );
    // kiosk-3 向け: 他 = 自分でない最新 = kiosk-2 (200) より新しい元の行 (100) ではなく kiosk-2
    let (_, v) = get_modules_as(&repo, b"kiosk-3").await;
    let item = &v["templates"][0];
    assert_eq!(item["updated_at"], json!(ts(300)));
    assert_eq!(
        item["record"],
        mixed_of(&template_of_seed(3, 3), &template_of_seed(2, 3))
    );
}

#[tokio::test]
async fn module_list_without_an_own_record_mixes_the_two_newest() {
    let repo = Arc::new(FakeRepo::default());
    let base = template_of_seed(1, 3);
    let id = approved_module(&repo, "山田", &base, Some("kiosk-1"));
    add_device_record(&repo, id, "kiosk-2", &template_of_seed(2, 3), 200);
    add_device_record(&repo, id, "kiosk-3", &template_of_seed(3, 3), 300);
    // 元の行が登録した端末が分からなくても同じ
    let unknown = approved_module(&repo, "鈴木", &base, None);
    add_device_record(&repo, unknown, "kiosk-2", &template_of_seed(2, 3), 150);

    let (_, v) = get_modules_as(&repo, b"kiosk-9").await;
    let item = &v["templates"][0];
    assert_eq!(item["composed"], true);
    assert_eq!(item["updated_at"], json!(ts(300)));
    assert_eq!(
        item["record"],
        mixed_of(&template_of_seed(3, 3), &template_of_seed(2, 3))
    );
    let item = &v["templates"][1];
    assert_eq!(item["composed"], true);
    assert_eq!(item["updated_at"], json!(ts(150)));
    assert_eq!(item["record"], mixed_of(&template_of_seed(2, 3), &base));
}

#[tokio::test]
async fn module_list_with_only_the_requesters_own_records_is_unchanged() {
    let repo = Arc::new(FakeRepo::default());
    // 元の行も端末ごとの記録も kiosk-1 のもの = 他が無い
    let base = template_of_seed(1, 3);
    let id = approved_module(&repo, "山田", &base, Some("kiosk-1"));
    add_device_record(&repo, id, "kiosk-1", &template_of_seed(2, 3), 200);
    let (_, v) = get_modules_as(&repo, b"kiosk-1").await;
    let item = &v["templates"][0];
    assert_eq!(item["composed"], false);
    assert_eq!(item["record"], base);
}

#[tokio::test]
async fn module_list_falls_back_to_the_base_when_mixing_fails() {
    let repo = Arc::new(FakeRepo::default());
    // base64 として読めない記録 / テンプレートとして読めない記録
    let base = template_of_seed(1, 3);
    let a = approved_module(&repo, "山田", &base, Some("kiosk-1"));
    add_device_record(&repo, a, "kiosk-2", "!!!", 200);
    let b = approved_module(&repo, "鈴木", &base, Some("kiosk-1"));
    add_device_record(&repo, b, "kiosk-2", &record(8), 200);
    let (status, v) = get_modules_as(&repo, b"kiosk-1").await;
    assert_eq!(status, StatusCode::OK);
    for item in v["templates"].as_array().unwrap() {
        assert_eq!(item["composed"], false);
        assert_eq!(item["record"], base);
        assert_eq!(item["updated_at"], json!(ts(100)));
    }
}

#[tokio::test]
async fn delete_also_removes_the_device_records_of_that_employee() {
    let repo = Arc::new(FakeRepo::default());
    let a = approved_module(&repo, "山田", &record(4), Some("kiosk-1"));
    let b = approved_module(&repo, "鈴木", &record(4), Some("kiosk-1"));
    add_device_record(&repo, a, "kiosk-2", &record(4), 200);
    add_device_record(&repo, b, "kiosk-2", &record(4), 200);
    let (status, _) = call_as(
        &repo,
        Some("admin"),
        "DELETE",
        &format!("/vein/templates/{a}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let devices = repo.device_records.lock().unwrap().clone();
    assert_eq!(devices.len(), 1);
    assert_eq!(devices[0].employee_id, b);
}

/// `X-Device-Id` つきで `GET /vein/module-templates?<query>`。
async fn get_modules_query(
    repo: &Arc<FakeRepo>,
    device: &[u8],
    query: &str,
) -> (StatusCode, Value) {
    let h: [(&str, &[u8]); 1] = [("x-device-id", device)];
    let uri = format!("/vein/module-templates?{query}");
    call_full(repo, None, &h, "GET", &uri, Value::Null).await
}

#[tokio::test]
async fn module_list_pick_own_returns_the_requesters_record_unmixed() {
    let repo = Arc::new(FakeRepo::default());
    let base = template_of_seed(1, 6);
    let id = approved_module(&repo, "山田", &base, Some("kiosk-1"));
    add_device_record(&repo, id, "kiosk-2", &template_of_seed(2, 6), 200);
    add_device_record(&repo, id, "kiosk-2", &template_of_seed(4, 6), 250);
    add_device_record(&repo, id, "kiosk-3", &template_of_seed(3, 6), 300);

    // kiosk-2 向け: kiosk-2 の端末ごとの記録の新しいほう (他の端末のほうが新しくても混ぜない)
    let (status, v) = get_modules_query(&repo, b"kiosk-2", "pick=own").await;
    assert_eq!(status, StatusCode::OK);
    let item = &v["templates"][0];
    assert_eq!(item["record"], template_of_seed(4, 6));
    assert_eq!(item["updated_at"], json!(ts(250)));
    assert_eq!(item["composed"], false);
    assert_eq!(item["source"], "own");
    assert_eq!(item["registered_device_id"], "kiosk-1");

    // kiosk-1 向け: 端末ごとの記録は無いが元の行が kiosk-1 で撮ったもの
    let (_, v) = get_modules_query(&repo, b"kiosk-1", "pick=own").await;
    let item = &v["templates"][0];
    assert_eq!(item["record"], base);
    assert_eq!(item["updated_at"], json!(ts(100)));
    assert_eq!(item["source"], "own");
}

#[tokio::test]
async fn module_list_pick_own_without_an_own_record_takes_the_newest_other() {
    let repo = Arc::new(FakeRepo::default());
    let base = template_of_seed(1, 6);
    // 端末ごとの記録のほうが新しい
    let a = approved_module(&repo, "山田", &base, Some("kiosk-1"));
    add_device_record(&repo, a, "kiosk-2", &template_of_seed(2, 6), 200);
    add_device_record(&repo, a, "kiosk-3", &template_of_seed(3, 6), 300);
    // 元の行と同時刻 = 元の行
    let b = approved_module(&repo, "鈴木", &base, None);
    add_device_record(&repo, b, "kiosk-2", &template_of_seed(2, 6), 100);
    // 端末ごとの記録が無い
    approved_module(&repo, "佐藤", &base, Some("kiosk-1"));

    let (_, v) = get_modules_query(&repo, b"kiosk-9", "pick=own").await;
    let items = v["templates"].as_array().unwrap();
    assert_eq!(items[0]["record"], template_of_seed(3, 6));
    assert_eq!(items[0]["updated_at"], json!(ts(300)));
    for item in items {
        assert_eq!(item["source"], "other");
        assert_eq!(item["composed"], false);
    }
    assert_eq!(items[1]["record"], base);
    assert_eq!(items[2]["record"], base);
}

#[tokio::test]
async fn module_list_filters_by_employee_and_rejects_an_unknown_pick() {
    let repo = Arc::new(FakeRepo::default());
    let base = template_of_seed(1, 6);
    approved_module(&repo, "山田", &base, Some("kiosk-1"));
    let b = approved_module(&repo, "鈴木", &template_of_seed(2, 6), Some("kiosk-1"));

    let (status, v) =
        get_modules_query(&repo, b"kiosk-1", &format!("pick=own&employee_id={b}")).await;
    assert_eq!(status, StatusCode::OK);
    let items = v["templates"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["employee_id"], b.to_string());
    // 居ない人は空の一覧。ヘッダ無しでも絞れる (source は出さない)
    let (_, v) = get_modules_query(
        &repo,
        b"kiosk-1",
        &format!("employee_id={}", Uuid::new_v4()),
    )
    .await;
    assert_eq!(v["templates"], json!([]));
    let uri = format!("/vein/module-templates?employee_id={b}");
    let (_, v) = call(&repo, "GET", &uri, Value::Null).await;
    assert_eq!(v["templates"].as_array().unwrap().len(), 1);
    assert!(v["templates"][0].get("source").is_none());

    let (status, v) = get_modules_query(&repo, b"kiosk-1", "pick=mixed").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "invalid_pick");
}
