//! `routes` の口を DB なしで確かめる (repo は fake に差し替える)。
//! SQL の側 (RLS・upsert・updated_at の競合) は crates/alc-vein/tests/sql_db.rs が組み込みの PostgreSQL で見る。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Extension;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

use alc_core_wasm::{DbError, TenantId};

use super::tenant_router;
use crate::matcher::{synth, MAX_TEMPLATES};
use crate::repo::{VeinTemplateRow, VeinTemplatesRepository};
use crate::VeinState;

/// fake の repo。`employees` に居る乗務員だけ登録でき、`fail` で全メソッドが DB エラー、
/// `conflict` で書き戻しが競合 (0 行) になる。
#[derive(Default)]
struct FakeRepo {
    employees: Mutex<Vec<(Uuid, String)>>,
    rows: Mutex<Vec<VeinTemplateRow>>,
    fail: AtomicBool,
    /// upsert だけを DB エラーにする (人数の数えは通す)。
    fail_upsert: AtomicBool,
    conflict: AtomicBool,
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
        });
        Ok(Some(now))
    }

    async fn list(&self, _tenant_id: Uuid) -> Result<Vec<VeinTemplateRow>, DbError> {
        self.check()?;
        Ok(self.rows.lock().unwrap().clone())
    }

    async fn registration_count(
        &self,
        _tenant_id: Uuid,
        employee_id: Uuid,
    ) -> Result<(i64, bool), DbError> {
        self.check()?;
        let rows = self.rows.lock().unwrap();
        let enrolled = rows.iter().any(|r| r.employee_id == employee_id);
        Ok((rows.len() as i64, enrolled))
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

    async fn delete(&self, _tenant_id: Uuid, employee_id: Uuid) -> Result<bool, DbError> {
        self.check()?;
        let mut rows = self.rows.lock().unwrap();
        let before = rows.len();
        rows.retain(|r| r.employee_id != employee_id);
        Ok(rows.len() < before)
    }
}

async fn call(repo: &Arc<FakeRepo>, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let state = VeinState {
        templates: repo.clone(),
    };
    let app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(Uuid::nil())));
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
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

/// 乗務員を作って種 `seed` の指を登録する。
async fn enroll(repo: &Arc<FakeRepo>, name: &str, seed: u64) -> Uuid {
    let id = repo.add_employee(name);
    let body = json!({ "charas": [hex(seed), hex(seed)] });
    let (status, _) = call(repo, "PUT", &format!("/vein/templates/{id}"), body).await;
    assert_eq!(status, StatusCode::OK);
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
    let (status, _) = call(&repo, "DELETE", &uri, Value::Null).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, v) = call(&repo, "DELETE", &uri, Value::Null).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(v["error"], "vein_template_not_found");
    repo.fail.store(true, Ordering::SeqCst);
    let (status, _) = call(&repo, "DELETE", &uri, Value::Null).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
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
