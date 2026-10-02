//! `repo::sql` の定数と RLS を実 DB で確かめる (Refs ippoan/rust-alc-api#721)。
//!
//! 口の分岐 (422 / 0〜2 人 / 501 人 / 書き戻しの競合) は `src/routes_tests.rs` が fake の repo で見る。
//! ここは SQL の側 — `ON CONFLICT (tenant_id, employee_id)` の upsert・`updated_at` を条件にした書き戻し・
//! 削除済みの乗務員の除外・RLS のテナント分離 — と、登録 → 照合で当たる → 学習後のテンプレートが
//! 書き戻される、の一連を固定する (backend に在った `tests/vein_templates_test.rs` の 4 本と同じ事柄)。
//!
//! repo の実装 ([`PgRepo`]) は worker の `src/repo.rs` (wasm 専用でここからは使えない) と同じ形 —
//! メソッド 1 回 = 1 トランザクション、`BEGIN` → [`SET_TENANT`] → `repo::sql` の定数 → `COMMIT` — を
//! native の tokio-postgres で書いたもの。**SQL はここに写さず定数を使う。**
//!
//! DB を使うテストは `#[ignore]` (通常の `cargo test -p alc-vein` には入らない)。回し方:
//!
//! ```bash
//! VEIN_TEST_DATABASE_URL=... cargo test -p alc-vein --test sql_db -- --ignored
//! ```
//!
//! - 接続先は `container/` の image の PgBouncer (transaction mode)。**使い捨ての DB に向けること**
//!   (テストはテナントと乗務員を自分で作り、終わりに消す)
//! - ロールは RLS が効く `alc_api_app` (superuser / BYPASSRLS なら落とす)
//! - **`VEIN_TEST_DATABASE_URL` が未設定なら失敗する** (skip して緑にしない)
//!
//! 特徴量は `alc_vein::matcher::synth` の合成 (0xBDBD 構造体)。

use std::sync::Arc;

use alc_core_wasm::{DbError, TenantId};
use alc_vein::matcher::{self, synth};
use alc_vein::repo::{sql, VeinTemplateRow, VeinTemplatesRepository};
use alc_vein::routes::tenant_router;
use alc_vein::VeinState;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Extension;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls, Transaction};
use tower::ServiceExt;
use uuid::Uuid;

const URL_ENV: &str = "VEIN_TEST_DATABASE_URL";

/// worker の `in_tenant_tx` (`src/repo.rs`) がトランザクションの頭で打つ文。worker 本体は wasm 専用で
/// 定数を共有できないので写しを持ち、`set_tenant_statement_matches_worker` が食い違いを落とす。
const SET_TENANT: &str = "SELECT set_config('app.current_tenant_id', $1, true), set_config('search_path', 'alc_api', true)";

async fn connect() -> Client {
    // 接続文字列は表示しない
    let url = std::env::var(URL_ENV).unwrap_or_else(|_| {
        panic!("{URL_ENV} が未設定。実 DB のテストは接続先が無ければ失敗させる (skip しない)")
    });
    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .unwrap_or_else(|e| panic!("{URL_ENV} の DB に繋げない: {e}"));
    tokio::spawn(connection);
    client
}

fn db_err(e: tokio_postgres::Error) -> DbError {
    match e.as_db_error() {
        Some(db) => DbError::Other(format!("{} ({})", db.message(), db.code().code())),
        None => DbError::Other(e.to_string()),
    }
}

/// worker の `WorkerVeinTemplatesRepository` と同じ形の repo。`guc` を持つと、引数の tenant_id ではなく
/// `guc` を `app.current_tenant_id` に入れる (RLS だけで止まることを確かめる用)。
///
/// PgBouncer は transaction mode なので、`Row` (prepared statement を握る) は COMMIT より前に手放す
/// (worker の `TxOutput` と同じ規律。破ると 42P05)。
struct PgRepo {
    client: Mutex<Client>,
    guc: Option<Uuid>,
}

impl PgRepo {
    fn new(client: Client, guc: Option<Uuid>) -> Arc<Self> {
        Arc::new(Self {
            client: Mutex::new(client),
            guc,
        })
    }
}

async fn begin(client: &mut Client, tenant_id: Uuid) -> Result<Transaction<'_>, DbError> {
    let tx = client.transaction().await.map_err(db_err)?;
    tx.execute(SET_TENANT, &[&tenant_id.to_string()])
        .await
        .map_err(db_err)?;
    Ok(tx)
}

#[async_trait]
impl VeinTemplatesRepository for PgRepo {
    async fn upsert(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        template: &str,
    ) -> Result<Option<DateTime<Utc>>, DbError> {
        let mut client = self.client.lock().await;
        let tx = begin(&mut client, self.guc.unwrap_or(tenant_id)).await?;
        let out = tx
            .query_opt(sql::UPSERT, &[&tenant_id, &employee_id, &template])
            .await
            .map_err(db_err)?
            .map(|r| r.get(0));
        tx.commit().await.map_err(db_err)?;
        Ok(out)
    }

    async fn list(&self, tenant_id: Uuid) -> Result<Vec<VeinTemplateRow>, DbError> {
        let mut client = self.client.lock().await;
        let tx = begin(&mut client, self.guc.unwrap_or(tenant_id)).await?;
        let out = tx
            .query(sql::LIST, &[&tenant_id])
            .await
            .map_err(db_err)?
            .into_iter()
            .map(|r| VeinTemplateRow {
                id: r.get(0),
                employee_id: r.get(1),
                name: r.get(2),
                template: r.get(3),
                updated_at: r.get(4),
            })
            .collect();
        tx.commit().await.map_err(db_err)?;
        Ok(out)
    }

    async fn registration_count(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
    ) -> Result<(i64, bool), DbError> {
        let mut client = self.client.lock().await;
        let tx = begin(&mut client, self.guc.unwrap_or(tenant_id)).await?;
        let out = {
            let row = tx
                .query_one(sql::REGISTRATION_COUNT, &[&tenant_id, &employee_id])
                .await
                .map_err(db_err)?;
            (row.get(0), row.get(1))
        };
        tx.commit().await.map_err(db_err)?;
        Ok(out)
    }

    async fn update_learned(
        &self,
        tenant_id: Uuid,
        id: Uuid,
        template: &str,
        read_updated_at: DateTime<Utc>,
    ) -> Result<bool, DbError> {
        let mut client = self.client.lock().await;
        let tx = begin(&mut client, self.guc.unwrap_or(tenant_id)).await?;
        let n = tx
            .execute(
                sql::UPDATE_LEARNED,
                &[&tenant_id, &id, &template, &read_updated_at],
            )
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(n == 1)
    }

    async fn delete(&self, tenant_id: Uuid, employee_id: Uuid) -> Result<bool, DbError> {
        let mut client = self.client.lock().await;
        let tx = begin(&mut client, self.guc.unwrap_or(tenant_id)).await?;
        let n = tx
            .execute(sql::DELETE, &[&tenant_id, &employee_id])
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(n == 1)
    }
}

fn hex(seed: u64) -> String {
    synth::hex(&synth::chara(seed))
}

/// テストごとのテナント (自分で作り、終わりに [`Ctx::cleanup`] で消す)。
struct Ctx {
    tenant_id: Uuid,
    repo: Arc<PgRepo>,
}

async fn setup(name: &str) -> Ctx {
    let mut client = connect().await;
    let tenant_id = Uuid::new_v4();
    {
        let tx = begin(&mut client, tenant_id).await.unwrap();
        // superuser / BYPASSRLS で繋ぐと RLS を素通りして、テナント分離のテストが意味を失う
        let bypass: bool = tx
            .query_one(
                "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            !bypass,
            "{URL_ENV} のロールは RLS を素通りする。alc_api_app で繋ぐこと"
        );
        tx.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2)",
            &[&tenant_id, &name],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    Ctx {
        tenant_id,
        repo: PgRepo::new(client, None),
    }
}

impl Ctx {
    /// 設定用の SQL (乗務員を作る・消す) を、このテナントのトランザクションで 1 文流す。
    async fn exec(&self, statement: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) {
        let mut client = self.repo.client.lock().await;
        let tx = begin(&mut client, self.tenant_id).await.unwrap();
        let n = tx.execute(statement, params).await.unwrap();
        assert_eq!(n, 1, "{statement}");
        tx.commit().await.unwrap();
    }

    async fn employee(&self, name: &str) -> Uuid {
        let id = Uuid::new_v4();
        self.exec(
            "INSERT INTO employees (id, tenant_id, name) VALUES ($1, $2, $3)",
            &[&id, &self.tenant_id, &name],
        )
        .await;
        id
    }

    async fn soft_delete_employee(&self, id: Uuid) {
        self.exec(
            "UPDATE employees SET deleted_at = NOW() WHERE tenant_id = $1 AND id = $2",
            &[&self.tenant_id, &id],
        )
        .await;
    }

    /// 口 (`routes::tenant_router`) をこのテナントで叩く。
    async fn send(&self, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
        let state = VeinState {
            templates: self.repo.clone(),
        };
        let app = tenant_router()
            .with_state(state)
            .layer(Extension(TenantId(self.tenant_id)));
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn put_template(&self, employee_id: Uuid, seed: u64) -> (StatusCode, Value) {
        let body = json!({ "charas": [hex(seed), hex(seed)] });
        self.send("PUT", &format!("/vein/templates/{employee_id}"), Some(body))
            .await
    }

    async fn identify(&self, seed: u64) -> Value {
        let body = json!({ "chara": hex(seed) });
        let (status, v) = self.send("POST", "/vein/identify", Some(body)).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        v
    }

    async fn list(&self) -> Value {
        let (status, v) = self.send("GET", "/vein/templates", None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        v
    }

    async fn cleanup(self) {
        let mut client = self.repo.client.lock().await;
        let tx = begin(&mut client, self.tenant_id).await.unwrap();
        for statement in [
            "DELETE FROM vein_templates WHERE tenant_id = $1",
            "DELETE FROM employees WHERE tenant_id = $1",
            "DELETE FROM tenants WHERE id = $1",
        ] {
            tx.execute(statement, &[&self.tenant_id]).await.unwrap();
        }
        tx.commit().await.unwrap();
    }
}

fn templates(list: &Value) -> &Vec<Value> {
    list["templates"].as_array().unwrap()
}

fn row_of(list: &Value, employee_id: Uuid) -> Value {
    templates(list)
        .iter()
        .find(|t| t["employee_id"] == employee_id.to_string())
        .unwrap()
        .clone()
}

/// 当たった乗務員の名前を返し、学習後のテンプレートを DB に書き戻す。
#[tokio::test]
#[ignore = "実 DB が要る (VEIN_TEST_DATABASE_URL)"]
async fn enroll_identify_writes_back_learned_template() {
    let ctx = setup("Vein Roundtrip Tenant").await;
    let yamada = ctx.employee("山田 太郎").await;
    let suzuki = ctx.employee("鈴木 花子").await;

    let (status, put) = ctx.put_template(yamada, 11).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    assert_eq!(put["employee_id"], yamada.to_string());
    assert!(put["updated_at"].is_string());
    assert_eq!(ctx.put_template(suzuki, 12).await.0, StatusCode::OK);

    let before = ctx.list().await;
    assert_eq!(before["logic_version"], "0.1.1");
    assert_eq!(templates(&before).len(), 2);
    let yamada_before = row_of(&before, yamada);

    let hit = ctx.identify(11).await;
    assert_eq!(hit, json!({ "employee_id": yamada, "name": "山田 太郎" }));

    // 学習後のテンプレートが書き戻され、updated_at が進む (鈴木さんの行は触らない)
    let after = ctx.list().await;
    let yamada_after = row_of(&after, yamada);
    assert_ne!(yamada_after["template"], yamada_before["template"]);
    assert_ne!(yamada_after["updated_at"], yamada_before["updated_at"]);
    assert_eq!(row_of(&after, suzuki), row_of(&before, suzuki));

    // 書き戻したテンプレートでも同じ指で当たる (import_temp_b64 で読み戻せる)
    let learned = yamada_after["template"].as_str().unwrap();
    let again = matcher::identify(&[learned], &synth::chara(11), 0).unwrap();
    assert_eq!(again.unreadable, Vec::<usize>::new());
    assert!(again.hit.is_some());

    // 登録していない指は外れ
    assert_eq!(ctx.identify(13).await, json!({ "employee_id": null }));
    ctx.cleanup().await;
}

/// 同じ乗務員の PUT は 1 行を上書きし、別の指に置き換わる。
#[tokio::test]
#[ignore = "実 DB が要る (VEIN_TEST_DATABASE_URL)"]
async fn put_upserts_one_row_per_employee() {
    let ctx = setup("Vein Upsert Tenant").await;
    let yamada = ctx.employee("山田").await;
    assert_eq!(ctx.put_template(yamada, 21).await.0, StatusCode::OK);
    assert_eq!(ctx.put_template(yamada, 22).await.0, StatusCode::OK);
    assert_eq!(templates(&ctx.list().await).len(), 1);
    assert_eq!(ctx.identify(22).await["employee_id"], yamada.to_string());
    assert_eq!(ctx.identify(21).await["employee_id"], Value::Null);

    // 未対応の形式は 422 で、登録は変わらない (照合の学習で変わった後の一覧と比べる)
    let list = ctx.list().await;
    let body = json!({ "charas": ["9911AABB"] });
    let (status, v) = ctx
        .send("PUT", &format!("/vein/templates/{yamada}"), Some(body))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "unsupported_chara_format");
    assert!(ctx.list().await == list, "422 の PUT で登録が変わった");
    ctx.cleanup().await;
}

/// 読んだ後に登録し直されていたら、学習の書き戻しは 0 行で捨てる。
#[tokio::test]
#[ignore = "実 DB が要る (VEIN_TEST_DATABASE_URL)"]
async fn update_learned_discards_on_conflict() {
    let ctx = setup("Vein Conflict Tenant").await;
    let yamada = ctx.employee("山田").await;
    assert_eq!(ctx.put_template(yamada, 31).await.0, StatusCode::OK);
    let read = ctx.repo.list(ctx.tenant_id).await.unwrap().remove(0);

    // 間に登録し直し (updated_at が進む)
    assert_eq!(ctx.put_template(yamada, 32).await.0, StatusCode::OK);
    let written = ctx
        .repo
        .update_learned(ctx.tenant_id, read.id, "stale", read.updated_at)
        .await
        .unwrap();
    assert!(!written);
    let now = ctx.repo.list(ctx.tenant_id).await.unwrap().remove(0);
    assert_ne!(now.template, "stale");

    // 読んだ値のままなら書ける
    let written = ctx
        .repo
        .update_learned(ctx.tenant_id, now.id, "fresh", now.updated_at)
        .await
        .unwrap();
    assert!(written);
    let fresh = ctx.repo.list(ctx.tenant_id).await.unwrap().remove(0);
    assert_eq!(fresh.template, "fresh");
    assert!(fresh.updated_at > now.updated_at);
    ctx.cleanup().await;
}

/// 別テナントからは見えず・当たらず・登録できない。削除済みの乗務員は除く。
#[tokio::test]
#[ignore = "実 DB が要る (VEIN_TEST_DATABASE_URL)"]
async fn tenant_isolation_and_deleted_employees() {
    let a = setup("Vein Tenant A").await;
    let b = setup("Vein Tenant B").await;
    let yamada = a.employee("山田").await;
    let tanaka = a.employee("田中").await;
    assert_eq!(a.put_template(yamada, 41).await.0, StatusCode::OK);
    assert_eq!(a.put_template(tanaka, 42).await.0, StatusCode::OK);

    assert_eq!(b.list().await["templates"], json!([]));
    assert_eq!(b.identify(41).await, json!({ "employee_id": null }));
    let (status, v) = b.put_template(yamada, 41).await;
    assert_eq!(
        (status, v["error"].clone()),
        (StatusCode::NOT_FOUND, json!("employee_not_found"))
    );

    // 乗務員を削除 (soft delete) すると、一覧にも照合にも出ず、登録もできない
    a.soft_delete_employee(tanaka).await;
    assert_eq!(templates(&a.list().await).len(), 1);
    assert_eq!(a.identify(42).await, json!({ "employee_id": null }));
    assert_eq!(a.put_template(tanaka, 42).await.0, StatusCode::NOT_FOUND);
    // PUT の上限判定の人数も削除済みを数えない (identify と同じ条件)
    let counted = a.repo.registration_count(a.tenant_id, yamada).await;
    assert_eq!(counted.unwrap(), (1, true));
    let counted = a.repo.registration_count(a.tenant_id, tanaka).await;
    assert_eq!(counted.unwrap(), (1, false));

    // テンプレートの削除: 204 → 2 回目は 404。別テナントからは消せない
    let path = format!("/vein/templates/{yamada}");
    assert_eq!(b.send("DELETE", &path, None).await.0, StatusCode::NOT_FOUND);
    assert_eq!(
        a.send("DELETE", &path, None).await.0,
        StatusCode::NO_CONTENT
    );
    let (status, v) = a.send("DELETE", &path, None).await;
    assert_eq!(
        (status, v["error"].clone()),
        (StatusCode::NOT_FOUND, json!("vein_template_not_found"))
    );
    assert_eq!(a.list().await["templates"], json!([]));
    a.cleanup().await;
    b.cleanup().await;
}

/// SQL の `WHERE tenant_id = $1` が正しいテナントを指していても、`app.current_tenant_id` が別テナントなら
/// RLS だけで止まる (上のテストは WHERE と RLS の両方が効いた結果しか見ていない)。
#[tokio::test]
#[ignore = "実 DB が要る (VEIN_TEST_DATABASE_URL)"]
async fn rls_alone_blocks_another_tenant() {
    let a = setup("Vein RLS Tenant A").await;
    let b = setup("Vein RLS Tenant B").await;
    let yamada = a.employee("山田").await;
    let tanaka = a.employee("田中").await;
    assert_eq!(a.put_template(yamada, 51).await.0, StatusCode::OK);
    let row = a.repo.list(a.tenant_id).await.unwrap().remove(0);

    // 引数は A、GUC は B
    let as_b = PgRepo::new(connect().await, Some(b.tenant_id));
    assert_eq!(as_b.list(a.tenant_id).await.unwrap(), vec![]);
    let counted = as_b.registration_count(a.tenant_id, yamada).await;
    assert_eq!(counted.unwrap(), (0, false));
    // employees も RLS で見えないので、登録は「乗務員が居ない」になる
    assert_eq!(as_b.upsert(a.tenant_id, tanaka, "x").await.unwrap(), None);
    let written = as_b
        .update_learned(a.tenant_id, row.id, "x", row.updated_at)
        .await;
    assert!(!written.unwrap());
    assert!(!as_b.delete(a.tenant_id, yamada).await.unwrap());
    {
        // 他テナントの行は書けない (policy の USING が INSERT の検査にもなる)
        let mut client = as_b.client.lock().await;
        let tx = begin(&mut client, b.tenant_id).await.unwrap();
        let err = tx
            .execute(
                "INSERT INTO vein_templates (tenant_id, employee_id, template) VALUES ($1, $2, 'x')",
                &[&a.tenant_id, &tanaka],
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE), "{err}");
    }
    {
        // テナントを設定しないトランザクションは読めない (前のトランザクションの値が残っていない)
        let mut client = a.repo.client.lock().await;
        let tx = client.transaction().await.unwrap();
        let read = tx.query(sql::LIST, &[&a.tenant_id]).await;
        assert!(read.is_err(), "テナント未設定で読めた");
    }

    // A の登録は元のまま
    assert_eq!(a.repo.list(a.tenant_id).await.unwrap(), vec![row]);
    a.cleanup().await;
    b.cleanup().await;
}

/// [`SET_TENANT`] が worker の `in_tenant_tx` の文と同じであること (DB は要らない)。
#[test]
fn set_tenant_statement_matches_worker() {
    let worker_repo = include_str!("../../../src/repo.rs");
    assert!(
        worker_repo.contains(&format!("\"{SET_TENANT}\"")),
        "src/repo.rs の in_tenant_tx の文が変わった。SET_TENANT を合わせること"
    );
}
