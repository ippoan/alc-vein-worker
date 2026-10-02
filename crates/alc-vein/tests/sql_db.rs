//! `repo::sql` の定数と RLS を実 DB で確かめる (Refs ippoan/rust-alc-api#721)。
//!
//! 口の分岐 (422 / 0〜2 人 / 501 人 / 書き戻しの競合) は `src/routes_tests.rs` が fake の repo で見る。
//! ここは SQL の側 — `ON CONFLICT (tenant_id, employee_id)` の upsert・`updated_at` を条件にした書き戻し・
//! 削除済みの乗務員の除外・RLS のテナント分離 — と、登録 → 照合で当たる → 学習後のテンプレートが
//! 書き戻される、の一連を固定する (backend に在った `tests/vein_templates_test.rs` の 4 本と同じ事柄)。
//!
//! repo は worker が使うものと同じ実装 ([`alc_vein::pg::PgVeinTemplates`]) — メソッド 1 回 = 1 トランザクション、
//! `BEGIN` → `alc_worker_db::SET_TENANT` → `repo::sql` の定数 (型付きの名前なしの文) → `COMMIT`。
//! 写しは持たない。接続だけ native の tokio-postgres でここが張る。準備・後始末と、RLS だけで止まることの検査は、
//! テストが自分で張った別の接続 ([`Ctx::db`] など) で流す (Refs ippoan/rust-alc-api#723)。
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

use alc_core_wasm::TenantId;
use alc_vein::matcher::{self, synth};
use alc_vein::pg::{self, PgVeinTemplates};
use alc_vein::repo::{sql, VeinTemplatesRepository};
use alc_vein::routes::tenant_router;
use alc_vein::VeinState;
use alc_worker_db::{PgClient, SET_TENANT};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Extension;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};
use tower::ServiceExt;
use uuid::Uuid;

const URL_ENV: &str = "VEIN_TEST_DATABASE_URL";

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

fn hex(seed: u64) -> String {
    synth::hex(&synth::chara(seed))
}

/// テストごとのテナント (自分で作り、終わりに [`Ctx::cleanup`] で消す)。
struct Ctx {
    tenant_id: Uuid,
    /// worker と同じ実装の repo
    repo: Arc<PgVeinTemplates>,
    /// 準備・後始末用の、テストが自分で張った接続 (repo の接続とは別)
    db: Mutex<PgClient>,
}

async fn setup(name: &str) -> Ctx {
    let mut db = PgClient::new(connect().await);
    let tenant_id = Uuid::new_v4();
    // superuser / BYPASSRLS で繋ぐと RLS を素通りして、テナント分離のテストが意味を失う
    let bypass: bool = db
        .tenant_tx(tenant_id, |tx| {
            Box::pin(async move {
                let row = tx
                    .query_typed_one(
                        "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user",
                        &[],
                    )
                    .await?;
                Ok(row.get(0))
            })
        })
        .await
        .unwrap();
    assert!(
        !bypass,
        "{URL_ENV} のロールは RLS を素通りする。alc_api_app で繋ぐこと"
    );
    let name = name.to_owned();
    db.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            tx.execute_typed(
                "INSERT INTO tenants (id, name) VALUES ($1, $2)",
                &[(&tenant_id, Type::UUID), (&name, Type::TEXT)],
            )
            .await
        })
    })
    .await
    .unwrap();
    Ctx {
        tenant_id,
        repo: Arc::new(PgVeinTemplates::new(PgClient::new(connect().await))),
        db: Mutex::new(db),
    }
}

impl Ctx {
    async fn employee(&self, name: &str) -> Uuid {
        let id = Uuid::new_v4();
        let tenant_id = self.tenant_id;
        let name = name.to_owned();
        let n = self
            .db
            .lock()
            .await
            .tenant_tx(self.tenant_id, move |tx| {
                Box::pin(async move {
                    tx.execute_typed(
                        "INSERT INTO employees (id, tenant_id, name) VALUES ($1, $2, $3)",
                        &[
                            (&id, Type::UUID),
                            (&tenant_id, Type::UUID),
                            (&name, Type::TEXT),
                        ],
                    )
                    .await
                })
            })
            .await
            .unwrap();
        assert_eq!(n, 1, "INSERT INTO employees");
        id
    }

    async fn soft_delete_employee(&self, id: Uuid) {
        let tenant_id = self.tenant_id;
        let n = self
            .db
            .lock()
            .await
            .tenant_tx(self.tenant_id, move |tx| {
                Box::pin(async move {
                    tx.execute_typed(
                        "UPDATE employees SET deleted_at = NOW() WHERE tenant_id = $1 AND id = $2",
                        &[(&tenant_id, Type::UUID), (&id, Type::UUID)],
                    )
                    .await
                })
            })
            .await
            .unwrap();
        assert_eq!(n, 1, "UPDATE employees");
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
        let tenant_id = self.tenant_id;
        self.db
            .lock()
            .await
            .tenant_tx(self.tenant_id, move |tx| {
                Box::pin(async move {
                    for statement in [
                        "DELETE FROM vein_templates WHERE tenant_id = $1",
                        "DELETE FROM employees WHERE tenant_id = $1",
                        "DELETE FROM tenants WHERE id = $1",
                    ] {
                        tx.execute_typed(statement, &[(&tenant_id, Type::UUID)])
                            .await?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
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

    // 引数は A、GUC は B (repo と同じ自由関数を、B を設定したトランザクションの中で呼ぶ)
    let mut as_b = PgClient::new(connect().await);
    let listed = as_b
        .tenant_tx(b.tenant_id, |tx| Box::pin(pg::list(tx, a.tenant_id)))
        .await;
    assert_eq!(listed.unwrap(), vec![]);
    let counted = as_b
        .tenant_tx(b.tenant_id, |tx| {
            Box::pin(pg::registration_count(tx, a.tenant_id, yamada))
        })
        .await;
    assert_eq!(counted.unwrap(), (0, false));
    // employees も RLS で見えないので、登録は「乗務員が居ない」になる
    let upserted = as_b
        .tenant_tx(b.tenant_id, |tx| {
            Box::pin(pg::upsert(tx, a.tenant_id, tanaka, "x"))
        })
        .await;
    assert_eq!(upserted.unwrap(), None);
    let written = as_b
        .tenant_tx(b.tenant_id, |tx| {
            Box::pin(pg::update_learned(
                tx,
                a.tenant_id,
                row.id,
                "x",
                row.updated_at,
            ))
        })
        .await;
    assert!(!written.unwrap());
    let deleted = as_b
        .tenant_tx(b.tenant_id, |tx| {
            Box::pin(pg::delete(tx, a.tenant_id, yamada))
        })
        .await;
    assert!(!deleted.unwrap());
    {
        // 他テナントの行は書けない (policy の USING が INSERT の検査にもなる)
        let a_id = a.tenant_id;
        let err = as_b
            .tenant_tx(b.tenant_id, move |tx| {
                Box::pin(async move {
                    tx.execute_typed(
                        "INSERT INTO vein_templates (tenant_id, employee_id, template) VALUES ($1, $2, 'x')",
                        &[(&a_id, Type::UUID), (&tanaka, Type::UUID)],
                    )
                    .await
                })
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE), "{err}");
    }
    {
        // テナントを設定しないトランザクションは読めない (前のトランザクションの値が残っていない)。
        // 未設定のトランザクションは PgClient からは作れないので、ここだけ素の接続 1 本を使う:
        // (1) A を設定して COMMIT → (2) 同じ接続の次のトランザクションで、設定せずに読む
        let mut raw = connect().await;
        let tx = raw.transaction().await.unwrap();
        tx.query_typed(SET_TENANT, &[(&a.tenant_id.to_string(), Type::TEXT)])
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let tx = raw.transaction().await.unwrap();
        let read = tx
            .query_typed(sql::LIST, &[(&a.tenant_id, Type::UUID)])
            .await;
        assert!(read.is_err(), "テナント未設定で読めた");
    }

    // A の登録は元のまま
    assert_eq!(a.repo.list(a.tenant_id).await.unwrap(), vec![row]);
    a.cleanup().await;
    b.cleanup().await;
}
