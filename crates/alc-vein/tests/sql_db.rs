//! `repo::sql` の定数と RLS を、テストの process の中で起こす組み込みの PostgreSQL で確かめる
//! (Refs ippoan/rust-alc-api#721 / ippoan/rust-alc-api#727)。
//!
//! 口の分岐 (422 / 0〜2 人 / 501 人 / 書き戻しの競合) は `src/routes_tests.rs` が fake の repo で見る。
//! ここは SQL の側 — `ON CONFLICT (tenant_id, employee_id)` の upsert・`updated_at` を条件にした書き戻し・
//! 削除済みの乗務員の除外・RLS のテナント分離 — と、登録 → 照合で当たる → 学習後のテンプレートが
//! 書き戻される、の一連を固定する (backend に在った `tests/vein_templates_test.rs` の 4 本と同じ事柄)。
//! 加えて `current_user` と、`tokio_postgres::Error` → `DbError` の写し (DB のエラー / DB でない失敗) を通す。
//!
//! repo は worker が使うものと同じ実装 ([`alc_vein::pg::PgVeinTemplates`]) — メソッド 1 回 = 1 トランザクション、
//! `BEGIN` → `alc_worker_db::SET_TENANT` → `repo::sql` の定数 (型付きの名前なしの文) → `COMMIT`。
//! 写しは持たない。接続だけ native の tokio-postgres でここが張る (Refs ippoan/rust-alc-api#723)。
//!
//! DB は `pglite-oxide` (PostgreSQL 17.5) をテストごとに 1 つ起こす (`embedded/mod.rs`)。**docker も外の DB も
//! env も要らず**、`#[ignore]` でもない (`cargo test -p alc-vein` と `cargo llvm-cov` に入る)。回し方:
//!
//! ```bash
//! bash scripts/fetch-migrations.sh && cargo test -p alc-vein --test sql_db
//! ```
//!
//! - **migration が未取得 (`container/.alc-migrations` が無い) なら失敗する** (skip して緑にしない)
//! - ロールは RLS が効く `alc_api_app` (superuser / BYPASSRLS / 表の所有者なら、準備の `Embedded::start` が落とす)
//! - **同時に張れる接続は 1 本。** 準備の接続と repo の接続を**順に**張り替える (閉じ切ってから次を張る)
//! - 直結で流す。PgBouncer (transaction mode) 越しの確かめは `tests/run-local.sh` が持つ
//!
//! 特徴量は `alc_vein::matcher::synth` の合成 (0xBDBD 構造体)。

mod embedded;

use std::sync::Arc;

use alc_core_wasm::{DbError, TenantId};
use alc_vein::matcher::{self, synth};
use alc_vein::pg::{self, PgVeinTemplates};
use alc_vein::repo::{sql, VeinTemplatesRepository};
use alc_vein::routes::tenant_router;
use alc_vein::VeinState;
use alc_worker_db::SET_TENANT;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Extension;
use embedded::{employee, soft_delete_employee, tenant, Embedded, APP_ROLE};
use serde_json::{json, Value};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::Type;
use tower::ServiceExt;
use uuid::Uuid;

fn hex(seed: u64) -> String {
    synth::hex(&synth::chara(seed))
}

/// 口 (`routes::tenant_router`) を `tenant_id` で叩く。
async fn send(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let state = VeinState {
        templates: repo.clone(),
    };
    let app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(tenant_id)));
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

async fn put_template(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    employee_id: Uuid,
    seed: u64,
) -> (StatusCode, Value) {
    let body = json!({ "charas": [hex(seed), hex(seed)] });
    let uri = format!("/vein/templates/{employee_id}");
    send(repo, tenant_id, "PUT", &uri, Some(body)).await
}

async fn identify(repo: &Arc<PgVeinTemplates>, tenant_id: Uuid, seed: u64) -> Value {
    let body = json!({ "chara": hex(seed) });
    let (status, v) = send(repo, tenant_id, "POST", "/vein/identify", Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    v
}

async fn list(repo: &Arc<PgVeinTemplates>, tenant_id: Uuid) -> Value {
    let (status, v) = send(repo, tenant_id, "GET", "/vein/templates", None).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    v
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
#[tokio::test(flavor = "multi_thread")]
async fn enroll_identify_writes_back_learned_template() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let t = tenant(&mut prep.inner, "Vein Roundtrip Tenant").await;
    let yamada = employee(&mut prep.inner, t, "山田 太郎").await;
    let suzuki = employee(&mut prep.inner, t, "鈴木 花子").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;

    let (status, put) = put_template(r, t, yamada, 11).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    assert_eq!(put["employee_id"], yamada.to_string());
    assert!(put["updated_at"].is_string());
    assert_eq!(put_template(r, t, suzuki, 12).await.0, StatusCode::OK);

    let before = list(r, t).await;
    assert_eq!(before["logic_version"], "0.1.1");
    assert_eq!(templates(&before).len(), 2);
    let yamada_before = row_of(&before, yamada);

    let hit = identify(r, t, 11).await;
    assert_eq!(hit, json!({ "employee_id": yamada, "name": "山田 太郎" }));

    // 学習後のテンプレートが書き戻され、updated_at が進む (鈴木さんの行は触らない)
    let after = list(r, t).await;
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
    assert_eq!(identify(r, t, 13).await, json!({ "employee_id": null }));
    repo.close().await;
    db.shutdown();
}

/// 同じ乗務員の PUT は 1 行を上書きし、別の指に置き換わる。
#[tokio::test(flavor = "multi_thread")]
async fn put_upserts_one_row_per_employee() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let t = tenant(&mut prep.inner, "Vein Upsert Tenant").await;
    let yamada = employee(&mut prep.inner, t, "山田").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;

    assert_eq!(put_template(r, t, yamada, 21).await.0, StatusCode::OK);
    assert_eq!(put_template(r, t, yamada, 22).await.0, StatusCode::OK);
    assert_eq!(templates(&list(r, t).await).len(), 1);
    assert_eq!(identify(r, t, 22).await["employee_id"], yamada.to_string());
    assert_eq!(identify(r, t, 21).await["employee_id"], Value::Null);

    // 未対応の形式は 422 で、登録は変わらない (照合の学習で変わった後の一覧と比べる)
    let listed = list(r, t).await;
    let body = json!({ "charas": ["9911AABB"] });
    let uri = format!("/vein/templates/{yamada}");
    let (status, v) = send(r, t, "PUT", &uri, Some(body)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(v["error"], "unsupported_chara_format");
    assert!(list(r, t).await == listed, "422 の PUT で登録が変わった");
    repo.close().await;
    db.shutdown();
}

/// 読んだ後に登録し直されていたら、学習の書き戻しは 0 行で捨てる。
#[tokio::test(flavor = "multi_thread")]
async fn update_learned_discards_on_conflict() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let t = tenant(&mut prep.inner, "Vein Conflict Tenant").await;
    let yamada = employee(&mut prep.inner, t, "山田").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;

    assert_eq!(put_template(r, t, yamada, 31).await.0, StatusCode::OK);
    let read = r.list(t).await.unwrap().remove(0);

    // 間に登録し直し (updated_at が進む)
    assert_eq!(put_template(r, t, yamada, 32).await.0, StatusCode::OK);
    let written = r
        .update_learned(t, read.id, "stale", read.updated_at)
        .await
        .unwrap();
    assert!(!written);
    let now = r.list(t).await.unwrap().remove(0);
    assert_ne!(now.template, "stale");

    // 読んだ値のままなら書ける
    let written = r
        .update_learned(t, now.id, "fresh", now.updated_at)
        .await
        .unwrap();
    assert!(written);
    let fresh = r.list(t).await.unwrap().remove(0);
    assert_eq!(fresh.template, "fresh");
    assert!(fresh.updated_at > now.updated_at);
    repo.close().await;
    db.shutdown();
}

/// 別テナントからは見えず・当たらず・登録できない。削除済みの乗務員は除く。
#[tokio::test(flavor = "multi_thread")]
async fn tenant_isolation_and_deleted_employees() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let a = tenant(&mut prep.inner, "Vein Tenant A").await;
    let b = tenant(&mut prep.inner, "Vein Tenant B").await;
    let yamada = employee(&mut prep.inner, a, "山田").await;
    let tanaka = employee(&mut prep.inner, a, "田中").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;
    assert_eq!(put_template(r, a, yamada, 41).await.0, StatusCode::OK);
    assert_eq!(put_template(r, a, tanaka, 42).await.0, StatusCode::OK);

    assert_eq!(list(r, b).await["templates"], json!([]));
    assert_eq!(identify(r, b, 41).await, json!({ "employee_id": null }));
    let (status, v) = put_template(r, b, yamada, 41).await;
    assert_eq!(
        (status, v["error"].clone()),
        (StatusCode::NOT_FOUND, json!("employee_not_found"))
    );

    // 乗務員を削除 (soft delete) すると、一覧にも照合にも出ず、登録もできない
    // (準備の接続は repo の接続と同時に持てないので、張り替える)
    repo.close().await;
    let mut prep = db.client(APP_ROLE).await;
    soft_delete_employee(&mut prep.inner, a, tanaka).await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;
    assert_eq!(templates(&list(r, a).await).len(), 1);
    assert_eq!(identify(r, a, 42).await, json!({ "employee_id": null }));
    assert_eq!(
        put_template(r, a, tanaka, 42).await.0,
        StatusCode::NOT_FOUND
    );
    // PUT の上限判定の人数も削除済みを数えない (identify と同じ条件)
    let counted = r.registration_count(a, yamada).await;
    assert_eq!(counted.unwrap(), (1, true));
    let counted = r.registration_count(a, tanaka).await;
    assert_eq!(counted.unwrap(), (1, false));

    // テンプレートの削除: 204 → 2 回目は 404。別テナントからは消せない
    let path = format!("/vein/templates/{yamada}");
    assert_eq!(
        send(r, b, "DELETE", &path, None).await.0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(r, a, "DELETE", &path, None).await.0,
        StatusCode::NO_CONTENT
    );
    let (status, v) = send(r, a, "DELETE", &path, None).await;
    assert_eq!(
        (status, v["error"].clone()),
        (StatusCode::NOT_FOUND, json!("vein_template_not_found"))
    );
    assert_eq!(list(r, a).await["templates"], json!([]));
    repo.close().await;
    db.shutdown();
}

/// SQL の `WHERE tenant_id = $1` が正しいテナントを指していても、`app.current_tenant_id` が別テナントなら
/// RLS だけで止まる (上のテストは WHERE と RLS の両方が効いた結果しか見ていない)。
/// ippoan/alc-migrations に全表の行の検査が入ったら外す候補 (Refs ippoan/rust-alc-api#727)。
#[tokio::test(flavor = "multi_thread")]
async fn rls_alone_blocks_another_tenant() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let a = tenant(&mut prep.inner, "Vein RLS Tenant A").await;
    let b = tenant(&mut prep.inner, "Vein RLS Tenant B").await;
    let yamada = employee(&mut prep.inner, a, "山田").await;
    let tanaka = employee(&mut prep.inner, a, "田中").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    assert_eq!(
        put_template(&repo.inner, a, yamada, 51).await.0,
        StatusCode::OK
    );
    let row = repo.inner.list(a).await.unwrap().remove(0);
    repo.close().await;

    // 引数は A、GUC は B (repo と同じ自由関数を、B を設定したトランザクションの中で呼ぶ)
    let mut as_b = db.client(APP_ROLE).await;
    let listed = as_b
        .inner
        .tenant_tx(b, |tx| Box::pin(pg::list(tx, a)))
        .await;
    assert_eq!(listed.unwrap(), vec![]);
    let counted = as_b
        .inner
        .tenant_tx(b, |tx| Box::pin(pg::registration_count(tx, a, yamada)))
        .await;
    assert_eq!(counted.unwrap(), (0, false));
    // employees も RLS で見えないので、登録は「乗務員が居ない」になる
    let upserted = as_b
        .inner
        .tenant_tx(b, |tx| Box::pin(pg::upsert(tx, a, tanaka, "x")))
        .await;
    assert_eq!(upserted.unwrap(), None);
    let written = as_b
        .inner
        .tenant_tx(b, |tx| {
            Box::pin(pg::update_learned(tx, a, row.id, "x", row.updated_at))
        })
        .await;
    assert!(!written.unwrap());
    let deleted = as_b
        .inner
        .tenant_tx(b, |tx| Box::pin(pg::delete(tx, a, yamada)))
        .await;
    assert!(!deleted.unwrap());
    {
        // 他テナントの行は書けない (policy の USING が INSERT の検査にもなる)
        let err = as_b
            .inner
            .tenant_tx(b, move |tx| {
                Box::pin(async move {
                    tx.execute_typed(
                        "INSERT INTO vein_templates (tenant_id, employee_id, template) VALUES ($1, $2, 'x')",
                        &[(&a, Type::UUID), (&tanaka, Type::UUID)],
                    )
                    .await
                })
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE), "{err}");
    }
    as_b.close().await;
    {
        // テナントを設定しないトランザクションは読めない (前のトランザクションの値が残っていない)。
        // 未設定のトランザクションは PgClient からは作れないので、ここだけ素の接続 1 本を使う:
        // (1) A を設定して COMMIT → (2) 同じ接続の次のトランザクションで、設定せずに読む
        let mut raw = db.raw(APP_ROLE).await;
        let tx = raw.inner.transaction().await.unwrap();
        tx.query_typed(SET_TENANT, &[(&a.to_string(), Type::TEXT)])
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let tx = raw.inner.transaction().await.unwrap();
        let read = tx.query_typed(sql::LIST, &[(&a, Type::UUID)]).await;
        // 表が見つからない (42P01) で落ちたのではなく、テナントの GUC が空で落ちたこと
        let err = read.expect_err("テナント未設定で読めた");
        assert_eq!(
            err.code(),
            Some(&SqlState::INVALID_TEXT_REPRESENTATION),
            "{err}"
        );
        drop(tx);
        raw.close().await;
    }

    // A の登録は元のまま
    let repo = db.repo(APP_ROLE).await;
    assert_eq!(repo.inner.list(a).await.unwrap(), vec![row]);
    repo.close().await;
    db.shutdown();
}

/// `current_user` は繋いだロール (表の所有者でない方)。
#[tokio::test(flavor = "multi_thread")]
async fn current_user_is_the_app_role() {
    let db = Embedded::start().await;
    let repo = db.repo(APP_ROLE).await;
    assert_eq!(repo.inner.current_user().await.as_deref(), Some(APP_ROLE));
    repo.close().await;
    db.shutdown();
}

/// DB のエラーは `DbError::Other` (DB の message と SQLSTATE)。失敗した transaction は残らない。
#[tokio::test(flavor = "multi_thread")]
async fn db_error_becomes_other_with_sqlstate() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let t = tenant(&mut prep.inner, "Vein DbError Tenant").await;
    let yamada = employee(&mut prep.inner, t, "山田").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;

    // NUL バイト入りの template は DB が弾く (22021 character_not_in_repertoire)
    let err = r.upsert(t, yamada, "a\0b").await.unwrap_err();
    assert!(
        matches!(&err, DbError::Other(m) if m.ends_with("(22021)")),
        "{err:?}"
    );
    // 同じ接続で次のメソッドが通る (ROLLBACK 済み)
    assert_eq!(r.list(t).await.unwrap(), vec![]);
    assert!(r.upsert(t, yamada, "ok").await.unwrap().is_some());
    let row = r.list(t).await.unwrap().remove(0);
    let err = r
        .update_learned(t, row.id, "a\0b", row.updated_at)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DbError::Other(m) if m.ends_with("(22021)")),
        "{err:?}"
    );
    assert_eq!(r.list(t).await.unwrap(), vec![row]);
    repo.close().await;

    // 表の権限を持たないロールで繋いだ repo は、5 メソッドとも 42501
    let repo = db.repo("anon").await;
    let r = &repo.inner;
    assert_eq!(r.current_user().await.as_deref(), Some("anon"));
    let denied = |e: DbError| matches!(&e, DbError::Other(m) if m.ends_with("(42501)"));
    assert!(denied(r.list(t).await.unwrap_err()));
    assert!(denied(r.registration_count(t, yamada).await.unwrap_err()));
    assert!(denied(r.upsert(t, yamada, "x").await.unwrap_err()));
    assert!(denied(
        r.update_learned(t, Uuid::new_v4(), "x", chrono::Utc::now())
            .await
            .unwrap_err()
    ));
    assert!(denied(r.delete(t, yamada).await.unwrap_err()));
    repo.close().await;
    db.shutdown();
}

/// 接続が切れているとき (DB のエラーでない失敗) は `DbError::Other`、`current_user` は `None`。
#[tokio::test(flavor = "multi_thread")]
async fn closed_connection_becomes_other() {
    let db = Embedded::start().await;
    let repo = db.repo(APP_ROLE).await;
    assert_eq!(repo.inner.current_user().await.as_deref(), Some(APP_ROLE));
    let repo = repo.sever().await;
    let t = Uuid::new_v4();
    let err = repo.list(t).await.unwrap_err();
    assert!(
        matches!(&err, DbError::Other(m) if m == "connection closed"),
        "{err:?}"
    );
    assert!(matches!(
        repo.upsert(t, t, "x").await,
        Err(DbError::Other(_))
    ));
    assert!(matches!(
        repo.registration_count(t, t).await,
        Err(DbError::Other(_))
    ));
    assert!(matches!(
        repo.update_learned(t, t, "x", chrono::Utc::now()).await,
        Err(DbError::Other(_))
    ));
    assert!(matches!(repo.delete(t, t).await, Err(DbError::Other(_))));
    assert_eq!(repo.current_user().await, None);
    drop(repo);
    db.shutdown();
}
