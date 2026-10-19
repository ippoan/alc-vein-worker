//! `repo::sql` の定数を、テストの process の中で起こす組み込みの PostgreSQL で確かめる
//! (Refs ippoan/rust-alc-api#721 / ippoan/rust-alc-api#727)。
//!
//! 口の分岐 (422 / 0〜2 人 / 501 人 / 書き戻しの競合) は `src/routes_tests.rs` が fake の repo で見る。
//! ここは SQL の側 — `ON CONFLICT (tenant_id, employee_id, is_dev)` の upsert・`updated_at` を条件にした書き戻し・
//! 削除済みの乗務員の除外・テナント分離 (`WHERE tenant_id` と RLS が合わさった結果) — と、登録 → 照合で当たる → 学習後のテンプレートが
//! 書き戻される、の一連を固定する (backend に在った `tests/vein_templates_test.rs` の 4 本と同じ事柄)。
//! dev端末の要求 (`X-Device-Dev: 1` → `app.device_dev`) が dev の行だけを読み書きすること (alc-migrations 179) も見る。
//! 加えて `current_user` と、`tokio_postgres::Error` → `DbError` の写し (DB のエラー / DB でない失敗) を通す。
//! RLS が実際に行を止めることは ippoan/alc-migrations の CI (`ci/check_rls_rows.sql`、全部の表を実行用ロールから実際の行で) が確かめる。ここでは確かめない。
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

use alc_core_wasm::{require_tenant_header, DbError};
use alc_vein::matcher::{self, synth};
use alc_vein::pg::PgVeinTemplates;
use alc_vein::repo::VeinTemplatesRepository;
use alc_vein::routes::{tenant_router, with_device_dev_slot};
use alc_vein::VeinState;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware;
use embedded::{employee, soft_delete_employee, tenant, Embedded, APP_ROLE};
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

fn hex(seed: u64) -> String {
    synth::hex(&synth::chara(seed))
}

/// 口 (`routes::tenant_router`) を `tenant_id` で叩く (本番の端末の要求)。
async fn send(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    send_as(repo, tenant_id, false, method, uri, body).await
}

/// worker (直下の `src/lib.rs`) と同じ層の順で叩く。`dev` = dev端末の要求 (`X-Device-Dev: 1`)。
async fn send_as(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    dev: bool,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let state = VeinState {
        templates: repo.clone(),
    };
    let app = tenant_router()
        .layer(middleware::from_fn(require_tenant_header))
        .layer(middleware::from_fn(with_device_dev_slot))
        .with_state(state);
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("X-Tenant-ID", tenant_id.to_string());
    if dev {
        req = req.header("X-Device-Dev", "1");
    }
    let req = req
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
    let read = r.list(t, false).await.unwrap().remove(0);

    // 間に登録し直し (updated_at が進む)
    assert_eq!(put_template(r, t, yamada, 32).await.0, StatusCode::OK);
    let written = r
        .update_learned(t, false, read.id, "stale", read.updated_at)
        .await
        .unwrap();
    assert!(!written);
    let now = r.list(t, false).await.unwrap().remove(0);
    assert_ne!(now.template, "stale");

    // 読んだ値のままなら書ける
    let written = r
        .update_learned(t, false, now.id, "fresh", now.updated_at)
        .await
        .unwrap();
    assert!(written);
    let fresh = r.list(t, false).await.unwrap().remove(0);
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
    let counted = r.registration_count(a, false, yamada).await;
    assert_eq!(counted.unwrap(), (1, true));
    let counted = r.registration_count(a, false, tanaka).await;
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

/// dev端末 (`X-Device-Dev: 1`) の登録は dev の行 (`is_dev = true`) になり、dev の要求からだけ見える
/// (本番の一覧・照合・人数の数えに出ない)。
#[tokio::test(flavor = "multi_thread")]
async fn device_dev_put_is_seen_only_by_dev() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let t = tenant(&mut prep.inner, "Vein Dev Tenant").await;
    let yamada = employee(&mut prep.inner, t, "山田").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;

    let body = json!({ "charas": [hex(51), hex(51)] });
    let uri = format!("/vein/templates/{yamada}");
    let (status, v) = send_as(r, t, true, "PUT", &uri, Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{v}");

    let (_, dev_list) = send_as(r, t, true, "GET", "/vein/templates", None).await;
    assert_eq!(templates(&dev_list).len(), 1);
    assert_eq!(templates(&dev_list)[0]["employee_id"], yamada.to_string());
    assert_eq!(list(r, t).await["templates"], json!([]));
    assert_eq!(identify(r, t, 51).await, json!({ "employee_id": null }));
    assert_eq!(
        r.registration_count(t, false, yamada).await.unwrap(),
        (0, false)
    );
    assert_eq!(
        r.registration_count(t, true, yamada).await.unwrap(),
        (1, true)
    );
    repo.close().await;

    // 書いた行は is_dev = true (列の既定値が app.device_dev を見る)
    let su = db.superuser().await;
    let rows = su
        .inner
        .query_typed("SELECT is_dev FROM vein_templates", &[])
        .await
        .unwrap();
    let flags: Vec<bool> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(flags, vec![true]);
    su.close().await;
    db.shutdown();
}

/// dev の照合は dev の行 (乗務員 A) だけを相手にし、学習の書き戻し・削除も dev の行にだけ入る。
/// 本番で登録した行 (乗務員 B) は dev から見えず、変わらない (同じ乗務員に両方の行を持つ形は、
/// 2 列の一意を外す migration の側で確かめる)。
#[tokio::test(flavor = "multi_thread")]
async fn device_dev_identify_writes_back_only_the_dev_row() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let t = tenant(&mut prep.inner, "Vein Dev Learn Tenant").await;
    let a = employee(&mut prep.inner, t, "開発 A").await;
    let b = employee(&mut prep.inner, t, "本番 B").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;

    let body = json!({ "charas": [hex(61), hex(61)] });
    let uri_a = format!("/vein/templates/{a}");
    let (status, v) = send_as(r, t, true, "PUT", &uri_a, Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(put_template(r, t, b, 62).await.0, StatusCode::OK);
    let prod_before = r.list(t, false).await.unwrap();
    let dev_before = r.list(t, true).await.unwrap();
    assert_eq!(prod_before.len(), 1);
    assert_eq!(prod_before[0].employee_id, b);
    assert_eq!(dev_before.len(), 1);
    assert_eq!(dev_before[0].employee_id, a);

    // dev の照合は本番の B の指に当たらず、dev の A の指に当たる
    let body = json!({ "chara": hex(62) });
    let (_, miss) = send_as(r, t, true, "POST", "/vein/identify", Some(body)).await;
    assert_eq!(miss, json!({ "employee_id": null }));
    let body = json!({ "chara": hex(61) });
    let (status, hit) = send_as(r, t, true, "POST", "/vein/identify", Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{hit}");
    assert_eq!(hit, json!({ "employee_id": a, "name": "開発 A" }));

    // dev の行は学習後のテンプレートに変わり、本番の行はそのまま
    let dev_after = r.list(t, true).await.unwrap();
    assert_ne!(dev_after[0].template, dev_before[0].template);
    assert!(dev_after[0].updated_at > dev_before[0].updated_at);
    assert_eq!(r.list(t, false).await.unwrap(), prod_before);

    // 本番の要求から dev の行は書き戻せない (0 行)
    let read = &dev_after[0];
    let written = r.update_learned(t, false, read.id, "prod", read.updated_at);
    assert!(!written.await.unwrap());
    assert_eq!(r.list(t, true).await.unwrap(), dev_after);

    // dev から本番の B は消せず、dev の A は消せる
    let uri_b = format!("/vein/templates/{b}");
    let (status, _) = send_as(r, t, true, "DELETE", &uri_b, None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send_as(r, t, true, "DELETE", &uri_a, None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(r.list(t, true).await.unwrap(), vec![]);
    assert_eq!(r.list(t, false).await.unwrap(), prod_before);
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
    let err = r.upsert(t, false, yamada, "a\0b").await.unwrap_err();
    assert!(
        matches!(&err, DbError::Other(m) if m.ends_with("(22021)")),
        "{err:?}"
    );
    // 同じ接続で次のメソッドが通る (ROLLBACK 済み)
    assert_eq!(r.list(t, false).await.unwrap(), vec![]);
    assert!(r.upsert(t, false, yamada, "ok").await.unwrap().is_some());
    let row = r.list(t, false).await.unwrap().remove(0);
    let err = r
        .update_learned(t, false, row.id, "a\0b", row.updated_at)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DbError::Other(m) if m.ends_with("(22021)")),
        "{err:?}"
    );
    assert_eq!(r.list(t, false).await.unwrap(), vec![row]);
    repo.close().await;

    // 表の権限を持たないロールで繋いだ repo は、5 メソッドとも 42501
    let repo = db.repo("anon").await;
    let r = &repo.inner;
    assert_eq!(r.current_user().await.as_deref(), Some("anon"));
    let denied = |e: DbError| matches!(&e, DbError::Other(m) if m.ends_with("(42501)"));
    assert!(denied(r.list(t, false).await.unwrap_err()));
    assert!(denied(
        r.registration_count(t, false, yamada).await.unwrap_err()
    ));
    assert!(denied(r.upsert(t, false, yamada, "x").await.unwrap_err()));
    assert!(denied(
        r.update_learned(t, false, Uuid::new_v4(), "x", chrono::Utc::now())
            .await
            .unwrap_err()
    ));
    assert!(denied(r.delete(t, false, yamada).await.unwrap_err()));
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
    let err = repo.list(t, false).await.unwrap_err();
    assert!(
        matches!(&err, DbError::Other(m) if m == "connection closed"),
        "{err:?}"
    );
    assert!(matches!(
        repo.upsert(t, false, t, "x").await,
        Err(DbError::Other(_))
    ));
    assert!(matches!(
        repo.registration_count(t, false, t).await,
        Err(DbError::Other(_))
    ));
    assert!(matches!(
        repo.update_learned(t, false, t, "x", chrono::Utc::now())
            .await,
        Err(DbError::Other(_))
    ));
    assert!(matches!(
        repo.delete(t, false, t).await,
        Err(DbError::Other(_))
    ));
    assert_eq!(repo.current_user().await, None);
    drop(repo);
    db.shutdown();
}
