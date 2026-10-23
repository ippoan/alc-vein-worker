//! `repo::sql` の定数を、テストの process の中で起こす組み込みの PostgreSQL で確かめる
//! (Refs ippoan/rust-alc-api#721 / ippoan/rust-alc-api#727)。
//!
//! 口の分岐 (422 / 0〜2 人 / 501 人 / 書き戻しの競合) は `src/routes_tests.rs` が fake の repo で見る。
//! ここは SQL の側 — `ON CONFLICT (tenant_id, employee_id)` の upsert・`updated_at` を条件にした書き戻し・
//! 削除済みの乗務員の除外・テナント分離 (`WHERE tenant_id` と RLS が合わさった結果) — と、登録 → 照合で当たる → 学習後のテンプレートが
//! 書き戻される、の一連を固定する (backend に在った `tests/vein_templates_test.rs` の 4 本と同じ事柄)。
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

use alc_core_wasm::{AuthUser, DbError, TenantId};
use alc_vein::matcher::{self, synth};
use alc_vein::pg::PgVeinTemplates;
use alc_vein::repo::{VeinTemplatesRepository, FORMAT_CHARA, FORMAT_MODULE};
use alc_vein::routes::tenant_router;
use alc_vein::VeinState;
use alc_worker_db::PgClient;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Extension;
use embedded::{employee, set_roles, soft_delete_employee, tenant, Embedded, APP_ROLE};
use serde_json::{json, Value};
use tokio_postgres::types::Type;
use tower::ServiceExt;
use uuid::Uuid;

fn hex(seed: u64) -> String {
    synth::hex(&synth::chara(seed))
}

/// 口 (`routes::tenant_router`) を `tenant_id` で叩く (端末の鍵 = `AuthUser` 無し)。
async fn send(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    send_as(repo, tenant_id, None, method, uri, body).await
}

/// `send` に、追加のリクエストヘッダ (`X-Device-Id`) を付けたもの。
async fn send_with_device(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    device: &str,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let state = VeinState {
        templates: repo.clone(),
    };
    let app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(tenant_id)));
    let req = Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/json")
        .header("x-device-id", device)
        .body(Body::from(body.to_string()))
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

/// `role` が `Some` なら、tenant_header が復元する `AuthUser` を付けて叩く (user_id は `admin_id()`)。
async fn send_as(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    role: Option<&str>,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let state = VeinState {
        templates: repo.clone(),
    };
    let mut app = tenant_router()
        .with_state(state)
        .layer(Extension(TenantId(tenant_id)));
    if let Some(role) = role {
        app = app.layer(Extension(AuthUser {
            user_id: admin_id(),
            email: "admin@example.com".to_string(),
            name: String::new(),
            tenant_id,
            tenant_slug: None,
            role: role.to_string(),
        }));
    }
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

/// 承認する管理者の user_id。
fn admin_id() -> Uuid {
    Uuid::from_u128(0xAD)
}

/// 登録して管理者が承認する (照合・一覧に載る)。PUT の結果を返す。
async fn put_approved(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    employee_id: Uuid,
    seed: u64,
) -> (StatusCode, Value) {
    let (status, put) = put_template(repo, tenant_id, employee_id, seed).await;
    if status == StatusCode::OK {
        let (approved, _) = approve(repo, tenant_id, employee_id, &put["updated_at"]).await;
        assert_eq!(approved, StatusCode::NO_CONTENT, "{put}");
    }
    (status, put)
}

/// 管理者として承認する。
async fn approve(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    employee_id: Uuid,
    updated_at: &Value,
) -> (StatusCode, Value) {
    let uri = format!("/vein/templates/{employee_id}/approve");
    let body = json!({ "updated_at": updated_at });
    send_as(repo, tenant_id, Some("admin"), "POST", &uri, Some(body)).await
}

/// 管理者として承認待ちの一覧を読む。
async fn pending(repo: &Arc<PgVeinTemplates>, tenant_id: Uuid) -> Value {
    let uri = "/vein/templates/pending";
    let (status, v) = send_as(repo, tenant_id, Some("admin"), "GET", uri, None).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    v
}

/// 管理者として承認済みの一覧を読む。
async fn approved(repo: &Arc<PgVeinTemplates>, tenant_id: Uuid) -> Value {
    let uri = "/vein/templates/approved";
    let (status, v) = send_as(repo, tenant_id, Some("admin"), "GET", uri, None).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    v
}

/// 管理者として登録を消す (DELETE は管理者だけ)。
async fn delete_as_admin(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    employee_id: Uuid,
) -> (StatusCode, Value) {
    let uri = format!("/vein/templates/{employee_id}");
    send_as(repo, tenant_id, Some("admin"), "DELETE", &uri, None).await
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

    let (status, put) = put_approved(r, t, yamada, 11).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    assert_eq!(put["employee_id"], yamada.to_string());
    assert!(put["updated_at"].is_string());
    assert_eq!(put_approved(r, t, suzuki, 12).await.0, StatusCode::OK);

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

    assert_eq!(put_approved(r, t, yamada, 21).await.0, StatusCode::OK);
    assert_eq!(put_approved(r, t, yamada, 22).await.0, StatusCode::OK);
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

    assert_eq!(put_approved(r, t, yamada, 31).await.0, StatusCode::OK);
    let read = r.list(t, FORMAT_CHARA).await.unwrap().remove(0);

    // 間に登録し直し (updated_at が進む)
    assert_eq!(put_approved(r, t, yamada, 32).await.0, StatusCode::OK);
    let written = r
        .update_learned(t, read.id, "stale", read.updated_at)
        .await
        .unwrap();
    assert!(!written);
    let now = r.list(t, FORMAT_CHARA).await.unwrap().remove(0);
    assert_ne!(now.template, "stale");

    // 読んだ値のままなら書ける
    let written = r
        .update_learned(t, now.id, "fresh", now.updated_at)
        .await
        .unwrap();
    assert!(written);
    let fresh = r.list(t, FORMAT_CHARA).await.unwrap().remove(0);
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
    assert_eq!(put_approved(r, a, yamada, 41).await.0, StatusCode::OK);
    assert_eq!(put_approved(r, a, tanaka, 42).await.0, StatusCode::OK);

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
    let counted = r.registration_count(a, yamada, FORMAT_CHARA).await;
    assert_eq!(counted.unwrap(), (1, true));
    let counted = r.registration_count(a, tanaka, FORMAT_CHARA).await;
    assert_eq!(counted.unwrap(), (1, false));

    // テンプレートの削除: 204 → 2 回目は 404。別テナントからは消せない
    // (別テナントへは管理者でも 404。403 で先に止まると RLS を見なくなるので管理者を付ける)
    assert_eq!(delete_as_admin(r, b, yamada).await.0, StatusCode::NOT_FOUND);
    assert_eq!(
        delete_as_admin(r, a, yamada).await.0,
        StatusCode::NO_CONTENT
    );
    let (status, v) = delete_as_admin(r, a, yamada).await;
    assert_eq!(
        (status, v["error"].clone()),
        (StatusCode::NOT_FOUND, json!("vein_template_not_found"))
    );
    assert_eq!(list(r, a).await["templates"], json!([]));
    repo.close().await;
    db.shutdown();
}

/// 承認済みの一覧は承認済みだけを (テンプレートなしで) 返し、別テナント・削除済みの乗務員は載せない。
/// 管理者以外は 403 で、DELETE も管理者だけ (承認済みの削除で一覧から消える)。
#[tokio::test(flavor = "multi_thread")]
async fn approved_list_and_admin_only_delete() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let a = tenant(&mut prep.inner, "Vein Approved Tenant A").await;
    let b = tenant(&mut prep.inner, "Vein Approved Tenant B").await;
    let yamada = employee(&mut prep.inner, a, "山田").await;
    let tanaka = employee(&mut prep.inner, a, "田中").await;
    let sato = employee(&mut prep.inner, a, "佐藤").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;

    // 承認済み 2 人 (山田・田中) と承認待ち 1 人 (佐藤)
    let (_, yamada_put) = put_approved(r, a, yamada, 61).await;
    assert_eq!(put_approved(r, a, tanaka, 62).await.0, StatusCode::OK);
    assert_eq!(put_template(r, a, sato, 63).await.0, StatusCode::OK);

    // 承認済みだけが employee_id と updated_at の 2 つで出る。承認待ちは pending のまま
    let listed = approved(r, a).await;
    assert_eq!(listed.as_array().unwrap().len(), 2);
    assert_eq!(listed[0]["employee_id"], yamada.to_string());
    assert_eq!(listed[0]["updated_at"], yamada_put["updated_at"]);
    assert_eq!(listed[0].as_object().unwrap().len(), 2);
    assert_eq!(pending(r, a).await[0]["employee_id"], sato.to_string());
    assert_eq!(approved(r, b).await, json!([]));

    // 管理者以外 (AuthUser 無し・admin 以外) は一覧も DELETE も 403 で、何も消えない
    let path = format!("/vein/templates/{yamada}");
    for role in [None, Some("user")] {
        let uri = "/vein/templates/approved";
        assert_eq!(
            send_as(r, a, role, "GET", uri, None).await.0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            send_as(r, a, role, "DELETE", &path, None).await.0,
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(approved(r, a).await.as_array().unwrap().len(), 2);

    // 乗務員の削除 (soft delete) で一覧から消え、承認済みの DELETE でも消える
    repo.close().await;
    let mut prep = db.client(APP_ROLE).await;
    soft_delete_employee(&mut prep.inner, a, tanaka).await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;
    assert_eq!(approved(r, a).await.as_array().unwrap().len(), 1);
    assert_eq!(
        delete_as_admin(r, a, yamada).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(approved(r, a).await, json!([]));
    repo.close().await;
    db.shutdown();
}

/// 登録は承認待ちで、照合にも一覧にも載らない。管理者が `updated_at` を固定して承認すると載る。
/// 登録し直すと承認待ちに戻り、別テナントからは承認できず、管理者以外は 403。
#[tokio::test(flavor = "multi_thread")]
async fn registration_is_pending_until_an_admin_approves() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let a = tenant(&mut prep.inner, "Vein Approval Tenant A").await;
    let b = tenant(&mut prep.inner, "Vein Approval Tenant B").await;
    let yamada = employee(&mut prep.inner, a, "山田").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;

    // 登録 (端末の鍵) は承認待ち。照合にも一覧にも出ず、承認待ちの一覧に出る (テンプレートは返さない)
    let (status, put) = put_template(r, a, yamada, 51).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    assert_eq!(identify(r, a, 51).await, json!({ "employee_id": null }));
    assert_eq!(list(r, a).await["templates"], json!([]));
    let waiting = pending(r, a).await;
    assert_eq!(
        waiting,
        json!([{ "employee_id": yamada, "updated_at": put["updated_at"] }])
    );
    // 承認待ちも人数に数える (上限の判定)
    assert_eq!(
        r.registration_count(a, yamada, FORMAT_CHARA).await.unwrap(),
        (1, true)
    );

    // 管理者以外 (AuthUser 無し・admin 以外) は一覧も承認も 403 で、承認されない
    let approve_uri = format!("/vein/templates/{yamada}/approve");
    let body = json!({ "updated_at": put["updated_at"] });
    for role in [None, Some("user")] {
        let uri = "/vein/templates/pending";
        assert_eq!(
            send_as(r, a, role, "GET", uri, None).await.0,
            StatusCode::FORBIDDEN
        );
        let sent = send_as(r, a, role, "POST", &approve_uri, Some(body.clone())).await;
        assert_eq!(sent.0, StatusCode::FORBIDDEN);
    }
    assert_eq!(list(r, a).await["templates"], json!([]));

    // 別テナントの管理者は承認できない (0 行 → 409)
    assert_eq!(
        approve(r, b, yamada, &put["updated_at"]).await.0,
        StatusCode::CONFLICT
    );
    // 古い updated_at は 409。登録し直しても承認待ちのまま
    let (_, again) = put_template(r, a, yamada, 52).await;
    assert_ne!(again["updated_at"], put["updated_at"]);
    let stale = approve(r, a, yamada, &put["updated_at"]).await;
    assert_eq!(stale.0, StatusCode::CONFLICT);
    assert_eq!(stale.1["error"], "approve_conflict");
    assert_eq!(identify(r, a, 52).await, json!({ "employee_id": null }));

    // 一致する updated_at で承認すると照合・一覧に載り、承認待ちから消える
    let ok = approve(r, a, yamada, &again["updated_at"]).await;
    assert_eq!(ok.0, StatusCode::NO_CONTENT);
    assert_eq!(identify(r, a, 52).await["employee_id"], yamada.to_string());
    assert_eq!(templates(&list(r, a).await).len(), 1);
    assert_eq!(pending(r, a).await, json!([]));
    // 承認済みの再承認は 409
    let twice = approve(r, a, yamada, &again["updated_at"]).await;
    assert_eq!(twice.0, StatusCode::CONFLICT);

    // 承認済みの乗務員を登録し直すと承認待ちに戻り、照合から外れる
    let (_, third) = put_template(r, a, yamada, 53).await;
    assert_eq!(identify(r, a, 53).await, json!({ "employee_id": null }));
    assert_eq!(list(r, a).await["templates"], json!([]));
    assert_eq!(pending(r, a).await[0]["updated_at"], third["updated_at"]);
    // 承認待ちの却下は DELETE
    assert_eq!(
        delete_as_admin(r, a, yamada).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(pending(r, a).await, json!([]));

    // approved_by に承認した管理者が残る (承認し直して superuser で読む)
    let (_, fourth) = put_template(r, a, yamada, 54).await;
    assert_eq!(
        approve(r, a, yamada, &fourth["updated_at"]).await.0,
        StatusCode::NO_CONTENT
    );
    repo.close().await;
    let su = db.superuser().await;
    let row = su
        .inner
        .query_one(
            "SELECT approved_by, approved_at IS NOT NULL FROM alc_api.vein_templates WHERE employee_id = $1",
            &[&yamada],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<Uuid>>(0), Some(admin_id()));
    assert!(row.get::<_, bool>(1));
    su.close().await;
    db.shutdown();
}

/// モジュールの記録 (`template_format = 'module'`) は、形式の列で chara の行と分かれる:
/// module の一覧にだけ出て (承認済みだけ)、identify・`GET /vein/templates`・chara の人数には載らない。
/// 承認・承認待ちの一覧・削除は形式を問わず、chara で登録し直すと chara に戻る。
#[tokio::test(flavor = "multi_thread")]
async fn module_records_are_kept_apart_from_chara_rows() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let a = tenant(&mut prep.inner, "Vein Module Tenant A").await;
    let b = tenant(&mut prep.inner, "Vein Module Tenant B").await;
    let yamada = employee(&mut prep.inner, a, "山田").await;
    let suzuki = employee(&mut prep.inner, a, "鈴木").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;
    let rec = "AAECAw==";
    let put_module = |employee_id: Uuid, record: &'static str| async move {
        let uri = format!("/vein/templates/{employee_id}/module");
        send(r, a, "PUT", &uri, Some(json!({ "record": record }))).await
    };
    let module_list = || async {
        let (status, v) = send(r, a, "GET", "/vein/module-templates", None).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        v
    };

    // chara の山田 (承認済み) と module の鈴木 (承認待ち)
    assert_eq!(put_approved(r, a, yamada, 71).await.0, StatusCode::OK);
    // 登録した端末 (X-Device-Id) つきで登録する
    let device = "alarm:0123456789abcdef";
    let uri = format!("/vein/templates/{suzuki}/module");
    let body = json!({ "record": rec });
    let (status, put) = send_with_device(r, a, device, &uri, body).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    // 承認待ちの module は module の一覧に出ず、承認待ちの一覧には形式を問わず出る
    assert_eq!(module_list().await["templates"], json!([]));
    assert_eq!(pending(r, a).await[0]["employee_id"], suzuki.to_string());
    // 人数は形式ごと (承認待ちを含む)
    assert_eq!(
        r.registration_count(a, suzuki, FORMAT_MODULE)
            .await
            .unwrap(),
        (1, true)
    );
    assert_eq!(
        r.registration_count(a, yamada, FORMAT_MODULE)
            .await
            .unwrap(),
        (1, false)
    );
    assert_eq!(
        r.registration_count(a, suzuki, FORMAT_CHARA).await.unwrap(),
        (1, false)
    );

    // 承認すると module の一覧に record をそのまま載せて出る。chara の一覧・identify には出ない
    assert_eq!(
        approve(r, a, suzuki, &put["updated_at"]).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        module_list().await,
        json!({ "templates": [{
            "employee_id": suzuki,
            "record": rec,
            "updated_at": put["updated_at"],
            "registered_device_id": device,
        }] })
    );
    let chara_list = list(r, a).await;
    assert_eq!(templates(&chara_list).len(), 1);
    assert_eq!(templates(&chara_list)[0]["employee_id"], yamada.to_string());
    assert_eq!(identify(r, a, 71).await["employee_id"], yamada.to_string());
    assert_eq!(approved(r, a).await.as_array().unwrap().len(), 2);

    // 別テナントには出ない
    let (_, other) = send(r, b, "GET", "/vein/module-templates", None).await;
    assert_eq!(other["templates"], json!([]));

    // module の山田への登録し直しは module に変わり、承認待ちに戻って chara の一覧から外れる
    let (status, _) = put_module(yamada, rec).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list(r, a).await["templates"], json!([]));
    assert_eq!(identify(r, a, 71).await, json!({ "employee_id": null }));
    // ヘッダ無しの登録し直しは端末を NULL に戻し、別の端末で登録し直すと上書きする
    let (status, put) = put_module(suzuki, rec).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    assert_eq!(
        approve(r, a, suzuki, &put["updated_at"]).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        module_list().await["templates"][0]["registered_device_id"],
        Value::Null
    );
    let (status, put) = send_with_device(r, a, "kiosk-2", &uri, json!({ "record": rec })).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    assert_eq!(
        approve(r, a, suzuki, &put["updated_at"]).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        module_list().await["templates"][0]["registered_device_id"],
        "kiosk-2"
    );
    // chara で登録し直すと chara に戻る
    assert_eq!(put_approved(r, a, yamada, 72).await.0, StatusCode::OK);
    assert_eq!(identify(r, a, 72).await["employee_id"], yamada.to_string());
    assert_eq!(
        module_list().await["templates"].as_array().unwrap().len(),
        1
    );

    // 削除は形式を問わず管理者だけ
    assert_eq!(
        delete_as_admin(r, a, suzuki).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(module_list().await["templates"], json!([]));
    repo.close().await;
    db.shutdown();
}

/// module の記録を端末の鍵で登録する (承認待ちのまま)。`updated_at` を返す。
async fn put_module_record(repo: &Arc<PgVeinTemplates>, tenant_id: Uuid, id: Uuid) -> Value {
    let uri = format!("/vein/templates/{id}/module");
    let body = json!({ "record": "AAECAw==" });
    let (status, v) = send(repo, tenant_id, "PUT", &uri, Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    v["updated_at"].clone()
}

/// 運行管理者 (社員) の承認を端末の鍵で叩く。`(status, error の code)`。
async fn approve_by_employee(
    repo: &Arc<PgVeinTemplates>,
    tenant_id: Uuid,
    id: Uuid,
    approver: Uuid,
    updated_at: Value,
) -> (StatusCode, Option<String>) {
    let uri = format!("/vein/templates/{id}/approve-by-employee");
    let body = json!({ "approver_employee_id": approver, "updated_at": updated_at });
    let (status, v) = send(repo, tenant_id, "POST", &uri, Some(body)).await;
    (status, v["error"].as_str().map(str::to_owned))
}

fn rejected(status: StatusCode, code: &str) -> (StatusCode, Option<String>) {
    (status, Some(code.to_owned()))
}

async fn pending_modules(repo: &Arc<PgVeinTemplates>, tenant_id: Uuid) -> Vec<Value> {
    let (status, v) = send(
        repo,
        tenant_id,
        "GET",
        "/vein/module-templates/pending",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{v}");
    v["templates"].as_array().unwrap().clone()
}

/// 行の `(approved_by, approved_by_employee_id)`。
async fn approver_columns(
    db: &mut PgClient,
    tenant_id: Uuid,
    employee_id: Uuid,
) -> (Option<Uuid>, Option<Uuid>) {
    db.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let row = tx
                .query_typed_one(
                    "SELECT approved_by, approved_by_employee_id FROM vein_templates WHERE tenant_id = $1 AND employee_id = $2",
                    &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
                )
                .await?;
            Ok((row.get(0), row.get(1)))
        })
    })
    .await
    .unwrap()
}

/// 運行管理者 (社員) による承認: 承認待ちの module の一覧・承認者の検査 (役割・登録・自己承認)・版の固定・
/// `approved_by` が NULL で `approved_by_employee_id` に承認者が入ること・登録し直しで承認待ちに戻ること。
#[tokio::test(flavor = "multi_thread")]
async fn approve_by_employee_checks_the_approver_and_pins_the_version() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let a = tenant(&mut prep.inner, "Vein Approve Tenant A").await;
    let b = tenant(&mut prep.inner, "Vein Approve Tenant B").await;
    let target = employee(&mut prep.inner, a, "山田").await;
    let manager = employee(&mut prep.inner, a, "管理者").await;
    let driver = employee(&mut prep.inner, a, "乗務員").await;
    let waiting = employee(&mut prep.inner, a, "待ち").await;
    let gone = employee(&mut prep.inner, a, "削除済み").await;
    let chara = employee(&mut prep.inner, a, "旧形式").await;
    let foreign = employee(&mut prep.inner, b, "別テナント").await;
    set_roles(&mut prep.inner, a, manager, &["driver", "manager"]).await;
    set_roles(&mut prep.inner, a, waiting, &["admin"]).await;
    set_roles(&mut prep.inner, a, gone, &["manager"]).await;
    set_roles(&mut prep.inner, b, foreign, &["manager"]).await;
    soft_delete_employee(&mut prep.inner, a, gone).await;
    prep.close().await;

    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;
    // 管理者・運行者・別テナントの管理者は承認済み (管理画面の admin が承認した)。待ちと山田は承認待ち
    for (t, id) in [(a, manager), (a, driver), (b, foreign)] {
        let at = put_module_record(r, t, id).await;
        assert_eq!(approve(r, t, id, &at).await.0, StatusCode::NO_CONTENT);
    }
    let waiting_at = put_module_record(r, a, waiting).await;
    let target_at = put_module_record(r, a, target).await;
    // chara の承認待ちは module の一覧に載らない
    assert_eq!(put_template(r, a, chara, 5).await.0, StatusCode::OK);

    // 承認待ちの一覧は module だけ・古い順・記録の中身は無い・別テナントは出ない
    assert_eq!(
        pending_modules(r, a).await,
        vec![
            json!({ "employee_id": waiting, "name": "待ち", "updated_at": waiting_at, "registered_device_id": null }),
            json!({ "employee_id": target, "name": "山田", "updated_at": target_at, "registered_device_id": null }),
        ]
    );
    assert!(pending_modules(r, b).await.is_empty());

    // 承認者の検査: 運行者 / 居ない / 削除済み / 別テナント / 自分の記録が承認待ち / 自己承認
    let not_manager = rejected(StatusCode::FORBIDDEN, "approver_not_manager");
    for approver in [driver, Uuid::new_v4(), gone, foreign] {
        let got = approve_by_employee(r, a, target, approver, target_at.clone()).await;
        assert_eq!(got, not_manager);
    }
    let got = approve_by_employee(r, a, target, waiting, target_at.clone()).await;
    assert_eq!(
        got,
        rejected(StatusCode::FORBIDDEN, "approver_not_enrolled")
    );
    let got = approve_by_employee(r, a, manager, manager, json!("2026-01-01T00:00:00Z")).await;
    assert_eq!(got, rejected(StatusCode::FORBIDDEN, "self_approval"));

    // 版が違えば 409、合えば 204、もう一度は 409
    let conflict = rejected(StatusCode::CONFLICT, "approve_conflict");
    let got = approve_by_employee(r, a, target, manager, json!("2000-01-01T00:00:00Z")).await;
    assert_eq!(got, conflict);
    let got = approve_by_employee(r, a, target, manager, target_at.clone()).await;
    assert_eq!(got, (StatusCode::NO_CONTENT, None));
    let got = approve_by_employee(r, a, target, manager, target_at.clone()).await;
    assert_eq!(got, conflict);
    repo.close().await;

    // approved_by は NULL・approved_by_employee_id は承認者。削除済みの乗務員は承認待ちの一覧から外れる
    let mut prep = db.client(APP_ROLE).await;
    assert_eq!(
        approver_columns(&mut prep.inner, a, target).await,
        (None, Some(manager))
    );
    soft_delete_employee(&mut prep.inner, a, waiting).await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;
    assert!(pending_modules(r, a).await.is_empty());

    // 登録し直すと承認待ちに戻り、approved_by_employee_id も NULL に戻る
    put_module_record(r, a, target).await;
    assert_eq!(pending_modules(r, a).await.len(), 1);
    repo.close().await;
    let mut prep = db.client(APP_ROLE).await;
    assert_eq!(
        approver_columns(&mut prep.inner, a, target).await,
        (None, None)
    );
    prep.close().await;
    db.shutdown();
}

/// テナントの登録の履歴 (`vein_enroll_events`) の `(employee_id, registered_device_id, is_dev)`、古い順。
async fn enroll_events(db: &mut PgClient, tenant_id: Uuid) -> Vec<(Uuid, Option<String>, bool)> {
    db.tenant_tx(tenant_id, move |tx| {
        Box::pin(async move {
            let rows = tx
                .query_typed(
                    "SELECT employee_id, registered_device_id, is_dev FROM vein_enroll_events WHERE tenant_id = $1 ORDER BY created_at, id",
                    &[(&tenant_id, Type::UUID)],
                )
                .await?;
            Ok(rows
                .into_iter()
                .map(|r| (r.get(0), r.get(1), r.get(2)))
                .collect::<Vec<_>>())
        })
    })
    .await
    .unwrap()
}

/// module の記録の登録は、履歴に 1 行と登録した端末 (X-Device-Id) を残す (Refs ippoan/alc-cam#2)。
#[tokio::test(flavor = "multi_thread")]
async fn module_enroll_writes_an_event_with_the_device() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let t = tenant(&mut prep.inner, "Vein Event Module Tenant").await;
    let yamada = employee(&mut prep.inner, t, "山田").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let uri = format!("/vein/templates/{yamada}/module");
    let body = json!({ "record": "AAECAw==" });
    let (status, put) = send_with_device(&repo.inner, t, "kiosk-1", &uri, body).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    repo.close().await;
    let mut prep = db.client(APP_ROLE).await;
    assert_eq!(
        enroll_events(&mut prep.inner, t).await,
        vec![(yamada, Some("kiosk-1".to_owned()), false)]
    );
    prep.close().await;
    db.shutdown();
}

/// chara のテンプレートの登録は、履歴に 1 行 (端末は NULL) を残す。
#[tokio::test(flavor = "multi_thread")]
async fn template_enroll_writes_an_event_without_a_device() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let t = tenant(&mut prep.inner, "Vein Event Template Tenant").await;
    let yamada = employee(&mut prep.inner, t, "山田").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let (status, put) = put_template(&repo.inner, t, yamada, 81).await;
    assert_eq!(status, StatusCode::OK, "{put}");
    repo.close().await;
    let mut prep = db.client(APP_ROLE).await;
    assert_eq!(
        enroll_events(&mut prep.inner, t).await,
        vec![(yamada, None, false)]
    );
    prep.close().await;
    db.shutdown();
}

/// 乗務員が居ない (別テナント・削除済み・存在しない) 登録は 404 で、履歴を書かない。
#[tokio::test(flavor = "multi_thread")]
async fn enroll_without_an_employee_writes_no_event() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let a = tenant(&mut prep.inner, "Vein Event Missing Tenant A").await;
    let b = tenant(&mut prep.inner, "Vein Event Missing Tenant B").await;
    let foreign = employee(&mut prep.inner, b, "他社").await;
    let gone = employee(&mut prep.inner, a, "削除済み").await;
    soft_delete_employee(&mut prep.inner, a, gone).await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;
    for id in [foreign, gone, Uuid::new_v4()] {
        assert_eq!(put_template(r, a, id, 82).await.0, StatusCode::NOT_FOUND);
        let uri = format!("/vein/templates/{id}/module");
        let body = json!({ "record": "AAECAw==" });
        let (status, _) = send_with_device(r, a, "kiosk-1", &uri, body).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
    repo.close().await;
    let mut prep = db.client(APP_ROLE).await;
    assert_eq!(enroll_events(&mut prep.inner, a).await, vec![]);
    assert_eq!(enroll_events(&mut prep.inner, b).await, vec![]);
    prep.close().await;
    db.shutdown();
}

/// 登録し直しはテンプレートを上書きするが、履歴は上書きせず 1 回ごとに 1 行足す。
#[tokio::test(flavor = "multi_thread")]
async fn re_enroll_appends_events() {
    let db = Embedded::start().await;
    let mut prep = db.client(APP_ROLE).await;
    let t = tenant(&mut prep.inner, "Vein Event Append Tenant").await;
    let yamada = employee(&mut prep.inner, t, "山田").await;
    prep.close().await;
    let repo = db.repo(APP_ROLE).await;
    let r = &repo.inner;
    let uri = format!("/vein/templates/{yamada}/module");
    let body = json!({ "record": "AAECAw==" });
    assert_eq!(
        send_with_device(r, t, "kiosk-1", &uri, body.clone())
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        send_with_device(r, t, "kiosk-2", &uri, body).await.0,
        StatusCode::OK
    );
    repo.close().await;
    let mut prep = db.client(APP_ROLE).await;
    assert_eq!(
        enroll_events(&mut prep.inner, t).await,
        vec![
            (yamada, Some("kiosk-1".to_owned()), false),
            (yamada, Some("kiosk-2".to_owned()), false),
        ]
    );
    prep.close().await;
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
    let err = r
        .upsert(t, yamada, "a\0b", FORMAT_CHARA, None)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DbError::Other(m) if m.ends_with("(22021)")),
        "{err:?}"
    );
    // 同じ接続で次のメソッドが通る (ROLLBACK 済み)
    assert_eq!(r.list(t, FORMAT_CHARA).await.unwrap(), vec![]);
    let at = r
        .upsert(t, yamada, "ok", FORMAT_CHARA, None)
        .await
        .unwrap()
        .unwrap();
    assert!(r.approve(t, yamada, admin_id(), at).await.unwrap());
    let row = r.list(t, FORMAT_CHARA).await.unwrap().remove(0);
    let err = r
        .update_learned(t, row.id, "a\0b", row.updated_at)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, DbError::Other(m) if m.ends_with("(22021)")),
        "{err:?}"
    );
    assert_eq!(r.list(t, FORMAT_CHARA).await.unwrap(), vec![row]);
    repo.close().await;

    // 表の権限を持たないロールで繋いだ repo は、7 メソッドとも 42501
    let repo = db.repo("anon").await;
    let r = &repo.inner;
    assert_eq!(r.current_user().await.as_deref(), Some("anon"));
    let denied = |e: DbError| matches!(&e, DbError::Other(m) if m.ends_with("(42501)"));
    assert!(denied(r.list(t, FORMAT_CHARA).await.unwrap_err()));
    assert!(denied(
        r.registration_count(t, yamada, FORMAT_CHARA)
            .await
            .unwrap_err()
    ));
    assert!(denied(
        r.upsert(t, yamada, "x", FORMAT_CHARA, None)
            .await
            .unwrap_err()
    ));
    assert!(denied(
        r.update_learned(t, Uuid::new_v4(), "x", chrono::Utc::now())
            .await
            .unwrap_err()
    ));
    assert!(denied(r.delete(t, yamada).await.unwrap_err()));
    assert!(denied(r.list_by_approval(t, false).await.unwrap_err()));
    let denied_approve = r.approve(t, yamada, admin_id(), chrono::Utc::now()).await;
    assert!(denied(denied_approve.unwrap_err()));
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
    let err = repo.list(t, FORMAT_CHARA).await.unwrap_err();
    assert!(
        matches!(&err, DbError::Other(m) if m == "connection closed"),
        "{err:?}"
    );
    assert!(matches!(
        repo.upsert(t, t, "x", FORMAT_CHARA, None).await,
        Err(DbError::Other(_))
    ));
    assert!(matches!(
        repo.registration_count(t, t, FORMAT_CHARA).await,
        Err(DbError::Other(_))
    ));
    assert!(matches!(
        repo.update_learned(t, t, "x", chrono::Utc::now()).await,
        Err(DbError::Other(_))
    ));
    assert!(matches!(repo.delete(t, t).await, Err(DbError::Other(_))));
    let by_approval = repo.list_by_approval(t, true).await;
    assert!(matches!(by_approval, Err(DbError::Other(_))));
    let approved = repo.approve(t, t, t, chrono::Utc::now()).await;
    assert!(matches!(approved, Err(DbError::Other(_))));
    assert_eq!(repo.current_user().await, None);
    drop(repo);
    db.shutdown();
}
