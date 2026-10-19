//! Worker の `VeinTemplatesRepository`。**実装は `alc_vein::pg`** (SQL は alc-vein の `repo::sql` の定数、
//! テナントの transaction の部品は共通 crate `alc-worker-db`。この Worker に SQL を書かない) で、
//! ここが足すのは DB に使った時間の計測と、失敗のログだけ。接続は [`crate::db::connect`] が張る。
//!
//! ## RLS (trait のメソッド 1 回 = 1 トランザクション)
//!
//! DB の前には transaction mode のプーラー (staging は Container 内の PgBouncer、本番は
//! Supabase のプーラー) が入り、トランザクション単位で上流のコネクションを使い回す。
//! monolith の `set_current_tenant` (migrations/004・062、`set_config(.., false)` = session スコープ)
//! をそのまま打つと、tenant が別リクエスト (別テナント) へ漏れるか、次の文が別の
//! コネクションに載って行ゼロになる。だから必ず `BEGIN` の中で
//! `set_config('app.current_tenant_id', $1, true)` (= `SET LOCAL`、COMMIT/ROLLBACK で消える)
//! を打ち、同じトランザクションの中でクエリを流す (`alc_worker_db::PgClient::tenant_tx` がこの順を固定する)。
//!
//! `set_current_tenant` は `set_config` を包むだけ (検証なし) で、SECURITY DEFINER も
//! custom GUC の設定に権限が要らないので効いていない。第 3 引数を `true` にした
//! `set_config` を直接打つのと、スコープ以外は同じ。
//!
//! search_path も同じ `SELECT` で `SET LOCAL` 相当にする (プーラーが接続文字列の
//! `options=-c search_path=..` を上流へ渡す保証が無く、DB 既定に頼らないため)。
//!
//! 流すのは型付きの名前なしの文だけ (名前付き prepared statement は Hyperdrive 経由で接続が切れる。
//! Refs ippoan/rust-alc-api#723)。
//!
//! メソッドごとにトランザクションを閉じるので、`POST /vein/identify` は
//! 「tx1 で一覧 → トランザクションの外で照合 → tx2 で学習の書き戻し」になり、照合の CPU の間
//! プーラーのコネクションを握らない。

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use alc_core_wasm::DbError;
use alc_vein::pg::PgVeinTemplates;
use alc_vein::repo::{VeinTemplateRow, VeinTemplatesRepository};
use alc_worker_db::PgClient;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use worker::{console_error, Date};

/// 1 リクエストぶんの repo。接続 ([`PgClient`]) は handler の外 (fetch) で張って渡す。
pub struct WorkerVeinTemplatesRepository {
    inner: PgVeinTemplates,
    /// DB に使った時間の合計 (ms)。fetch がリクエスト全体から引いて Server-Timing に載せる
    db_ms: AtomicU64,
}

impl WorkerVeinTemplatesRepository {
    pub fn new(client: PgClient) -> Self {
        Self {
            inner: PgVeinTemplates::new(client),
            db_ms: AtomicU64::new(0),
        }
    }

    pub fn db_ms(&self) -> u64 {
        self.db_ms.load(Ordering::Relaxed)
    }

    /// この接続の `current_user` (`GET /internal/db-role` 用、Refs ippoan/auth-worker#605)。
    /// テナントを取らず、トランザクションも張らない単発の 1 文 (`alc_worker_db::PgClient::current_user`)。
    /// 失敗は `None` (エラーの詳細は呼び出し側にもログにも出さない)。
    pub async fn current_user(&self) -> Option<String> {
        let started = Date::now().as_millis();
        let user = self.inner.current_user().await;
        self.add_db_ms(started);
        user
    }

    /// `inner` の 1 メソッドを待ち、成功したときだけ所要時間を `db_ms` に足す。
    /// routes は 500 の中身を返さないので、失敗の原因はここで Workers のログに残す。
    async fn timed<T>(&self, call: impl Future<Output = Result<T, DbError>>) -> Result<T, DbError> {
        let started = Date::now().as_millis();
        match call.await {
            Ok(out) => {
                self.add_db_ms(started);
                Ok(out)
            }
            Err(err) => {
                console_error!("vein repo: {err:?}");
                Err(err)
            }
        }
    }

    fn add_db_ms(&self, started: u64) {
        let spent = Date::now().as_millis().saturating_sub(started);
        self.db_ms.fetch_add(spent, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl VeinTemplatesRepository for WorkerVeinTemplatesRepository {
    async fn upsert(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        template: &str,
    ) -> Result<Option<DateTime<Utc>>, DbError> {
        self.timed(self.inner.upsert(tenant_id, employee_id, template))
            .await
    }

    async fn list(&self, tenant_id: Uuid) -> Result<Vec<VeinTemplateRow>, DbError> {
        self.timed(self.inner.list(tenant_id)).await
    }

    async fn registration_count(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
    ) -> Result<(i64, bool), DbError> {
        self.timed(self.inner.registration_count(tenant_id, employee_id))
            .await
    }

    async fn update_learned(
        &self,
        tenant_id: Uuid,
        id: Uuid,
        template: &str,
        read_updated_at: DateTime<Utc>,
    ) -> Result<bool, DbError> {
        self.timed(
            self.inner
                .update_learned(tenant_id, id, template, read_updated_at),
        )
        .await
    }

    async fn delete(&self, tenant_id: Uuid, employee_id: Uuid) -> Result<bool, DbError> {
        self.timed(self.inner.delete(tenant_id, employee_id)).await
    }
}
