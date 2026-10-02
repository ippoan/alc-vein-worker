//! `VeinTemplatesRepository` の tokio-postgres 実装 (Refs ippoan/rust-alc-api#723)。直下の worker と
//! 実 DB のテスト (`tests/sql_db.rs`) が、同じこの実装を使う。**接続は持たない** (張るのは worker とテスト)。
//!
//! SQL は [`crate::repo::sql`] の定数だけを、共通 crate `alc-worker-db` の [`TenantTx`] が出す
//! **型付きの名前なしの文** (`query_typed` 系・`execute_typed`) で流す。名前付き prepared statement は
//! Hyperdrive 経由で接続が切れるので使わない (`TenantTx` に口が無い)。引数の型の並びが書かれるのは、
//! 下の自由関数 5 つの 1 か所だけ (`$n` の意味は `repo::sql` の各定数の doc)。
//!
//! ## RLS (trait のメソッド 1 回 = 1 トランザクション)
//!
//! [`PgVeinTemplates`] の各メソッドは `PgClient::tenant_tx` を 1 回開く。順は `BEGIN` →
//! `alc_worker_db::SET_TENANT` (`set_config('app.current_tenant_id', $1, true)` と search_path) → 自由関数 →
//! `COMMIT`。自由関数は [`TenantTx`] を取るので、テナントを設定していない transaction では呼べない
//! (`TenantTx` は `tenant_tx` の中でしか手に入らない)。戻り値は `TxOutput` (owned な型) に限られ、
//! `Row` を transaction の外へは持ち出せない。

use alc_core_wasm::DbError;
use alc_worker_db::{PgClient, TenantTx, TxOutput};
use chrono::{DateTime, Utc};
use futures_util::lock::Mutex;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::repo::{sql, VeinTemplateRow, VeinTemplatesRepository};

/// PostgreSQL の unique_violation (alc-core-wasm の sqlx 版と同じ写し方)。
const PG_UNIQUE_VIOLATION: &str = "23505";

impl TxOutput for VeinTemplateRow {}

/// [`sql::UPSERT`]。`Ok(None)` = そのテナントに生きている乗務員が居ない。
pub async fn upsert(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
    template: &str,
) -> Result<Option<DateTime<Utc>>, tokio_postgres::Error> {
    let row = tx
        .query_typed_opt(
            sql::UPSERT,
            &[
                (&tenant_id, Type::UUID),
                (&employee_id, Type::UUID),
                (&template, Type::TEXT),
            ],
        )
        .await?;
    Ok(row.map(|r| r.get(0)))
}

/// [`sql::LIST`]。
pub async fn list(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
) -> Result<Vec<VeinTemplateRow>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(sql::LIST, &[(&tenant_id, Type::UUID)])
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| VeinTemplateRow {
            id: r.get(0),
            employee_id: r.get(1),
            name: r.get(2),
            template: r.get(3),
            updated_at: r.get(4),
        })
        .collect())
}

/// [`sql::REGISTRATION_COUNT`]。
pub async fn registration_count(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
) -> Result<(i64, bool), tokio_postgres::Error> {
    let row = tx
        .query_typed_one(
            sql::REGISTRATION_COUNT,
            &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

/// [`sql::UPDATE_LEARNED`]。`Ok(true)` = 1 行書いた。
pub async fn update_learned(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    id: Uuid,
    template: &str,
    read_updated_at: DateTime<Utc>,
) -> Result<bool, tokio_postgres::Error> {
    let n = tx
        .execute_typed(
            sql::UPDATE_LEARNED,
            &[
                (&tenant_id, Type::UUID),
                (&id, Type::UUID),
                (&template, Type::TEXT),
                (&read_updated_at, Type::TIMESTAMPTZ),
            ],
        )
        .await?;
    Ok(n == 1)
}

/// [`sql::DELETE`]。`Ok(true)` = 1 行消した。
pub async fn delete(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
) -> Result<bool, tokio_postgres::Error> {
    let n = tx
        .execute_typed(
            sql::DELETE,
            &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
        )
        .await?;
    Ok(n == 1)
}

/// ログは出さない (出すのは呼び手 = worker)。
fn db_err(e: tokio_postgres::Error) -> DbError {
    // tokio_postgres::Error の Display は "db error" だけなので、DB の message も載せる
    match e.as_db_error() {
        Some(db) if db.code().code() == PG_UNIQUE_VIOLATION => {
            DbError::Conflict(db.message().to_string())
        }
        Some(db) => DbError::Other(format!("{} ({})", db.message(), db.code().code())),
        None => DbError::Other(e.to_string()),
    }
}

/// 1 本の接続の上の repo。`tenant_tx` に `&mut PgClient` が要るので async の Mutex で包む。
pub struct PgVeinTemplates {
    client: Mutex<PgClient>,
}

impl PgVeinTemplates {
    pub fn new(client: PgClient) -> Self {
        Self {
            client: Mutex::new(client),
        }
    }

    /// この接続の `current_user` (`PgClient::current_user`。テナントを取らず、transaction も張らない
    /// 固定の 1 文)。失敗は `None` (エラーの詳細は返さない)。
    pub async fn current_user(&self) -> Option<String> {
        let client = self.client.lock().await;
        client.current_user().await.ok().flatten()
    }
}

// `tenant_tx` の閉包は `'static` でない借用を future へ持ち込めないので、`&str` は先に owned にする
#[async_trait::async_trait]
impl VeinTemplatesRepository for PgVeinTemplates {
    async fn upsert(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        template: &str,
    ) -> Result<Option<DateTime<Utc>>, DbError> {
        let template = template.to_owned();
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { upsert(tx, tenant_id, employee_id, &template).await })
            })
            .await
            .map_err(db_err)
    }

    async fn list(&self, tenant_id: Uuid) -> Result<Vec<VeinTemplateRow>, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { list(tx, tenant_id).await })
            })
            .await
            .map_err(db_err)
    }

    async fn registration_count(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
    ) -> Result<(i64, bool), DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { registration_count(tx, tenant_id, employee_id).await })
            })
            .await
            .map_err(db_err)
    }

    async fn update_learned(
        &self,
        tenant_id: Uuid,
        id: Uuid,
        template: &str,
        read_updated_at: DateTime<Utc>,
    ) -> Result<bool, DbError> {
        let template = template.to_owned();
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move {
                    update_learned(tx, tenant_id, id, &template, read_updated_at).await
                })
            })
            .await
            .map_err(db_err)
    }

    async fn delete(&self, tenant_id: Uuid, employee_id: Uuid) -> Result<bool, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { delete(tx, tenant_id, employee_id).await })
            })
            .await
            .map_err(db_err)
    }
}
