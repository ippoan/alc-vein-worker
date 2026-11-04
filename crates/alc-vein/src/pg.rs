//! `VeinTemplatesRepository` の tokio-postgres 実装 (Refs ippoan/rust-alc-api#723)。直下の worker と
//! 組み込みの PostgreSQL に流すテスト (`tests/sql_db.rs`) が、同じこの実装を使う。**接続は持たない** (張るのは worker とテスト)。
//!
//! SQL は [`crate::repo::sql`] の定数だけを、共通 crate `alc-worker-db` の [`TenantTx`] が出す
//! **型付きの名前なしの文** (`query_typed` 系・`execute_typed`) で流す。名前付き prepared statement は
//! Hyperdrive 経由で接続が切れるので使わない (`TenantTx` に口が無い)。引数の型の並びが書かれるのは、
//! 下の自由関数 11 の 1 か所だけ (`$n` の意味は `repo::sql` の各定数の doc)。
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
use serde_json::json;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::auth_shots::{
    AuthShotStatus, Candidate, Judgment, NewAuthShot, StoredJudgment, StoredShot,
};
use crate::repo::{
    sql, ApprovalItem, ApproveByEmployee, DeviceRecordRow, PendingModuleItem, VeinTemplateRow,
    VeinTemplatesRepository,
};

impl TxOutput for VeinTemplateRow {}
impl TxOutput for ApprovalItem {}
impl TxOutput for PendingModuleItem {}
impl TxOutput for ApproveByEmployee {}
impl TxOutput for DeviceRecordRow {}
impl TxOutput for StoredShot {}
impl TxOutput for AuthShotStatus {}

/// [`sql::UPSERT`] → (行を返したときだけ) [`sql::INSERT_ENROLL_EVENT`] を、渡された 1 つの transaction の中で
/// 続けて流す (テンプレートと登録の履歴がずれない)。`Ok(None)` = そのテナントに生きている乗務員が居ない (履歴も書かない)。
pub async fn upsert(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
    template: &str,
    format: &str,
    registered_device_id: Option<&str>,
) -> Result<Option<DateTime<Utc>>, tokio_postgres::Error> {
    let row = tx
        .query_typed_opt(
            sql::UPSERT,
            &[
                (&tenant_id, Type::UUID),
                (&employee_id, Type::UUID),
                (&template, Type::TEXT),
                (&format, Type::TEXT),
                (&registered_device_id, Type::TEXT),
            ],
        )
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    tx.execute_typed(
        sql::INSERT_ENROLL_EVENT,
        &[
            (&tenant_id, Type::UUID),
            (&employee_id, Type::UUID),
            (&registered_device_id, Type::TEXT),
        ],
    )
    .await?;
    Ok(Some(row.get(0)))
}

/// [`sql::LIST`]。
pub async fn list(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    format: &str,
) -> Result<Vec<VeinTemplateRow>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(
            sql::LIST,
            &[(&tenant_id, Type::UUID), (&format, Type::TEXT)],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| VeinTemplateRow {
            id: r.get(0),
            employee_id: r.get(1),
            name: r.get(2),
            template: r.get(3),
            updated_at: r.get(4),
            registered_device_id: r.get(5),
        })
        .collect())
}

/// [`sql::LIST_BY_APPROVAL`]。
pub async fn list_by_approval(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    approved: bool,
) -> Result<Vec<ApprovalItem>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(
            sql::LIST_BY_APPROVAL,
            &[(&tenant_id, Type::UUID), (&approved, Type::BOOL)],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| ApprovalItem {
            employee_id: r.get(0),
            updated_at: r.get(1),
        })
        .collect())
}

/// [`sql::APPROVE`]。`Ok(true)` = 1 行承認した。
pub async fn approve(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
    approved_by: Uuid,
    updated_at: DateTime<Utc>,
) -> Result<bool, tokio_postgres::Error> {
    let n = tx
        .execute_typed(
            sql::APPROVE,
            &[
                (&tenant_id, Type::UUID),
                (&employee_id, Type::UUID),
                (&approved_by, Type::UUID),
                (&updated_at, Type::TIMESTAMPTZ),
            ],
        )
        .await?;
    Ok(n == 1)
}

/// [`sql::LIST_PENDING_MODULE`]。
pub async fn list_pending_modules(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
) -> Result<Vec<PendingModuleItem>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(sql::LIST_PENDING_MODULE, &[(&tenant_id, Type::UUID)])
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| PendingModuleItem {
            employee_id: r.get(0),
            name: r.get(1),
            updated_at: r.get(2),
            registered_device_id: r.get(3),
        })
        .collect())
}

/// [`sql::APPROVER_CHECK`] → [`sql::APPROVE_BY_EMPLOYEE`] を、渡された 1 つの transaction の中で続けて流す。
pub async fn approve_by_employee(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
    approver_employee_id: Uuid,
    updated_at: DateTime<Utc>,
) -> Result<ApproveByEmployee, tokio_postgres::Error> {
    let check = tx
        .query_typed_opt(
            sql::APPROVER_CHECK,
            &[
                (&tenant_id, Type::UUID),
                (&approver_employee_id, Type::UUID),
            ],
        )
        .await?;
    let (is_manager, enrolled): (bool, bool) = match check {
        Some(r) => (r.get(0), r.get(1)),
        None => (false, false),
    };
    if !is_manager {
        return Ok(ApproveByEmployee::ApproverNotManager);
    }
    if !enrolled {
        return Ok(ApproveByEmployee::ApproverNotEnrolled);
    }
    let n = tx
        .execute_typed(
            sql::APPROVE_BY_EMPLOYEE,
            &[
                (&tenant_id, Type::UUID),
                (&employee_id, Type::UUID),
                (&approver_employee_id, Type::UUID),
                (&updated_at, Type::TIMESTAMPTZ),
            ],
        )
        .await?;
    Ok(if n == 1 {
        ApproveByEmployee::Approved
    } else {
        ApproveByEmployee::Conflict
    })
}

/// [`sql::REGISTRATION_COUNT`]。
pub async fn registration_count(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
    format: &str,
) -> Result<(i64, bool), tokio_postgres::Error> {
    let row = tx
        .query_typed_one(
            sql::REGISTRATION_COUNT,
            &[
                (&tenant_id, Type::UUID),
                (&employee_id, Type::UUID),
                (&format, Type::TEXT),
            ],
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

/// [`sql::UPSERT_DEVICE_RECORD`]。`Ok(None)` = 承認済みの module の行が在る乗務員が居ない。
pub async fn upsert_device_record(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
    device_id: &str,
    record: &str,
) -> Result<Option<DateTime<Utc>>, tokio_postgres::Error> {
    let row = tx
        .query_typed_opt(
            sql::UPSERT_DEVICE_RECORD,
            &[
                (&tenant_id, Type::UUID),
                (&employee_id, Type::UUID),
                (&device_id, Type::TEXT),
                (&record, Type::TEXT),
            ],
        )
        .await?;
    Ok(row.map(|r| r.get(0)))
}

/// [`sql::LIST_DEVICE_RECORDS`]。
pub async fn list_device_records(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
) -> Result<Vec<DeviceRecordRow>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(sql::LIST_DEVICE_RECORDS, &[(&tenant_id, Type::UUID)])
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| DeviceRecordRow {
            employee_id: r.get(0),
            device_id: r.get(1),
            record: r.get(2),
            updated_at: r.get(3),
        })
        .collect())
}

/// [`sql::DELETE_DEVICE_RECORDS`] → [`sql::DELETE`] を、渡された 1 つの transaction の中で続けて流す。
/// `Ok(true)` = テンプレートの行を 1 行消した。
pub async fn delete(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    employee_id: Uuid,
) -> Result<bool, tokio_postgres::Error> {
    tx.execute_typed(
        sql::DELETE_DEVICE_RECORDS,
        &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
    )
    .await?;
    let n = tx
        .execute_typed(
            sql::DELETE,
            &[(&tenant_id, Type::UUID), (&employee_id, Type::UUID)],
        )
        .await?;
    Ok(n == 1)
}

/// [`sql::INSERT_AUTH_SHOT`] → [`sql::INSERT_AUTH_JUDGMENT`] (method ごとに 1 行) を、渡された 1 つの transaction の中で続けて流す。
/// `Ok(false)` = 生きている乗務員が居ない (どちらも書かない)。
pub async fn insert_auth_shot(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    shot: &NewAuthShot,
    judgments: &[Judgment],
) -> Result<bool, tokio_postgres::Error> {
    let n = tx
        .execute_typed(
            sql::INSERT_AUTH_SHOT,
            &[
                (&shot.id, Type::UUID),
                (&tenant_id, Type::UUID),
                (&shot.employee_id, Type::UUID),
                (&shot.device_id, Type::TEXT),
                (&shot.firmware, Type::TEXT),
                (&shot.module_result, Type::TEXT),
                (&shot.module_slot, Type::INT4),
                (&shot.module_hit, Type::TEXT),
                (&shot.module_score, Type::INT4),
                (&shot.module_thresholds, Type::INT4_ARRAY),
                (&shot.shot_kind, Type::TEXT),
                (&shot.shot_encrypted, Type::TEXT),
                (&shot.captured_at, Type::TIMESTAMPTZ),
                (&shot.pick, Type::TEXT),
            ],
        )
        .await?;
    if n == 0 {
        return Ok(false);
    }
    for judgment in judgments {
        insert_judgment(tx, tenant_id, shot.id, judgment).await?;
    }
    Ok(true)
}

/// [`sql::INSERT_AUTH_JUDGMENT`]。candidates は JSON の文字列にして渡す (SQL 側で jsonb にする)。
pub async fn insert_judgment(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    shot_id: Uuid,
    judgment: &Judgment,
) -> Result<(), tokio_postgres::Error> {
    let candidates = json!(judgment.candidates).to_string();
    tx.execute_typed(
        sql::INSERT_AUTH_JUDGMENT,
        &[
            (&tenant_id, Type::UUID),
            (&shot_id, Type::UUID),
            (&judgment.logic_version, Type::TEXT),
            (&judgment.method, Type::TEXT),
            (&judgment.result, Type::TEXT),
            (&judgment.matched_employee_id, Type::UUID),
            (&candidates, Type::TEXT),
            (&judgment.error, Type::TEXT),
            (&judgment.elapsed_us, Type::INT4),
        ],
    )
    .await?;
    Ok(())
}

/// [`sql::LIST_AUTH_SHOTS`]。
pub async fn list_auth_shots(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    since: Option<DateTime<Utc>>,
    limit: i64,
) -> Result<Vec<StoredShot>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(
            sql::LIST_AUTH_SHOTS,
            &[
                (&tenant_id, Type::UUID),
                (&since, Type::TIMESTAMPTZ),
                (&limit, Type::INT8),
            ],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| StoredShot {
            id: r.get(0),
            employee_id: r.get(1),
            device_id: r.get(2),
            shot_encrypted: r.get(3),
            pick: r.get(4),
            created_at: r.get(5),
        })
        .collect())
}

/// [`sql::LIST_AUTH_SHOT_STATUS`]。判定の列は、判定の行が無ければ全部 NULL (= `judgment: None`)。
pub async fn list_auth_shot_status(
    tx: &TenantTx<'_>,
    tenant_id: Uuid,
    limit: i64,
) -> Result<Vec<AuthShotStatus>, tokio_postgres::Error> {
    let rows = tx
        .query_typed(
            sql::LIST_AUTH_SHOT_STATUS,
            &[(&tenant_id, Type::UUID), (&limit, Type::INT8)],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let judgment = stored_judgment(&r, 13);
            let judgment_top10 = stored_judgment(&r, 21);
            AuthShotStatus {
                id: r.get(0),
                employee_id: r.get(1),
                device_id: r.get(2),
                firmware: r.get(3),
                module_result: r.get(4),
                module_slot: r.get(5),
                module_hit: r.get(6),
                module_score: r.get(7),
                module_thresholds: r.get(8),
                pick: r.get(9),
                shot_kind: r.get(10),
                captured_at: r.get(11),
                created_at: r.get(12),
                judgment,
                judgment_top10,
            }
        })
        .collect())
}

/// 判定の 8 列 (logic_version から created_at) を `base` 列目から読む。LEFT JOIN で行が無ければ全部 NULL (= `None`)。
fn stored_judgment(r: &tokio_postgres::Row, base: usize) -> Option<StoredJudgment> {
    r.get::<_, Option<String>>(base)
        .map(|logic_version| StoredJudgment {
            judgment: Judgment {
                logic_version,
                method: r.get(base + 1),
                result: r.get(base + 2),
                matched_employee_id: r.get(base + 3),
                candidates: serde_json::from_str::<Vec<Candidate>>(&r.get::<_, String>(base + 4))
                    .unwrap_or_default(),
                error: r.get(base + 5),
                elapsed_us: r.get(base + 6),
            },
            created_at: r.get(base + 7),
        })
}

/// [`sql::PURGE_AUTH_SHOTS`]。消した行数。
pub async fn purge_auth_shots(tx: &TenantTx<'_>) -> Result<i64, tokio_postgres::Error> {
    let row = tx.query_typed_one(sql::PURGE_AUTH_SHOTS, &[]).await?;
    Ok(row.get(0))
}

/// ログは出さない (出すのは呼び手 = worker)。
fn db_err(e: tokio_postgres::Error) -> DbError {
    // tokio_postgres::Error の Display は "db error" だけなので、DB の message も載せる
    match e.as_db_error() {
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
        format: &str,
        registered_device_id: Option<&str>,
    ) -> Result<Option<DateTime<Utc>>, DbError> {
        let template = template.to_owned();
        let format = format.to_owned();
        let registered_device_id = registered_device_id.map(str::to_owned);
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move {
                    upsert(
                        tx,
                        tenant_id,
                        employee_id,
                        &template,
                        &format,
                        registered_device_id.as_deref(),
                    )
                    .await
                })
            })
            .await
            .map_err(db_err)
    }

    async fn list(&self, tenant_id: Uuid, format: &str) -> Result<Vec<VeinTemplateRow>, DbError> {
        let format = format.to_owned();
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { list(tx, tenant_id, &format).await })
            })
            .await
            .map_err(db_err)
    }

    async fn list_by_approval(
        &self,
        tenant_id: Uuid,
        approved: bool,
    ) -> Result<Vec<ApprovalItem>, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { list_by_approval(tx, tenant_id, approved).await })
            })
            .await
            .map_err(db_err)
    }

    async fn approve(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        approved_by: Uuid,
        updated_at: DateTime<Utc>,
    ) -> Result<bool, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move {
                    approve(tx, tenant_id, employee_id, approved_by, updated_at).await
                })
            })
            .await
            .map_err(db_err)
    }

    async fn list_pending_modules(
        &self,
        tenant_id: Uuid,
    ) -> Result<Vec<PendingModuleItem>, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { list_pending_modules(tx, tenant_id).await })
            })
            .await
            .map_err(db_err)
    }

    async fn approve_by_employee(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        approver_employee_id: Uuid,
        updated_at: DateTime<Utc>,
    ) -> Result<ApproveByEmployee, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move {
                    approve_by_employee(
                        tx,
                        tenant_id,
                        employee_id,
                        approver_employee_id,
                        updated_at,
                    )
                    .await
                })
            })
            .await
            .map_err(db_err)
    }

    async fn registration_count(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        format: &str,
    ) -> Result<(i64, bool), DbError> {
        let format = format.to_owned();
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(
                    async move { registration_count(tx, tenant_id, employee_id, &format).await },
                )
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

    async fn upsert_device_record(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        device_id: &str,
        record: &str,
    ) -> Result<Option<DateTime<Utc>>, DbError> {
        let device_id = device_id.to_owned();
        let record = record.to_owned();
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move {
                    upsert_device_record(tx, tenant_id, employee_id, &device_id, &record).await
                })
            })
            .await
            .map_err(db_err)
    }

    async fn list_device_records(&self, tenant_id: Uuid) -> Result<Vec<DeviceRecordRow>, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { list_device_records(tx, tenant_id).await })
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

    async fn insert_auth_shot(
        &self,
        tenant_id: Uuid,
        shot: &NewAuthShot,
        judgments: &[Judgment],
    ) -> Result<bool, DbError> {
        let (shot, judgments) = (shot.clone(), judgments.to_vec());
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { insert_auth_shot(tx, tenant_id, &shot, &judgments).await })
            })
            .await
            .map_err(db_err)
    }

    async fn list_auth_shots(
        &self,
        tenant_id: Uuid,
        since: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<StoredShot>, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { list_auth_shots(tx, tenant_id, since, limit).await })
            })
            .await
            .map_err(db_err)
    }

    async fn insert_judgments(
        &self,
        tenant_id: Uuid,
        judgments: &[(Uuid, Judgment)],
    ) -> Result<(), DbError> {
        let judgments = judgments.to_vec();
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move {
                    for (shot_id, judgment) in &judgments {
                        insert_judgment(tx, tenant_id, *shot_id, judgment).await?;
                    }
                    Ok(())
                })
            })
            .await
            .map_err(db_err)
    }

    async fn list_auth_shot_status(
        &self,
        tenant_id: Uuid,
        limit: i64,
    ) -> Result<Vec<AuthShotStatus>, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(tenant_id, move |tx| {
                Box::pin(async move { list_auth_shot_status(tx, tenant_id, limit).await })
            })
            .await
            .map_err(db_err)
    }

    /// テナントをまたぐ定時の掃除。テナントは nil で、消すのは SECURITY DEFINER の関数
    async fn purge_auth_shots(&self) -> Result<i64, DbError> {
        let mut client = self.client.lock().await;
        client
            .tenant_tx(Uuid::nil(), |tx| {
                Box::pin(async move { purge_auth_shots(tx).await })
            })
            .await
            .map_err(db_err)
    }
}
