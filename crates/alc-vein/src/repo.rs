//! `vein_templates` (乗務員 1 人 1 件の指静脈テンプレート、migrations/151) の読み書き。
//!
//! ここに在るのは trait・型・SQL の定数だけで、DB 実装は [`crate::pg`] が持つ (handler のテストが
//! mock に差し替えられるよう `VeinState` は trait object で持つ)。RLS に加えて
//! `WHERE tenant_id` を明示する (staging は superuser 接続で RLS が効かないため)。

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use alc_core_wasm::DbError;

/// 照合に使う 1 行 (乗務員名つき。削除済みの乗務員は含めない)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VeinTemplateRow {
    pub id: Uuid,
    pub employee_id: Uuid,
    pub name: String,
    pub template: String,
    pub updated_at: DateTime<Utc>,
    /// 記録を登録した端末の id (alc-migrations 183。登録した端末が分からない行は None)。
    pub registered_device_id: Option<String>,
}

/// `vein_templates.template_format` (alc-migrations 182)。`chara` = 特徴量から作ったテンプレート (サーバーが照合する)、
/// `module` = 端末のモジュールが作った記録全体 (サーバーは作り直さず照合もしない)。
pub const FORMAT_CHARA: &str = "chara";
pub const FORMAT_MODULE: &str = "module";

/// `GET /vein/templates` の 1 件 (オフライン照合用)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VeinTemplateItem {
    pub employee_id: Uuid,
    pub template: String,
    pub updated_at: DateTime<Utc>,
}

/// `GET /vein/templates/pending` と `GET /vein/templates/approved` の 1 件 (テンプレートは返さない)。
/// 承認待ちの `updated_at` は承認の版 (`POST .../approve` の body に同じ値を返す)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApprovalItem {
    pub employee_id: Uuid,
    pub updated_at: DateTime<Utc>,
}

/// `GET /vein/module-templates/pending` の 1 件 (記録の中身は返さない)。`updated_at` は承認の版
/// (`POST .../approve-by-employee` の body に同じ値を返す)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingModuleItem {
    pub employee_id: Uuid,
    pub name: String,
    pub updated_at: DateTime<Utc>,
    pub registered_device_id: Option<String>,
}

/// 運行管理者 (社員) による承認の結果。`Approved` 以外は書き込んでいない。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApproveByEmployee {
    Approved,
    /// 承認者が居ない・削除済み・`role` に manager も admin も無い。
    ApproverNotManager,
    /// 承認者自身の module の記録が承認済みでない。
    ApproverNotEnrolled,
    /// 版が違う (登録し直された) か、承認待ちでない (承認済み・登録が無い)。
    Conflict,
}

/// 端末ごとに預かった module の記録 1 行 (`vein_device_records`、alc-migrations 196。Refs ippoan/alc-app#558)。
/// `device_id` はその記録を撮った端末 (`vein_templates.registered_device_id` と同じ id)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceRecordRow {
    pub employee_id: Uuid,
    pub device_id: String,
    pub record: String,
    pub updated_at: DateTime<Utc>,
}

/// `GET /vein/module-templates` の 1 件。`record` は端末のモジュールの記録を base64 のまま (中身は解釈しない)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModuleRecordItem {
    pub employee_id: Uuid,
    pub record: String,
    pub updated_at: DateTime<Utc>,
    /// 登録した端末の id (JSON では null か文字列)。常に元の行 (vein_templates) のもの。
    pub registered_device_id: Option<String>,
    /// 要求した端末のために、端末ごとの記録を混ぜて組んだ記録なら true。
    pub composed: bool,
}

impl From<VeinTemplateRow> for ModuleRecordItem {
    fn from(r: VeinTemplateRow) -> Self {
        Self {
            employee_id: r.employee_id,
            record: r.template,
            updated_at: r.updated_at,
            registered_device_id: r.registered_device_id,
            composed: false,
        }
    }
}

impl From<VeinTemplateRow> for VeinTemplateItem {
    fn from(r: VeinTemplateRow) -> Self {
        Self {
            employee_id: r.employee_id,
            template: r.template,
            updated_at: r.updated_at,
        }
    }
}

/// `vein_templates` の SQL。実装は `crate::pg` (worker と `tests/sql_db.rs` のテストが使う) で、そこが
/// これを使う (placeholder は `$n`)。
/// RLS に加えて `WHERE tenant_id` を明示する。
pub mod sql {
    /// 乗務員のテンプレートを登録し直す。$1 tenant_id / $2 employee_id / $3 template / $4 template_format
    /// (`chara` か `module`) / $5 registered_device_id (登録した端末の id。無ければ NULL) → updated_at (乗務員が居なければ 0 行)。
    /// 登録は常に承認待ち (`approved_at` / `approved_by` / `approved_by_employee_id` = NULL)。上書きも承認待ちに戻す (migration 181 / 185)。
    /// 登録した端末も上書きする (登録し直しで $5 が NULL なら NULL に戻る。migration 183)。
    /// 形式も上書きする (module の行を chara で登録し直したら chara に戻る。逆も同じ。migration 182)。
    pub const UPSERT: &str = r#"INSERT INTO vein_templates (tenant_id, employee_id, template, template_format, registered_device_id, approved_at, approved_by)
    SELECT e.tenant_id, e.id, $3, $4, $5, NULL::timestamptz, NULL::uuid FROM employees e
    WHERE e.tenant_id = $1 AND e.id = $2 AND e.deleted_at IS NULL
    ON CONFLICT (tenant_id, employee_id)
    DO UPDATE SET template = EXCLUDED.template, template_format = EXCLUDED.template_format,
        registered_device_id = EXCLUDED.registered_device_id, updated_at = NOW(), approved_at = NULL, approved_by = NULL,
        approved_by_employee_id = NULL
    RETURNING updated_at"#;

    /// 登録の履歴を 1 行足す ([`UPSERT`] が行を返したときだけ、同じ transaction で)。拠点カメラの録画から
    /// 登録の前後を切り出す cron が読む (ippoan/alc-cam#2、migration 187)。上書きしない。
    /// $1 tenant_id / $2 employee_id / $3 registered_device_id (無ければ NULL)。is_dev は列の DEFAULT
    /// (`app.device_dev` の設定値) で決まるので書かない。
    pub const INSERT_ENROLL_EVENT: &str =
        "INSERT INTO alc_api.vein_enroll_events (tenant_id, employee_id, registered_device_id) VALUES ($1, $2, $3)";

    /// テナントの承認済みの、$2 の形式のテンプレート (削除済みの乗務員を除く、登録順)。照合 (identify) と
    /// `GET /vein/templates` は `chara`、`GET /vein/module-templates` は `module`。承認待ちは載せない。
    /// $1 tenant_id / $2 template_format → id, employee_id, name, template, updated_at, registered_device_id。
    pub const LIST: &str = r#"SELECT v.id, v.employee_id, e.name, v.template, v.updated_at, v.registered_device_id
    FROM vein_templates v
    JOIN employees e ON e.id = v.employee_id AND e.tenant_id = v.tenant_id
    WHERE v.tenant_id = $1 AND e.deleted_at IS NULL AND v.approved_at IS NOT NULL AND v.template_format = $2
    ORDER BY v.created_at, v.id"#;

    /// 承認待ち ($2 = false) か承認済み ($2 = true) の一覧 (削除済みの乗務員を除く、古い順)。テンプレートは返さない。
    /// $1 tenant_id / $2 承認済みか → employee_id, updated_at。
    pub const LIST_BY_APPROVAL: &str = r#"SELECT v.employee_id, v.updated_at
    FROM vein_templates v
    JOIN employees e ON e.id = v.employee_id AND e.tenant_id = v.tenant_id
    WHERE v.tenant_id = $1 AND e.deleted_at IS NULL AND (v.approved_at IS NOT NULL) = $2
    ORDER BY v.updated_at, v.id"#;

    /// 承認する。$4 = 一覧で見た updated_at のままで、承認待ちのときだけ (版の固定)。
    /// $1 tenant_id / $2 employee_id / $3 approved_by (承認した管理者の user_id) / $4 updated_at。0 行 = 409。
    pub const APPROVE: &str = r#"UPDATE vein_templates SET approved_at = NOW(), approved_by = $3
    WHERE tenant_id = $1 AND employee_id = $2 AND updated_at = $4 AND approved_at IS NULL"#;

    /// テナントの承認待ちの module の記録の一覧 (削除済みの乗務員を除く、古い順)。記録の中身は返さない。
    /// $1 tenant_id → employee_id, name, updated_at, registered_device_id。
    pub const LIST_PENDING_MODULE: &str = r#"SELECT v.employee_id, e.name, v.updated_at, v.registered_device_id
    FROM vein_templates v
    JOIN employees e ON e.id = v.employee_id AND e.tenant_id = v.tenant_id
    WHERE v.tenant_id = $1 AND e.deleted_at IS NULL AND v.approved_at IS NULL AND v.template_format = 'module'
    ORDER BY v.updated_at, v.id"#;

    /// 承認者 $2 の検査。生きている社員が居なければ 0 行。居れば (`role` に manager か admin を含むか,
    /// 自分の module の記録が承認済みか)。$1 tenant_id / $2 approver_employee_id → (BOOL, BOOL)。
    pub const APPROVER_CHECK: &str = r#"SELECT COALESCE(e.role && ARRAY['manager', 'admin']::TEXT[], FALSE),
        EXISTS (SELECT 1 FROM vein_templates v
            WHERE v.tenant_id = e.tenant_id AND v.employee_id = e.id
            AND v.template_format = 'module' AND v.approved_at IS NOT NULL)
    FROM employees e
    WHERE e.tenant_id = $1 AND e.id = $2 AND e.deleted_at IS NULL"#;

    /// 運行管理者 (社員) として承認する。`approved_by` (利用者) は NULL のまま、`approved_by_employee_id` に承認者を書く。
    /// $4 = 一覧で見た updated_at のままで、承認待ちの module の記録のときだけ (版の固定)。
    /// $1 tenant_id / $2 employee_id / $3 approver_employee_id / $4 updated_at。0 行 = 409。
    pub const APPROVE_BY_EMPLOYEE: &str = r#"UPDATE vein_templates
    SET approved_at = NOW(), approved_by = NULL, approved_by_employee_id = $3
    WHERE tenant_id = $1 AND employee_id = $2 AND updated_at = $4 AND approved_at IS NULL AND template_format = 'module'"#;

    /// $3 の形式の登録の人数 (承認待ちを含む、削除済みの乗務員を除く) と、$2 employee_id がその中に既に居るか。
    /// $1 tenant_id / $2 employee_id / $3 template_format → (COUNT, BOOL)。
    pub const REGISTRATION_COUNT: &str = r#"SELECT COUNT(*), COALESCE(BOOL_OR(v.employee_id = $2), FALSE)
    FROM vein_templates v
    JOIN employees e ON e.id = v.employee_id AND e.tenant_id = v.tenant_id
    WHERE v.tenant_id = $1 AND e.deleted_at IS NULL AND v.template_format = $3"#;

    /// 学習後のテンプレートの書き戻し ($4 = 読んだときの updated_at のままのときだけ)。$1 tenant_id / $2 id / $3 template。
    pub const UPDATE_LEARNED: &str = r#"UPDATE vein_templates SET template = $3, updated_at = NOW()
    WHERE tenant_id = $1 AND id = $2 AND updated_at = $4"#;

    /// 端末ごとの記録を預かる (同じ端末なら差し替え)。承認済みの module の行が在る生きている乗務員にだけ足せる
    /// (無ければ 0 行)。vein_templates の承認には触らない。
    /// $1 tenant_id / $2 employee_id / $3 device_id / $4 record (base64) → updated_at。
    pub const UPSERT_DEVICE_RECORD: &str = r#"INSERT INTO vein_device_records (tenant_id, employee_id, device_id, record)
    SELECT v.tenant_id, v.employee_id, $3, $4 FROM vein_templates v
    JOIN employees e ON e.id = v.employee_id AND e.tenant_id = v.tenant_id
    WHERE v.tenant_id = $1 AND v.employee_id = $2 AND e.deleted_at IS NULL
        AND v.approved_at IS NOT NULL AND v.template_format = 'module'
    ON CONFLICT (tenant_id, employee_id, device_id)
    DO UPDATE SET record = EXCLUDED.record, updated_at = NOW()
    RETURNING updated_at"#;

    /// テナントの、承認済みの module の行が在る生きている乗務員の端末ごとの記録 (1 回で全員分)。
    /// $1 tenant_id → employee_id, device_id, record, updated_at。
    pub const LIST_DEVICE_RECORDS: &str = r#"SELECT d.employee_id, d.device_id, d.record, d.updated_at
    FROM vein_device_records d
    JOIN vein_templates v ON v.tenant_id = d.tenant_id AND v.employee_id = d.employee_id
    JOIN employees e ON e.id = d.employee_id AND e.tenant_id = d.tenant_id
    WHERE d.tenant_id = $1 AND e.deleted_at IS NULL AND v.approved_at IS NOT NULL AND v.template_format = 'module'
    ORDER BY d.updated_at, d.id"#;

    /// 乗務員の端末ごとの記録を消す ([`DELETE`] と同じ transaction で)。$1 tenant_id / $2 employee_id。
    pub const DELETE_DEVICE_RECORDS: &str =
        "DELETE FROM vein_device_records WHERE tenant_id = $1 AND employee_id = $2";

    /// 乗務員のテンプレートを消す。$1 tenant_id / $2 employee_id。
    pub const DELETE: &str = "DELETE FROM vein_templates WHERE tenant_id = $1 AND employee_id = $2";
}

/// wasm32 (Workers) でも `Send + Sync` を外さない: routes の axum handler と
/// `Router::with_state` が Send を要求するため。!Send を持つ実装は `SendWrapper` で包む。
#[async_trait::async_trait]
pub trait VeinTemplatesRepository: Send + Sync {
    /// 乗務員のテンプレートを `format` ([`FORMAT_CHARA`] か [`FORMAT_MODULE`]) で登録し直す (1 人 1 件)。
    /// `Ok(None)` = そのテナントに生きている乗務員が居ない (404 に写す)。
    async fn upsert(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        template: &str,
        format: &str,
        registered_device_id: Option<&str>,
    ) -> Result<Option<DateTime<Utc>>, DbError>;

    /// テナントの承認済みの `format` のテンプレート (削除済みの乗務員を除く)。並びは登録順。
    async fn list(&self, tenant_id: Uuid, format: &str) -> Result<Vec<VeinTemplateRow>, DbError>;

    /// `approved` = false なら承認待ち、true なら承認済みの一覧 (古い順)。
    async fn list_by_approval(
        &self,
        tenant_id: Uuid,
        approved: bool,
    ) -> Result<Vec<ApprovalItem>, DbError>;

    /// 承認する。`updated_at` が一致し承認待ちのときだけ承認して `Ok(true)`、そうでなければ `Ok(false)` (409 に写す)。
    async fn approve(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        approved_by: Uuid,
        updated_at: DateTime<Utc>,
    ) -> Result<bool, DbError>;

    /// テナントの承認待ちの module の記録の一覧 (古い順。記録の中身は含めない)。
    async fn list_pending_modules(
        &self,
        tenant_id: Uuid,
    ) -> Result<Vec<PendingModuleItem>, DbError>;

    /// 運行管理者 (社員) として承認する。承認者の検査 (生きている社員・manager か admin・自分の module の記録が承認済み)
    /// と書き込みを 1 つの transaction で行う。`employee_id == approver_employee_id` の拒否は呼び手の仕事。
    async fn approve_by_employee(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        approver_employee_id: Uuid,
        updated_at: DateTime<Utc>,
    ) -> Result<ApproveByEmployee, DbError>;

    /// `format` の登録の人数 (承認待ちを含み、削除済みの乗務員を除く) と、
    /// `employee_id` がその中に既に居るか (居れば PUT は上書きで人数が増えない)。
    async fn registration_count(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        format: &str,
    ) -> Result<(i64, bool), DbError>;

    /// 学習後のテンプレートを書き戻す。読んだときの `updated_at` のままのときだけ書き、
    /// 間に登録し直し・別の照合の書き戻しが入っていたら何もしない (`Ok(false)`)。
    async fn update_learned(
        &self,
        tenant_id: Uuid,
        id: Uuid,
        template: &str,
        read_updated_at: DateTime<Utc>,
    ) -> Result<bool, DbError>;

    /// 端末 `device_id` で撮った module の記録を預かる (1 乗務員 1 端末 1 行、同じ端末なら差し替え)。
    /// `Ok(None)` = 承認済みの module の行が在る生きている乗務員が居ない (409 に写す)。
    async fn upsert_device_record(
        &self,
        tenant_id: Uuid,
        employee_id: Uuid,
        device_id: &str,
        record: &str,
    ) -> Result<Option<DateTime<Utc>>, DbError>;

    /// テナントの端末ごとの記録 (承認済みの module の行が在る乗務員の分。古い順)。
    async fn list_device_records(&self, tenant_id: Uuid) -> Result<Vec<DeviceRecordRow>, DbError>;

    /// 乗務員のテンプレート (と端末ごとの記録) を消す。`Ok(false)` = 登録が無い。
    async fn delete(&self, tenant_id: Uuid, employee_id: Uuid) -> Result<bool, DbError>;
}
