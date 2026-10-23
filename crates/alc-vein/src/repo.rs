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

/// `GET /vein/module-templates` の 1 件。`record` は端末のモジュールの記録を base64 のまま (中身は解釈しない)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModuleRecordItem {
    pub employee_id: Uuid,
    pub record: String,
    pub updated_at: DateTime<Utc>,
    /// 登録した端末の id (JSON では null か文字列)。
    pub registered_device_id: Option<String>,
}

impl From<VeinTemplateRow> for ModuleRecordItem {
    fn from(r: VeinTemplateRow) -> Self {
        Self {
            employee_id: r.employee_id,
            record: r.template,
            updated_at: r.updated_at,
            registered_device_id: r.registered_device_id,
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
    /// 登録は常に承認待ち (`approved_at` / `approved_by` = NULL)。上書きも承認待ちに戻す (migration 181)。
    /// 登録した端末も上書きする (登録し直しで $5 が NULL なら NULL に戻る。migration 183)。
    /// 形式も上書きする (module の行を chara で登録し直したら chara に戻る。逆も同じ。migration 182)。
    pub const UPSERT: &str = r#"INSERT INTO vein_templates (tenant_id, employee_id, template, template_format, registered_device_id, approved_at, approved_by)
    SELECT e.tenant_id, e.id, $3, $4, $5, NULL::timestamptz, NULL::uuid FROM employees e
    WHERE e.tenant_id = $1 AND e.id = $2 AND e.deleted_at IS NULL
    ON CONFLICT (tenant_id, employee_id)
    DO UPDATE SET template = EXCLUDED.template, template_format = EXCLUDED.template_format,
        registered_device_id = EXCLUDED.registered_device_id, updated_at = NOW(), approved_at = NULL, approved_by = NULL
    RETURNING updated_at"#;

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

    /// $3 の形式の登録の人数 (承認待ちを含む、削除済みの乗務員を除く) と、$2 employee_id がその中に既に居るか。
    /// $1 tenant_id / $2 employee_id / $3 template_format → (COUNT, BOOL)。
    pub const REGISTRATION_COUNT: &str = r#"SELECT COUNT(*), COALESCE(BOOL_OR(v.employee_id = $2), FALSE)
    FROM vein_templates v
    JOIN employees e ON e.id = v.employee_id AND e.tenant_id = v.tenant_id
    WHERE v.tenant_id = $1 AND e.deleted_at IS NULL AND v.template_format = $3"#;

    /// 学習後のテンプレートの書き戻し ($4 = 読んだときの updated_at のままのときだけ)。$1 tenant_id / $2 id / $3 template。
    pub const UPDATE_LEARNED: &str = r#"UPDATE vein_templates SET template = $3, updated_at = NOW()
    WHERE tenant_id = $1 AND id = $2 AND updated_at = $4"#;

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

    /// 乗務員のテンプレートを消す。`Ok(false)` = 登録が無い。
    async fn delete(&self, tenant_id: Uuid, employee_id: Uuid) -> Result<bool, DbError>;
}
