//! テストの中で起こす組み込みの PostgreSQL (`pglite-oxide`)。docker も外の DB も要らない。
//!
//! 制約 (実測): **同時に張れる接続は 1 本** (2 本目は 1 本目が閉じるまで待たされる)。なので準備の接続と
//! repo の接続は同時には持てず、[`Held::close`] で閉じ切ってから次を張る。
//! `ALTER DATABASE … SET search_path` が効かないので、migration を流す接続だけ options で search_path を渡す
//! (repo の接続には渡さない — `alc_worker_db::SET_TENANT` が transaction ごとに設定する)。
//!
//! migration を流す順は `container/start.sh` と同じ (順を変えたらもう一方も — [`Embedded::migrate`])。

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alc_vein::pg::PgVeinTemplates;
use alc_worker_db::PgClient;
use pglite_oxide::PgliteServer;
use tokio::task::JoinHandle;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

/// 表の所有者でない・NOBYPASSRLS のロール (`local_app_grants.sql` が権限を付ける)。
pub const APP_ROLE: &str = "alc_api_app";
const SUPERUSER: &str = "postgres";
const SOCKET_FILE: &str = ".s.PGSQL.5432";
const SEARCH_PATH: &str = "-c search_path=alc_api,public";

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// `scripts/fetch-migrations.sh` が取り出す先 (版は `container/ALC_MIGRATIONS_REV`)。
fn migrations_dir() -> PathBuf {
    let dir = PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../container/.alc-migrations"
    ));
    assert!(
        dir.join("migrations").is_dir(),
        "container/.alc-migrations が無い。先に `bash scripts/fetch-migrations.sh` を流す"
    );
    dir
}

/// unix socket の置き場。`sockaddr_un` は 108 byte までなので、TMPDIR が長いときは相対パス
/// (cwd = crate の直下) に置く。
fn socket_dir() -> PathBuf {
    let name = format!(
        "vein-pg-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    );
    let abs = std::env::temp_dir().join(&name);
    if abs.as_os_str().len() + 1 + SOCKET_FILE.len() < 100 {
        abs
    } else {
        PathBuf::from(format!(".{name}"))
    }
}

/// 接続 1 本 (`T` = `Client`・`PgClient`・`Arc<PgVeinTemplates>`) と、その接続の task。
pub struct Held<T> {
    pub inner: T,
    task: JoinHandle<()>,
}

impl<T> Held<T> {
    /// 接続を閉じ切る (次の接続を張る前に必ず呼ぶ)。`inner` の写しが残っていると閉じないので、時間切れで落とす。
    pub async fn close(self) {
        drop(self.inner);
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .expect("接続が閉じない (Client の写しが残っている)")
            .unwrap();
    }

    /// 接続の task を止める (= 接続が切れた状態にする)。`inner` はそのまま返す。
    pub async fn sever(self) -> T {
        self.task.abort();
        let _ = self.task.await;
        self.inner
    }
}

pub struct Embedded {
    server: Option<PgliteServer>,
    socket_dir: PathBuf,
}

impl Embedded {
    /// 起動 → `init_local_db.sql` → 空の `_sqlx_migrations` → migrations (1 ファイル 1 transaction) →
    /// `local_app_grants.sql` (`container/start.sh` と同じ順) → 接続ロールの検査。
    pub async fn start() -> Self {
        let socket_dir = socket_dir();
        std::fs::create_dir_all(&socket_dir).unwrap();
        let server = PgliteServer::builder()
            .temporary()
            .database("postgres")
            .unix(socket_dir.join(SOCKET_FILE))
            .start()
            .unwrap();
        let this = Self {
            server: Some(server),
            socket_dir,
        };
        this.migrate().await;
        // 全テストがここを通る: 以降の接続ロールが RLS を素通りしない・表の所有者でないこと
        let mut app = this.client(APP_ROLE).await;
        assert_rls_applies(&mut app.inner).await;
        app.close().await;
        this
    }

    async fn connect(&self, user: &str, options: Option<&str>) -> Held<Client> {
        let mut config = tokio_postgres::Config::new();
        config
            .host_path(&self.socket_dir)
            .port(5432)
            .user(user)
            .dbname("postgres");
        if let Some(options) = options {
            config.options(options);
        }
        let (client, connection) = config.connect(NoTls).await.unwrap();
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        Held {
            inner: client,
            task,
        }
    }

    /// superuser の接続 (準備だけに使う)。
    pub async fn superuser(&self) -> Held<Client> {
        self.connect(SUPERUSER, Some(SEARCH_PATH)).await
    }

    /// 流す順は `container/start.sh` (staging の DB の image) と同じ。**順を変えたら `container/start.sh` も変える。**
    /// 種 (`tests/seed.sql`) と PgBouncer は無い (テストが自分でテナントと乗務員を作り、直結で流す)。
    async fn migrate(&self) {
        let dir = migrations_dir();
        let read = |p: PathBuf| std::fs::read_to_string(p).unwrap();
        let su = self.superuser().await;
        su.inner
            .batch_execute(&read(dir.join("scripts/init_local_db.sql")))
            .await
            .unwrap();
        su.inner
            .batch_execute("CREATE TABLE IF NOT EXISTS alc_api._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)")
            .await
            .unwrap();
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir.join("migrations"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "sql"))
            .collect();
        files.sort();
        assert!(!files.is_empty());
        for file in files {
            let name = file.file_name().unwrap().to_owned();
            let sql = read(file);
            if let Err(e) = su
                .inner
                .batch_execute(&format!("BEGIN;\n{sql}\n;COMMIT;"))
                .await
            {
                panic!("migration {name:?}: {:?}", e.code());
            }
        }
        su.inner
            .batch_execute(&read(dir.join("scripts/local_app_grants.sql")))
            .await
            .unwrap();
        // RLS が効く alc_api_app で繋がせる (init_local_db.sql では NOLOGIN)
        su.inner
            .batch_execute("ALTER ROLE alc_api_app LOGIN")
            .await
            .unwrap();
        su.close().await;
    }

    /// `role` で繋いだ `PgClient` (準備に使う)。
    pub async fn client(&self, role: &str) -> Held<PgClient> {
        let Held { inner, task } = self.connect(role, None).await;
        Held {
            inner: PgClient::new(inner),
            task,
        }
    }

    /// worker と同じ実装の repo (`role` で繋ぐ)。
    pub async fn repo(&self, role: &str) -> Held<Arc<PgVeinTemplates>> {
        let Held { inner, task } = self.client(role).await;
        Held {
            inner: Arc::new(PgVeinTemplates::new(inner)),
            task,
        }
    }

    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        if let Some(server) = self.server.take() {
            server.shutdown().unwrap();
        }
        let _ = std::fs::remove_dir_all(&self.socket_dir);
    }
}

impl Drop for Embedded {
    fn drop(&mut self) {
        self.stop();
    }
}

/// superuser / BYPASSRLS で繋ぐと RLS を素通りして、テナント分離のテストが意味を失う。
/// `vein_templates` には FORCE ROW LEVEL SECURITY が無いので、表の所有者でも同じ。
async fn assert_rls_applies(db: &mut PgClient) {
    let (bypass, owner): (bool, bool) = db
        .tenant_tx(Uuid::new_v4(), |tx| {
            Box::pin(async move {
                let row = tx
                    .query_typed_one(
                        "SELECT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), \
                         (SELECT tableowner = current_user FROM pg_tables WHERE schemaname = 'alc_api' AND tablename = 'vein_templates')",
                        &[],
                    )
                    .await?;
                Ok((row.get(0), row.get(1)))
            })
        })
        .await
        .unwrap();
    assert!(!bypass, "RLS を素通りするロールで繋いでいる");
    assert!(!owner, "表の所有者で繋いでいる");
}

pub async fn tenant(db: &mut PgClient, name: &str) -> Uuid {
    let tenant_id = Uuid::new_v4();
    let name = name.to_owned();
    let n = db
        .tenant_tx(tenant_id, move |tx| {
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
    assert_eq!(n, 1, "INSERT INTO tenants");
    tenant_id
}

pub async fn employee(db: &mut PgClient, tenant_id: Uuid, name: &str) -> Uuid {
    let id = Uuid::new_v4();
    let name = name.to_owned();
    let n = db
        .tenant_tx(tenant_id, move |tx| {
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

pub async fn soft_delete_employee(db: &mut PgClient, tenant_id: Uuid, id: Uuid) {
    let n = db
        .tenant_tx(tenant_id, move |tx| {
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
