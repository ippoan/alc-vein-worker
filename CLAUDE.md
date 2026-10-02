# alc-vein-worker

指静脈 (vein) の Cloudflare Worker `alc-vein` と、その route の crate `alc-vein`。
backend (ippoan/rust-alc-api) から分けた repo (Refs ippoan/rust-alc-api#721)。構造は `.claude/skills/alc-vein-worker-map`、詳細は `README.md`。

- 直下 = Worker 本体 (workers-rs + tokio-postgres、wasm32-unknown-unknown。workspace の root)
- `crates/alc-vein/` = 口 4 本・照合・trait・SQL の定数・repo の実装 `pg` (tokio-postgres。接続は持たない — 張るのは worker とテスト。テストの DB は process の中で起こす組み込みの PostgreSQL)
- `container/` = staging の DB の image。SQL は ippoan/alc-migrations から取る

## コマンド

```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh   # 公開範囲の検査と陰性対照
cargo fmt --check
cargo clippy --target wasm32-unknown-unknown --release -- -D warnings
bash scripts/fetch-migrations.sh && cargo test -p alc-vein              # unit test (routes と matcher) と tests/sql_db.rs (組み込みの PostgreSQL。docker も env も要らない)
cargo llvm-cov --locked -p alc-vein --text > /tmp/alc-vein-cov.txt && bash scripts/check_coverage_100.sh --use-cache /tmp/alc-vein-cov.txt   # coverage 100% の gate (CI はテストをこの計測の 1 回にまとめる)
worker-build --release                                                  # worker-build 0.8.7
bash scripts/fetch-migrations.sh                                        # テスト・coverage・docker build の前に必ず
docker build -f container/Dockerfile -t vein-db .
npx wrangler@4.144.0 deploy --dry-run [--env staging]                   # 配信しない
bash scripts/fetch-migrations.sh && cargo test -p alc-vein --test sql_db   # repo::sql の定数 (README の「DB の検査」)
# worker を通したテナント漏れテスト (上の image を空きポートで立ててから)
APP_DB_URL=... bash tests/run-local.sh
```

private repo ippoan/vein-match への git 依存がある。ローカルは `gh auth setup-git` 済みであること。

## 規範

- **公開範囲と、その検査を弱めない。** この Worker は JWT を検証せず `X-Tenant-ID` を信頼する。本番の到達経路は
  auth-worker からの Service Binding だけ (`workers_dev` / `preview_urls` = false、route 無し)。
  `scripts/check-exposure.sh` と陰性対照 `check-exposure-test.sh` を緩めない・外さない。
- **`wrangler.toml` の表の順を変えない・前の方に表を足さない。** 陰性対照は `[build]` の初出の前・
  `[env.staging]` 直後の `[env.staging.observability]` という位置に行を挿す作りで、順が変わると検査が意味を失う。
  worker 名・binding・`[version_metadata]` (トップレベルと `env.staging` の両方) も変えない (同じ worker への上書きにするため)。
- **secret・binding・入口 (route) を増やさない。** Cloudflare の token は org の secret を使い、repo 単位の secret を作らない。
  本番の DB は Hyperdrive の binding `VEIN_HYPERDRIVE` (トップレベルにだけ。設定は共有で worker ごとに作らない)。
  **`env.*` の下に `hyperdrive` を置かない** (本番の DB へ届くため。`check-exposure.sh` が検査)。平文の DB binding (`vpc_services` 等) を本番に置かないのも同じ検査。
- **public repo。** ホスト名・IP・account ID・Tunnel ID・project ref・テナント ID・メール・接続文字列の実物を、
  コード・コメント・commit・PR に書かない (`wrangler.toml` に既に在る binding 用の ID は別)。
- **タグ `v*` = 本番。** main へのマージは staging に出るだけ。本番は Actions の Tag Release を手動で打つ
  (マージで自動のタグは付けない)。手で `v*` のタグを push しない。
- DB 操作は共通 crate `alc-worker-db` (ippoan/alc-worker-kit) の `PgClient::tenant_tx` を通す (1 メソッド = 1 トランザクション、戻り値は `TxOutput`)。
  SQL は `crates/alc-vein` の `repo::sql` の 1 か所、流すのは `crates/alc-vein/src/pg.rs` の 1 か所。
  **名前付き prepared statement を呼ぶコード (`execute`・`query`・`query_one`・`query_opt`・`prepare`) を足さない** — 使うのは
  `TenantTx` の `query_typed`・`query_typed_one`・`query_typed_opt`・`execute_typed` だけ (Hyperdrive 経由では名前付きの文で接続が切れる)。
  生の `tokio_postgres::Client` を `src/db.rs` の外へ出さない。
- **DB の検査と coverage の gate を弱めない。** `crates/alc-vein/tests/sql_db.rs` は、テストの中で起こす組み込みの PostgreSQL
  (`pglite-oxide`) に流す。migration が未取得 (`container/.alc-migrations` が無い) なら失敗する作り
  (skip にしない・`#[ignore]` にしない)。`repo::sql` の定数・`pg.rs` の引数の型・`alc-worker-db` の rev を変えたら、このテストを通す。
  CI は本数を固定して回す (`ci.yml` の `7 passed`。テストを減らさない)。CI がテストを走らせるのは coverage の計測の 1 回だけ
  (素の `cargo test` の step は無い — dev-dependency を 2 回 compile しないため。step を分けて計測から外さない)。
  `coverage_100.toml` の 4 ファイル (`matcher.rs`・`routes.rs`・`repo.rs`・`pg.rs`) は行カバレッジ 100% (登録を外さない)。
  RLS が実際に行を止めることの確かめは ippoan/alc-migrations の CI (`ci/check_rls_rows.sql`) が持ち、この repo では確かめない
  (RLS だけを見ていた 1 本を外して 8 本 → 7 本にしたのは、同じ確かめが alc-migrations に在ることを示したうえでのオーナーの決定。Refs ippoan/rust-alc-api#727)。
- **`pglite-oxide` は `=0.5.0` に固定** (`crates/alc-vein/Cargo.toml` の dev-dependency)。wasmer 系 13 crate は `Cargo.lock` で
  alpha 版に pin している (Rust 1.92.0 で build が通る版)。lock を作り直すときは pin し直す (手順は README の「lock の pin」)。
  本番の wasm に入らないこと (`cargo tree --target wasm32-unknown-unknown -e normal` に pglite / wasmer が 0 行) を崩さない。
- **staging の DB を動かしている機では、`127.0.0.1:6432` を常駐のコンテナが使っている** (README)。手元の検査の DB は別名・空きポート
  (`-p 127.0.0.1::6432`) で立て、常駐のコンテナには繋がない・止めない。

## rev を上げる手順

- **`alc-core-wasm`** (ippoan/rust-alc-api): 直下の `Cargo.toml` の `[workspace.dependencies]` の `rev` を変え (**書くのはここ 1 か所だけ**。
  `crates/alc-vein/Cargo.toml` や `[dependencies]` に git / path を書かない)、`cargo update -p alc-core-wasm` で `Cargo.lock` を一緒に更新する。
  その後 `cargo tree -i alc-core-wasm --target wasm32-unknown-unknown` で出どころが 1 つだけであることを確かめる
  (2 つになると、コンパイルは通るのに全リクエストが 500 になる)。
- **`alc-worker-db`** (ippoan/alc-worker-kit): `alc-core-wasm` と同じ。直下の `Cargo.toml` の `[workspace.dependencies]` の `rev` の
  1 か所だけを変え、`cargo update -p alc-worker-db` で `Cargo.lock` を一緒に更新し、`cargo tree -i alc-worker-db --target wasm32-unknown-unknown`
  で出どころが 1 つだけであることを確かめる (`worker`・`tokio-postgres` も版が 1 つのままであること)。その後、
  `bash scripts/fetch-migrations.sh && cargo test -p alc-vein --test sql_db` を通す。
- **alc-migrations** (staging の DB と、テストの組み込みの PostgreSQL の SQL): `container/ALC_MIGRATIONS_REV` の 1 行を変える (**rev を書くのはこのファイルだけ**)。
  rev は ippoan/rust-alc-api の `Cargo.toml` が固定している alc-migrations の rev と揃える。
  `bash scripts/fetch-migrations.sh` → `cargo test -p alc-vein` → `docker build` → 起動確認 (CI の「staging DB の Container image が起動する」と同じ命令) を手元で通す。
- **vein-match**: `crates/alc-vein/Cargo.toml` の `tag` (2 行とも) と `Cargo.lock`。
