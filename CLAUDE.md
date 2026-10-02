# alc-vein-worker

指静脈 (vein) の Cloudflare Worker `alc-vein` と、その route の crate `alc-vein`。
backend (ippoan/rust-alc-api) から分けた repo (Refs ippoan/rust-alc-api#721)。構造は `.claude/skills/alc-vein-worker-map`、詳細は `README.md`。

- 直下 = Worker 本体 (workers-rs + tokio-postgres、wasm32-unknown-unknown。workspace の root)
- `crates/alc-vein/` = 口 4 本・照合・trait・SQL の定数 (DB 実装は持たない)
- `container/` = staging の DB の image。SQL は ippoan/alc-migrations から取る

## コマンド

```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh   # 公開範囲の検査と陰性対照
cargo fmt --check
cargo clippy --target wasm32-unknown-unknown --release -- -D warnings
cargo test -p alc-vein                                                  # unit test (routes と matcher)
cargo llvm-cov --locked -p alc-vein --text > /tmp/alc-vein-cov.txt && bash scripts/check_coverage_100.sh --use-cache /tmp/alc-vein-cov.txt   # coverage 100% の gate
worker-build --release                                                  # worker-build 0.8.7
bash scripts/fetch-migrations.sh                                        # docker build の前に必ず
docker build -f container/Dockerfile -t vein-db .
npx wrangler@4.144.0 deploy --dry-run [--env staging]                   # 配信しない
# 実 DB の検査 (上の image を空きポートで立ててから。README の「実 DB の検査」)
VEIN_TEST_DATABASE_URL=... cargo test -p alc-vein --test sql_db -- --ignored   # repo::sql の定数と RLS
APP_DB_URL=... bash tests/run-local.sh                                         # worker を通したテナント漏れテスト
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
- **public repo。** ホスト名・IP・account ID・Tunnel ID・project ref・テナント ID・メール・接続文字列の実物を、
  コード・コメント・commit・PR に書かない (`wrangler.toml` に既に在る binding 用の ID は別)。
- **タグ `v*` = 本番。** main へのマージは staging に出るだけ。本番は Actions の Tag Release を手動で打つ
  (マージで自動のタグは付けない)。手で `v*` のタグを push しない。
- DB 操作は `in_tenant_tx` を通す (1 メソッド = 1 トランザクション、戻り値は `TxOutput`)。SQL は `crates/alc-vein` の `repo::sql` の 1 か所。
- **実 DB の検査と coverage の gate を弱めない。** `crates/alc-vein/tests/sql_db.rs` は接続先 (`VEIN_TEST_DATABASE_URL`) が無ければ失敗する作り
  (skip にしない)。`repo::sql` の定数や `in_tenant_tx` の頭の文を変えたら、このテストを実 DB で通す。
  `coverage_100.toml` の 3 ファイル (`matcher.rs`・`routes.rs`・`repo.rs`) は行カバレッジ 100% (登録を外さない)。
- **staging の DB を動かしている機では、`127.0.0.1:6432` を常駐のコンテナが使っている** (README)。手元の検査の DB は別名・空きポート
  (`-p 127.0.0.1::6432`) で立て、常駐のコンテナには繋がない・止めない。

## rev を上げる手順

- **`alc-core-wasm`** (ippoan/rust-alc-api): 直下の `Cargo.toml` の `[workspace.dependencies]` の `rev` を変え (**書くのはここ 1 か所だけ**。
  `crates/alc-vein/Cargo.toml` や `[dependencies]` に git / path を書かない)、`cargo update -p alc-core-wasm` で `Cargo.lock` を一緒に更新する。
  その後 `cargo tree -i alc-core-wasm --target wasm32-unknown-unknown` で出どころが 1 つだけであることを確かめる
  (2 つになると、コンパイルは通るのに全リクエストが 500 になる)。
- **alc-migrations** (staging の DB の SQL): `container/ALC_MIGRATIONS_REV` の 1 行を変える (**rev を書くのはこのファイルだけ**)。
  rev は ippoan/rust-alc-api の `Cargo.toml` が固定している alc-migrations の rev と揃える。
  `bash scripts/fetch-migrations.sh` → `docker build` → 起動確認 (CI の「staging DB の Container image が起動する」と同じ命令) を手元で通す。
- **vein-match**: `crates/alc-vein/Cargo.toml` の `tag` (2 行とも) と `Cargo.lock`。
