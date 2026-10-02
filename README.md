# alc-vein-worker

指静脈 (vein) の 4 本の口 (crates/alc-vein) を workers-rs + tokio-postgres で提供する Cloudflare Worker `alc-vein`。
rust-alc-api を Cloudflare Workers へ段階移行する最初の 1 本 (Refs ippoan/rust-alc-api#680 / ippoan/rust-alc-api#683 / ippoan/rust-alc-api#691) で、
backend (ippoan/rust-alc-api) の `workers/vein/` と `crates/alc-vein/` をこの repo へ分けた (Refs ippoan/rust-alc-api#721)。
口・照合・trait・SQL (`repo::sql`) は alc-vein (`crates/alc-vein`) をそのまま使い、
この Worker が持つのは repo 実装 (`src/repo.rs`)・DB への経路 (`src/db.rs`)・staging の DB を抱える
Durable Object (`src/vein_db.rs`) と workers-rs への載せ方
(`src/lib.rs`) だけ。

## 配置

| 場所 | 中身 |
|---|---|
| 直下 (`Cargo.toml`・`wrangler.toml`・`src/`) | Worker 本体 (package `alc-vein-worker`、wasm32-unknown-unknown)。workspace の root で、`Cargo.lock` はここの 1 つだけ |
| `crates/alc-vein/` | route の crate (口・照合 `matcher`・trait・SQL の定数)。DB 実装は持たない |
| `container/` | staging の DB の image (postgres + PgBouncer)。SQL は ippoan/alc-migrations から取る (下の「migration の取り方」) |
| `scripts/` | 公開範囲の検査 (`check-exposure.sh` と陰性対照 `check-exposure-test.sh`)、`fetch-migrations.sh` |
| `tests/` | テナント漏れテスト・測定 (staging / ローカル向け) |
| `.github/workflows/` | `ci.yml` (検査) / `deploy.yml` (デプロイ) / `tag-release.yml` (本番用のタグ) |

## 依存の取り方

- **`alc-core-wasm`** は ippoan/rust-alc-api (public) に残る。直下の `Cargo.toml` の `[workspace.dependencies]` に
  **git 依存・rev 固定で 1 か所だけ**書き、Worker と `crates/alc-vein` は `workspace = true` で継承する。
  出どころが 2 つになると `TenantId` が別の型になり、**コンパイルは通るのに全リクエストが 500** になる
  (layer が入れる型と route が取り出す型が合わない)。確かめ方:

  ```bash
  cargo tree -i alc-core-wasm --target wasm32-unknown-unknown   # 出どころが 1 つだけ
  ```
- **`vein-match` / `vein-match-search`** は private repo ippoan/vein-match への git 依存 (tag 固定)。取得に GitHub の認証が要る:
  ローカルは `gh auth setup-git` 済みであること、CI は cargo を打つ job の checkout 直後に
  `ippoan/ci-workflows/.github/actions/private-git-auth` (GitHub App の token で git の URL を書き換える)。
  **cargo を打つ job を足すときはこの step も足す。**
- rev を上げる手順は `CLAUDE.md`。

## デプロイ

| きっかけ | 行き先 | workflow |
|---|---|---|
| pull_request | `wrangler deploy --dry-run` (本番と `--env staging`) だけ | `deploy.yml` |
| main への push (= PR のマージ) | staging (`alc-vein-staging`、`--tag staging-<短い SHA>`) | `deploy.yml` |
| タグ `v*` の push | **本番** (`alc-vein`、`--tag <タグ> --message <SHA>`) | `deploy.yml` |

本番のタグは Actions の **Tag Release** (`tag-release.yml`、workflow_dispatch) を手動で打つ。マージで自動のタグは付かない。
応答ヘッダー `x-worker-version` / `x-worker-tag` で、どの版が応えたか分かる。Cloudflare の token は org の secret を使う
(repo 単位の secret を作らない)。

## DB への経路 (`src/db.rs` の 1 か所で出し分ける)

上から順に見て、最初にあったものを使う (Cloudflare の DB 接続プールは使わない — staging で同じ経路を
通せないため。ippoan/rust-alc-api#691)。1・2 とローカルは間に **transaction mode のプーラー** (PgBouncer) が入る。3 は接続文字列の
host:port へ繋ぐだけで、宛先の形 (プーラーか直接か) は接続文字列しだい (repo の RLS はトランザクション単位なので、どちらでも動く作り)。

| 順 | env | 読むもの | 経路 |
|---|---|---|---|
| 1 | staging (`--env staging`、**一時**、ippoan/rust-alc-api#695) | Workers VPC の binding `VEIN_DB_VPC` (VPC Service 型、TCP) | Worker → 既存の Cloudflare Tunnel → 運用者の Linux 機の docker (`127.0.0.1:6432` にだけ bind。手元で動かす `container/` の image 内の PgBouncer) |
| 2 | staging (fallback、ippoan/rust-alc-api#691) | Durable Object の binding `VEIN_DB` | Worker → `VeinDb` へ TCP (`Stub::connect`) → Container の 6432 (PgBouncer、`container/`) へ中継。`VEIN_DB_VPC` を外して deploy するとこちらに戻る |
| 3 | 本番 (トップレベル) | Secrets Store の binding `VEIN_DATABASE_URL` (secret の名前 `alc-app-database-url-rt`。backend と同じ実行用ロールの接続文字列。Refs ippoan/auth-worker#605) | 接続文字列の host:port へ Worker の TCP (STARTTLS、常に TLS) |
| 4 | ローカル | 文字列 `DATABASE_URL` (worker 自身の secret / `wrangler dev --var`) | 接続文字列の host:port へ STARTTLS。`sslmode=disable` + var `ALLOW_INSECURE_DB=1` のときだけ手元の PgBouncer へ平文 |

どれも無ければ 503 (`database_not_configured`)。`src/db.rs` は binding (`VEIN_DB_VPC` → `VEIN_DB`) を接続文字列
(`VEIN_DATABASE_URL` → `DATABASE_URL`) より先に見る。どちらの binding も
平文 (trust 認証) なので本番 (トップレベル) に置かないことを `scripts/check-exposure.sh` が検査する。
`ALLOW_INSECURE_DB` はローカル専用 (`wrangler dev --var` /
`.dev.vars`) で、読むのは 4 の段だけ。無ければ `sslmode=disable` でも TLS を強制する。wrangler.toml の vars に書かないことを
`scripts/check-exposure.sh` が検査する。

### 本番の接続文字列の入れ直し

- GCP Secret Manager の secret `alc-app-database-url-rt` を、secret 管理の MCP で Cloudflare の Secrets Store へ
  コピーする (値は人にも LLM にも見えない)。Worker は毎リクエスト読むので、**次の要求から効く (deploy は要らない)**。
- **binding `VEIN_DATABASE_URL` が在るのに読めない (型が違う / 取得に失敗 / 値が無い) ときは 500** (`internal_error`) で、
  4 の `DATABASE_URL` へは戻らない (古い secret へ黙って戻らないため)。4 へ落ちるのは binding が無いときだけ。
- worker 自身の secret `DATABASE_URL` (`wrangler secret put`) は、ローカル (`tests/run-local.sh`) 専用の経路として残る。
  ローカルの `wrangler dev` は **`--env local`** (binding を持たない env。deploy しない) で立てる — `--env` なしだと、トップレベルの `VEIN_DATABASE_URL` が「在るが読めない」に見えて全リクエストが 500 になる。
  本番に残っている古い worker secret は、Secrets Store への切り替えを確かめた後に消す。

## 到達面

- **本番の到達経路は auth-worker からの Service Binding だけ。** JWT を検証せず `X-Tenant-ID` を
  信頼するので、トップレベルは `workers_dev` / `preview_urls` を false にし、`route` / `routes` を持たない。
- **staging はテストから叩くため `workers_dev = true`**
  (URL は `wrangler deploy --env staging` の出力を見る)。**この workers.dev は Cloudflare Access で保護する前提**
  (アプリ名・ポリシー・service token は親タスク / 運用側が Access に設定する。repo には持たない)。
  Access を通らないリクエストは Worker に届かず、Access がログインへの 302 か 403 を返す。
  テストは service token を `CF-Access-Client-Id` / `CF-Access-Client-Secret` で付ける。
- `workers_dev = true` を許すのは `env.staging` だけ。`scripts/check-exposure.sh` が CI で毎回これを検査し、
  `scripts/check-exposure-test.sh` が陰性対照 (wrangler.toml を崩すと exit 1) を回す。
- **`VeinDb` の `connect` ハンドラは Worker の `Stub::connect` (DO binding `VEIN_DB`) からしか呼べない。**
  DO も Container も外部から直接届く口は無い (Container の 6432 は `getTcpPort()` 経由だけ)。
- **staging の手元 DB (ippoan/rust-alc-api#695) に届く口は VPC binding `VEIN_DB_VPC` だけ。** ホストの 6432 は `127.0.0.1` にだけ
  bind し、Tunnel の ingress / public hostname / CIDR route には出さない。VPC Service 型は宛先を 1 host:port に
  固定するので、Worker からホストの他のポートへは届かない (VPC Networks 型は Tunnel の先の網全体に届くので使わない)。

## RLS

**repo のメソッド 1 回 = 1 トランザクション。** `BEGIN` の中で
`set_config('app.current_tenant_id', $1, true)` を打つ (プーラーはトランザクション単位で
コネクションを使い回すので、session スコープの `set_current_tenant` は使えない)。
**repo の DB 操作はすべて `in_tenant_tx` を通し、`Row` / `Statement` をトランザクションの外へ出さない**
(戻り値は印 `TxOutput` の付いた owned 型に限るので、`Row` を返すとコンパイルが通らない)。
`Row` は prepared statement を握っていて、COMMIT 後に drop すると Close がトランザクションの外に出て
別のサーバー接続へ回り、`prepared statement "s1" already exists` (42P05) になる (staging で実測)。
詳細は `src/repo.rs`。

## 接続ロールを返す口 `GET /internal/db-role` (Refs ippoan/auth-worker#605)

**目的**: この Worker の DB 接続がどのロールで繋がっているかを確かめる。vein が触る表には
FORCE ROW LEVEL SECURITY の無いものがあり、表の所有者で繋ぐと RLS が掛からない。実行用ロール
(`alc_api_rt`、非所有者) で繋いでいるかを、auth-worker の MCP tool (`verify_rls`) が Service Binding で
1 回呼んで確かめる。

- **引数なし。** path・query・header・body を読まない。同じ接続に `SELECT current_user` を 1 文流すだけ
  (書き込み・`SET`・トランザクションなし。テナントを取らないので `in_tenant_tx` は通さない)。
  `simple_query` で流すので prepared statement を作らず、上の 42P05 は起きない
- **返す値** (200):

  ```json
  { "current_user": "alc_api_rt", "is_runtime_role": true }
  ```

  `is_runtime_role` は `current_user` が `alc_api_rt` と一致するか。接続文字列・ホスト・DB 名・版は返さない
- **失敗**: 問い合わせ (`SELECT current_user`) のエラーは 500 `{"error":"internal_error"}` で、DB のエラー文は
  応答にもログにも出さない (ログは固定の 1 行だけ)。**接続の失敗はこの口より手前**で、ほかの口と同じく
  `fetch` が扱う — 応答は 500 `{"error":"internal_error"}` か 503 `{"error":"database_not_configured"}`、
  原因は今までどおり `fetch` が Workers のログに出す
- **`/api` の外・tenant ヘッダーの layer の外に置く** (`src/lib.rs` の `router` で素にだけ merge する)。
  auth-worker の proxy がブラウザ・端末から vein に転送するのは `/api/vein` で始まる path だけなので、
  `/internal/db-role` には proxy からは届かず、auth-worker のコードが binding で直接呼んだときだけ届く。
  `/api/internal/db-role` に route は無く、ほかの未定義の path と同じ扱い (tenant ヘッダー無しは 401、
  有りは 404)。**path を `/api/…` や `/vein/…` に変えないこと**
- `router` では **この口の Router を alc-vein の Router より先に merge する** (axum の `merge` は fallback を
  後から merge した側で置き換える。後に置くと、未定義の path の fallback から tenant の layer が外れる)
- staging は workers.dev が Access 配下で開いているので、Access を通る者はこの口を呼べる
  (返るのは staging の設定に直書きのロール名だけ)

## staging の DB (手元の docker + Workers VPC、一時、ippoan/rust-alc-api#695)

Container 経路は止まった後の cold start (約 3.5 秒)・置き場所が `APAC` までしか絞れない (往復 60〜140ms)・
起動のたびの migration が重いので、**一時的に** staging の DB を運用者の Linux 機の docker に置く。
Hyperdrive は使わない (ippoan/rust-alc-api#680)。

- DB は `container/` の image をそのまま使う (build context は repo の直下)。systemd --user の unit が
  `docker rm -f` → `docker run --rm -p 127.0.0.1:6432:6432` で毎回作り直す (start.sh は既存の PGDATA があると
  落ちるので restart policy・volume は使わない)。**ポートは必ず `127.0.0.1` に bind する** (PgBouncer は
  trust 認証で、届けば全テナントを読める)
- Worker からの到達口は **VPC Service (TCP 型、宛先 = そのホストの `127.0.0.1:6432` だけ)** の binding だけ。
  既存の Tunnel の ingress・public hostname・CIDR route には出さない (VPC Service は ingress 不要)。
  宛先は Service 側で固定なので、`connect()` に渡すアドレスは名目だけ
- wrangler.toml に書くのは VPC Service の ID だけ。Tunnel の ID・ホスト名・account ID・IP は repo に書かない
  (Service は `wrangler vpc service create <名前> --type tcp --tunnel-id … --ipv4 127.0.0.1 --tcp-port 6432` で作る)
- **wrangler は 4.78.0 以上**を使う (TCP 型の VPC Service は 4.78.0 から。手元の 4.58 には `--type tcp` が無い)。
  CI (`deploy.yml`) の版は `WRANGLER_VERSION`

```bash
bash scripts/fetch-migrations.sh            # Container の image が COPY する SQL を取る (下の「migration の取り方」)
npx wrangler@4.144.0 deploy --env staging   # 4.78.0 以上なら可
```
- **Worker の実行場所は `[env.staging.placement] region` で DB の近く (Tunnel の繋がる関西) に固定する。**
  指定しないとリクエストが入った colo (実測で SIN) で動き、DB の往復ごとに海を越えて一覧の p50 が 944ms になる
  (固定後は入口が SIN でも `cf-placement: remote-KIX` で動き、DB 部分は connect 37ms + db 120ms)
- 本番 (トップレベルの `[placement]`) は `aws:ap-northeast-1` (東京)。DB の近くで動かす。
  staging と同じくヒントであり、実際に動いた場所は応答ヘッダー `cf-placement` で分かる
- DB は止まらないので cold start は無い。ディスクは揮発のまま (unit の再起動で空の DB から作り直す)

**終わりの条件** — 次のどれかが来たら、`[[env.staging.vpc_services]]`・`src/db.rs` の `connect_vpc`・VPC Service・
systemd の unit・コンテナ・image を消し、staging を Container 経路 (または本番と同じ経路) に戻す:

- 本番の DB 接続が決まり、staging をそれに揃えるとき
- Workers VPC が有料化されるとき
- この Linux 機を止めるとき

## staging の DB (Cloudflare Containers、fallback)

`container/`: postgres 16 + PgBouncer (`pool_mode = transaction`、サーバー側 4 本、
`max_prepared_statements = 0`)。**ディスクは揮発**なので、起動のたびに空の DB から
alc-migrations の `scripts/init_local_db.sql` → `migrations/` → `scripts/local_app_grants.sql` (本番の GRANT の写し) →
テナント漏れテストの種 (`tests/seed.sql`) を流し、最後に PgBouncer を起動する。

migration は sqlx ではなく psql で 1 ファイルずつ流すので、sqlx の適用履歴の表 `alc_api._sqlx_migrations` が無い。
migration 160 の `migration_status()` が本体でその表を参照するため、`container/start.sh` が migration の前に**空の表だけ**用意する
(中身は入れないので、この DB では `migration_status()` は適用 0 件を返す。vein は呼ばない)。

### migration の取り方

image が COPY する SQL (`init_local_db.sql`・`local_app_grants.sql`・`migrations/`) は、この repo に写しを置かず、
正本の ippoan/alc-migrations (public) から**版を固定して**取る。

- 版は `container/ALC_MIGRATIONS_REV` の 1 行 (commit の SHA)。**rev を書くのはこのファイルだけ**
- `bash scripts/fetch-migrations.sh` がその rev を `container/.alc-migrations/` (`.gitignore` 済み) に取り出す。
  既に同じ rev が在れば何もしない
- **docker build の前に必ず通す** (CI・`wrangler deploy --env staging`・手元で image を作るとき)。通していないと
  Dockerfile の COPY が落ちる

```bash
bash scripts/fetch-migrations.sh
docker build -f container/Dockerfile -t vein-db .     # build context は repo の直下
```

- `VeinDb` は Container が止まっていれば起動し、PgBouncer が応答するまで StartupMessage を送り直して待つ
  (`getTcpPort().connect()` は listen 前でも「開く」ので、応答が返ったかで判定する。上限 90 秒)
- **常時起動にしない**: 最後の接続が閉じて 10 分で alarm が Container を止める。次のリクエストは
  cold start (Container 起動 + migration) になる
- Cloudflare Containers には `/var/run/postgresql` も `/dev/shm` も無い (unix socket は `/tmp` に置く)
- 既定だと北米に置かれて往復ごとに太平洋を越えるので `constraints.regions = ["APAC"]`

```bash
bash scripts/fetch-migrations.sh
wrangler deploy --env staging    # docker で container/ を build して push する
```

## ビルド

```bash
cargo install worker-build@0.8.7 --locked
worker-build --release
```

## 検査 (CI の `ci.yml` と同じもの)

```bash
bash scripts/check-exposure.sh && bash scripts/check-exposure-test.sh
cargo fmt --check
cargo clippy --target wasm32-unknown-unknown --release -- -D warnings
cargo test -p alc-vein          # crates/alc-vein の unit test (routes と matcher)
```

## テナント漏れテスト / 測定

`tests/tenant-leak.mjs` は 2 テナント (A / B) のリクエストを並列に交互に投げ、全レスポンスが
自テナントの件数・ID と完全一致することを数える。worker は **RLS が効く `alc_api_app` で**繋ぐこと
(superuser だと RLS を素通りして全部通ってしまう)。

staging (VPC 経路でも Container 経路でも、種は DB の起動時に `container/start.sh` が入れている):

```bash
VEIN_URL="<wrangler deploy --env staging が出す URL>" \
  CF_ACCESS_CLIENT_ID=... CF_ACCESS_CLIENT_SECRET=... \
  TENANT_A=0a000000-0000-4000-8000-00000000000a TENANT_B=0b000000-0000-4000-8000-00000000000b \
  N_A=7 N_B=13 node tests/tenant-leak.mjs     # Access のヘッダー無し・値違いが 302/403 になることも数える
```

ローカル (接続文字列は repo に書かず環境変数で渡す。`container/` の image をそのまま DB に使える —
image を作る前に `bash scripts/fetch-migrations.sh`):

```bash
PG_ADMIN_URL=... APP_DB_URL=... bash tests/run-local.sh     # tenant-leak.mjs (A/B 各 200 本、同時 20)
PG_ADMIN_URL=... APP_DB_URL=... bash tests/bench-local.sh   # /vein/identify の CPU 時間 (100/500/1000/5000 件)
```

DB は alc-migrations の `scripts/init_local_db.sql` + `migrations/` を流したもの。`alc_api_app` は `NOLOGIN` で
作られるので、ローカルでは `ALTER ROLE alc_api_app LOGIN PASSWORD '...'` が要る。
