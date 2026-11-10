#!/bin/bash
# staging の alc-vein 用 DB の起動 (Refs ippoan/rust-alc-api#691)。ディスクは揮発なので毎回空の DB から立ち上げる:
#   initdb → init_local_db.sql → 空の _sqlx_migrations → migrations (1 ファイル 1 トランザクション、sqlx migrate と同じ)
#   → local_app_grants.sql (本番の alc_api_app の GRANT の写し、ippoan/rust-alc-api#689) → テナント漏れテストの種
#   → PgBouncer (6432) を前面で起動
# PgBouncer は最後に起動するので、Worker (VeinDb) から 6432 に繋がった時点で DB は準備済み。
set -euo pipefail

# initdb は root で動かない。Container の実行ユーザーが image の USER と違っても動くよう、
# root で起動されたら postgres に降りて自分を実行し直す
if [ "$(id -u)" = 0 ]; then
  mkdir -p "$PGDATA" && chown postgres:postgres "$PGDATA"
  exec gosu postgres "$0" "$@"
fi
trap 'echo "[vein-db] failed at line $LINENO (exit $?)" >&2' ERR

t0=$(date +%s%3N)
log() { echo "[vein-db +$(($(date +%s%3N) - t0))ms] $*"; }

initdb -D "$PGDATA" -U postgres --auth=trust --no-sync >/dev/null
# Cloudflare Containers には /var/run/postgresql (既定の unix socket の置き場) も /dev/shm も無い
pg_ctl -D "$PGDATA" -w -s -o "-c listen_addresses=127.0.0.1 -c unix_socket_directories=/tmp -c fsync=off" start
log "postgres started"

psql() { PGOPTIONS="-c client_min_messages=warning" command psql -q -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -U postgres -d postgres "$@"; }

# ここから ALTER ROLE … LOGIN までの順は crates/alc-vein/tests/embedded/mod.rs の migrate と同じ (順を変えたらもう一方も)
psql -f /vein/init_local_db.sql
# この image は sqlx ではなく psql で流すので表が無い。160 の `migration_status()` が本体で参照するため
# 空の表だけ用意する。中身は入れないので、この DB では `migration_status()` は適用 0 件を返す
psql -c "CREATE TABLE IF NOT EXISTS alc_api._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL)"
{
  for f in /vein/migrations/*.sql; do
    printf 'BEGIN;\n\\i %s\nCOMMIT;\n' "$f"
  done
} | psql
log "migrations applied ($(ls /vein/migrations/*.sql | wc -l) files)"

psql -f /vein/local_app_grants.sql
# RLS が効く alc_api_app で繋がせる (init_local_db.sql では NOLOGIN)。認証は trust
psql -c "ALTER ROLE alc_api_app LOGIN"

# テナント漏れテスト (tests/tenant-leak.mjs) の種。値は run-local.sh の既定と同じ
psql -v tenant=0a000000-0000-4000-8000-00000000000a -v name=leak-a -v employees=7 -f /vein/seed.sql
psql -v tenant=0b000000-0000-4000-8000-00000000000b -v name=leak-b -v employees=13 -f /vein/seed.sql
# alc-app の staging e2e が使う CI の dev token (auth-worker の `POST /api/ci/staging-token`) の会社と利用者。
# 値は auth-worker の wrangler.toml `[env.staging.vars]` の CI_TOKEN_TENANT_ID / CI_TOKEN_SUBJECT / CI_TOKEN_EMAIL /
# CI_TOKEN_ROLE と同じ (片方だけ変えない)。無いと作成系の e2e が tenants / users への外部キーで落ちる
# (Refs ippoan/ci-dashboard#520)。DB は起動のたびに空なので、ここで毎回入れる
psql <<'SQL'
INSERT INTO alc_api.tenants (id, name) VALUES ('11111111-1111-1111-1111-111111111111', 'ci-staging') ON CONFLICT (id) DO NOTHING;
INSERT INTO alc_api.users (id, tenant_id, email, name, role)
VALUES ('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111', 'admin@example.com', 'CI staging', 'admin')
ON CONFLICT (id) DO NOTHING;
SQL
log "grants + seed applied"

echo '"alc_api_app" ""' >/tmp/pgbouncer-userlist.txt
log "starting pgbouncer on :6432 (pool_mode=transaction)"
exec pgbouncer /etc/pgbouncer/pgbouncer.ini
