#!/usr/bin/env bash
# alc-vein Worker を `wrangler dev` で立て、テナント漏れテスト (tenant-leak.mjs) を回す
# (Refs ippoan/rust-alc-api#680 / ippoan/rust-alc-api#683 / ippoan/rust-alc-api#691)。接続文字列は repo に書かず環境変数で渡す:
#
#   PG_ADMIN_URL  種 (seed.sql) を流す superuser の接続文字列
#   APP_DB_URL    worker が繋ぐ接続文字列 (DATABASE_URL として渡す)。**RLS が効く alc_api_app で**
#                 (superuser は RLS を素通りしてテストが意味を失う)。本番 (Supabase のプーラー) /
#                 staging (Container の PgBouncer) と同じくトランザクション単位でコネクションを
#                 使い回させるなら、PgBouncer の transaction mode を前に置いてそこを指す
#                 (container/ の image をそのまま使える。README 参照)
#
#   bash tests/run-local.sh            # 省略時 PER_TENANT=200 CONCURRENCY=20
set -euo pipefail
cd "$(dirname "$0")/.."

: "${PG_ADMIN_URL:?superuser の接続文字列が要る}"
: "${APP_DB_URL:?alc_api_app の接続文字列が要る}"
PORT="${PORT:-8787}"
export TENANT_A="${TENANT_A:-0a000000-0000-4000-8000-00000000000a}"
export TENANT_B="${TENANT_B:-0b000000-0000-4000-8000-00000000000b}"
export N_A="${N_A:-7}" N_B="${N_B:-13}"

user="$(psql "$APP_DB_URL" -Atc 'SELECT current_user')"
bypass="$(psql "$APP_DB_URL" -Atc 'SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user')"
if [ "$bypass" != "f" ]; then
  echo "APP_DB_URL の $user は RLS を素通りする (superuser / BYPASSRLS)。alc_api_app で繋ぐこと" >&2
  exit 1
fi

psql -q "$PG_ADMIN_URL" -v tenant="$TENANT_A" -v name=leak-a -v employees="$N_A" -f tests/seed.sql
psql -q "$PG_ADMIN_URL" -v tenant="$TENANT_B" -v name=leak-b -v employees="$N_B" -f tests/seed.sql

log="$(mktemp)"
# 本番と同じ口 (DATABASE_URL、src/db.rs) を平文 (sslmode=disable) で手元の DB へ向ける。
# 平文はローカル専用フラグ ALLOW_INSECURE_DB=1 があるときだけ許される (無ければ TLS を強制)
case "$APP_DB_URL" in *sslmode=*) url="$APP_DB_URL" ;; *\?*) url="$APP_DB_URL&sslmode=disable" ;; *) url="$APP_DB_URL?sslmode=disable" ;; esac
npx --yes "wrangler@${WRANGLER_VERSION:-4.143.0}" dev --env local --port "$PORT" --ip 127.0.0.1 --var "DATABASE_URL:$url" --var ALLOW_INSECURE_DB:1 >"$log" 2>&1 &
pid=$!
cleanup() {
  local rc=$?
  kill $pid 2>/dev/null || true
  wait $pid 2>/dev/null || true
  # 失敗したときは worker 側のログ (console_log / panic) を残す
  if [ "$rc" -ne 0 ]; then echo "--- wrangler dev log (tail)"; tail -n 60 "$log"; fi
  if [ -n "${KEEP_LOG:-}" ]; then cp "$log" "$KEEP_LOG"; fi
  rm -f "$log"
}
trap cleanup EXIT
for _ in $(seq 1 300); do
  if curl -s -o /dev/null "http://127.0.0.1:$PORT/vein/templates"; then break; fi
  if ! kill -0 $pid 2>/dev/null; then cat "$log"; exit 1; fi
  sleep 1
done

VEIN_URL="http://127.0.0.1:$PORT" node tests/tenant-leak.mjs
