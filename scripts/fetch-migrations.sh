#!/usr/bin/env bash
# staging の DB の image (container/Dockerfile) が COPY する SQL を、正本の ippoan/alc-migrations (public) から
# 版を固定して取り出す (Refs ippoan/rust-alc-api#721)。写しをこの repo に commit しない。
#
#   - 版は container/ALC_MIGRATIONS_REV の 1 行 (commit の SHA)。**rev を書くのはそのファイルだけ**
#   - 取り出す先は container/.alc-migrations/ (.gitignore 済み)。持ち込むのは Dockerfile が COPY する
#     scripts/init_local_db.sql・scripts/local_app_grants.sql・migrations/ だけ
#   - 既に同じ rev が在れば何もしない
# docker build (CI・`wrangler deploy --env staging`・手元の image 作り) の前に必ず通す。
#
#   bash scripts/fetch-migrations.sh
set -euo pipefail
cd "$(dirname "$0")/.."

REPO_URL="https://github.com/ippoan/alc-migrations"
dest="container/.alc-migrations"
rev="$(tr -d '[:space:]' <container/ALC_MIGRATIONS_REV)"
if ! [[ "$rev" =~ ^[0-9a-f]{40}$ ]]; then
  echo "container/ALC_MIGRATIONS_REV が 40 桁の commit SHA ではない" >&2
  exit 1
fi

if [ -f "$dest/.rev" ] && [ "$(cat "$dest/.rev")" = "$rev" ]; then
  echo "alc-migrations ${rev}: 取得済み (${dest})"
  exit 0
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
git init -q "$tmp/src"
git -C "$tmp/src" fetch -q --depth 1 "$REPO_URL" "$rev"
git -C "$tmp/src" checkout -q FETCH_HEAD
got="$(git -C "$tmp/src" rev-parse HEAD)"
if [ "$got" != "$rev" ]; then
  echo "取り出した commit (${got}) が container/ALC_MIGRATIONS_REV (${rev}) と違う" >&2
  exit 1
fi

mkdir -p "$tmp/out/scripts"
cp "$tmp/src/scripts/init_local_db.sql" "$tmp/src/scripts/local_app_grants.sql" "$tmp/out/scripts/"
cp -R "$tmp/src/migrations" "$tmp/out/migrations"
# .rev は最後に書く (途中で落ちたら次回は取り直す)
echo "$rev" >"$tmp/out/.rev"
rm -rf "$dest"
mv "$tmp/out" "$dest"
echo "alc-migrations ${rev}: $(ls "$dest/migrations"/*.sql | wc -l) 個の migration を ${dest} に取り出した"
