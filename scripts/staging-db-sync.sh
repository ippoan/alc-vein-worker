#!/usr/bin/env bash
# staging の DB (運用者の Linux 機の docker `vein-db-695`、README「staging の DB (手元の docker + Workers VPC)」) を、
# この repo の main の `container/` (= `container/ALC_MIGRATIONS_REV` を含む) と `tests/seed.sql` に追従させる
# (Refs ippoan/ci-dashboard#520)。systemd --user の timer (`container/systemd/vein-db-695-sync.timer`) が定期に呼ぶ。
#
#   1. main の先頭を取り、image の入力 (`container/` の tree と `tests/seed.sql` の blob) から鍵を作る
#   2. 鍵が image の label `org.ippoan.vein-db.inputs` と同じで、動いているコンテナがその image なら何もしない
#   3. 違えば、main の中身を一時ディレクトリに取り出し、fetch-migrations.sh → docker build (label 付き、tag は :candidate) →
#      候補を別名のコンテナで起こして DB が立つか確かめる → 立てば tag を付け替えて `systemctl --user restart vein-db-695`
#      → PgBouncer 越しに応答するまで待つ
#
# **restart で DB は空から作り直される** (ディスクは揮発)。staging の vein / lineworks / trouble / measurements 等が
# この DB を共有しているので、作り直すのは入力が変わったときだけにする。build や候補の起動が落ちたら restart せず、前の image のまま動かす。
#
#   bash scripts/staging-db-sync.sh            # 追従する (要れば build と restart)
#   bash scripts/staging-db-sync.sh --check    # 見るだけ。追従済みなら exit 0、ずれていれば exit 10
#   bash scripts/staging-db-sync.sh --install  # この script と unit を ~/.local・~/.config/systemd/user に置き、timer を有効にする
set -euo pipefail

REPO_URL="${VEIN_DB_SYNC_REPO:-https://github.com/ippoan/alc-vein-worker}"
BRANCH="${VEIN_DB_SYNC_BRANCH:-main}"
IMAGE="vein-db-695"
UNIT="vein-db-695.service"
STATE="${VEIN_DB_SYNC_STATE:-${XDG_CACHE_HOME:-$HOME/.cache}/vein-db-695-sync}"
LABEL_INPUTS="org.ippoan.vein-db.inputs"
LABEL_REV="org.ippoan.vein-db.alc-migrations-rev"
LABEL_COMMIT="org.ippoan.vein-db.source-commit"
READY_TIMEOUT_S=180

log() { echo "[vein-db-sync] $*"; }

mode="${1:-sync}"
case "$mode" in
  sync | --check | --install) ;;
  *) echo "usage: $0 [--check | --install]" >&2; exit 2 ;;
esac

if [ "$mode" = "--install" ]; then
  src_dir="$(cd "$(dirname "$0")/.." && pwd)"
  install -D -m 0755 "$src_dir/scripts/staging-db-sync.sh" "$HOME/.local/libexec/vein-db-695-sync.sh"
  for u in vein-db-695.service vein-db-695-sync.service vein-db-695-sync.timer; do
    install -D -m 0644 "$src_dir/container/systemd/$u" "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/$u"
  done
  systemctl --user daemon-reload
  systemctl --user enable --now vein-db-695-sync.timer
  log "installed: ~/.local/libexec/vein-db-695-sync.sh と unit 3 つ。timer を有効にした"
  exit 0
fi

# timer の実行と手動の実行が重ならないように
mkdir -p "$STATE"
exec 9>"$STATE/lock"
if ! flock -n 9; then
  log "別の同期が実行中なので抜ける"
  exit 0
fi

# main の先頭を取る (浅い clone を使い回す)
if [ ! -d "$STATE/repo/.git" ]; then
  git init -q "$STATE/repo"
fi
git -C "$STATE/repo" fetch -q --depth 1 "$REPO_URL" "$BRANCH"
commit="$(git -C "$STATE/repo" rev-parse FETCH_HEAD)"
# image の入力: container/ (Dockerfile・start.sh・pgbouncer.ini・ALC_MIGRATIONS_REV) と tests/seed.sql。
# どれかが変われば鍵が変わる。commit の SHA を鍵にしないのは、関係の無い merge で DB を作り直さないため
inputs="$(git -C "$STATE/repo" rev-parse "FETCH_HEAD:container" "FETCH_HEAD:tests/seed.sql" | sha256sum | cut -c1-16)"
rev="$(git -C "$STATE/repo" show "FETCH_HEAD:container/ALC_MIGRATIONS_REV" | tr -d '[:space:]')"

image_inputs="$(docker image inspect "$IMAGE" --format "{{index .Config.Labels \"$LABEL_INPUTS\"}}" 2>/dev/null || true)"
image_id="$(docker image inspect "$IMAGE" --format '{{.Id}}' 2>/dev/null || true)"
running_id="$(docker inspect "$IMAGE" --format '{{.Image}}' 2>/dev/null || true)"

need_build=1
[ -n "$image_inputs" ] && [ "$image_inputs" = "$inputs" ] && need_build=0
need_restart=$need_build
[ -z "$running_id" ] || [ "$running_id" != "$image_id" ] && need_restart=1

if [ "$need_build" = 0 ] && [ "$need_restart" = 0 ]; then
  log "追従済み (main ${commit:0:7}、alc-migrations ${rev:0:7}、inputs ${inputs})"
  exit 0
fi
if [ "$mode" = "--check" ]; then
  log "ずれ: main ${commit:0:7} の inputs ${inputs} / image の inputs ${image_inputs:-なし} / 要 build ${need_build}・要 restart ${need_restart}"
  exit 10
fi

if [ "$need_build" = 1 ]; then
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
  git -C "$STATE/repo" archive FETCH_HEAD container scripts/fetch-migrations.sh tests/seed.sql | tar -x -C "$work"
  log "build: main ${commit:0:7}、alc-migrations ${rev:0:7}、inputs ${image_inputs:-なし} → ${inputs}"
  bash "$work/scripts/fetch-migrations.sh"
  docker build -q -f "$work/container/Dockerfile" -t "$IMAGE:candidate" \
    --label "$LABEL_INPUTS=$inputs" --label "$LABEL_REV=$rev" --label "$LABEL_COMMIT=$commit" \
    "$work" >/dev/null
fi

# 候補の image を別名のコンテナで起こし (ポートは出さない)、DB が立つことを先に確かめる。
# start.sh や migration が落ちる image で本物を restart すると、共有の staging DB が止まったままになるため
wait_ready() { # $1 = コンテナ名。PgBouncer 越しに alc_api_app で 1 文通れば migration と種まで済んでいる
  local deadline=$(($(date +%s) + READY_TIMEOUT_S))
  until docker exec "$1" psql -h 127.0.0.1 -p 6432 -U alc_api_app -d postgres -Atqc 'SELECT 1' >/dev/null 2>&1; do
    if [ "$(docker inspect "$1" --format '{{.State.Running}}' 2>/dev/null)" != true ]; then
      log "$1 が起動中に止まった"; return 1
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "$1 が ${READY_TIMEOUT_S} 秒で応答しない"; return 1
    fi
    sleep 2
  done
}

if [ "$need_build" = 1 ]; then
  probe="${IMAGE}-probe"
  docker rm -f "$probe" >/dev/null 2>&1 || true
  # --rm を付けない (落ちたときにログを残す)
  docker run -d --name "$probe" "$IMAGE:candidate" >/dev/null
  if ! wait_ready "$probe"; then
    docker logs --tail 30 "$probe" >&2 || true
    docker rm -f "$probe" >/dev/null 2>&1 || true
    log "候補の image が立たないので restart しない (動いている DB は前の image のまま)"
    exit 1
  fi
  docker rm -f "$probe" >/dev/null
  docker tag "$IMAGE:candidate" "$IMAGE"
  log "候補の image が立つことを確かめた"
fi

log "restart: ${UNIT} (DB は空から作り直される)"
systemctl --user restart "$UNIT"
wait_ready "$IMAGE"
latest="$(docker exec "$IMAGE" sh -c 'ls /vein/migrations | tail -1')"
log "ready: main ${commit:0:7}、alc-migrations ${rev:0:7} (最後の migration ${latest})"
