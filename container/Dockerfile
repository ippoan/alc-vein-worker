# staging の alc-vein Worker が繋ぐ DB (Cloudflare Containers、Refs ippoan/rust-alc-api#691 / ippoan/rust-alc-api#680)。
# postgres + PgBouncer (transaction mode) を 1 つの Container に入れる。
#
# 本番の経路 (Supabase のプーラー 6543、transaction mode) と同じく「トランザクションごとに
# サーバー側のコネクションが使い回される」条件を作るため、Worker は postgres ではなく
# PgBouncer (6432) にだけ繋ぐ。Container へ届くのは Durable Object (`VeinDb`) の
# getTcpPort() 経由だけで、外からは届かない。
#
# ディスクは揮発 (sleep 後の再起動で image の状態に戻る) ので、毎回の起動で空の DB から
# init_local_db.sql → migrations → local_app_grants.sql (本番の GRANT の写し、ippoan/rust-alc-api#689) を流す
# (ippoan/rust-alc-api の staging/entrypoint.sh と同じ方式)。build context は repo の直下 (wrangler.toml の
# image_build_context)。持ち込むファイルは Dockerfile.dockerignore で絞る。
#
# SQL (init_local_db.sql・local_app_grants.sql・migrations/) はこの repo に写しを置かず、正本の
# ippoan/alc-migrations から版を固定して取る: build の前に `bash scripts/fetch-migrations.sh` が
# container/ALC_MIGRATIONS_REV の rev を container/.alc-migrations/ に取り出す (無いと下の COPY が落ちる)。
FROM postgres:17

RUN apt-get update \
    && apt-get install -y --no-install-recommends pgbouncer \
    && rm -rf /var/lib/apt/lists/*

# 認証は trust (ippoan/rust-alc-api の staging/Dockerfile.postgres と同じ)。postgres も PgBouncer も Container の
# 外へは出ず、到達経路は VeinDb の getTcpPort() だけなので、パスワードを持たない。
ENV POSTGRES_HOST_AUTH_METHOD=trust
# VOLUME (/var/lib/postgresql/data) に頼らず、揮発のディスク上の普通のディレクトリに置く
ENV PGDATA=/tmp/vein-pgdata

COPY container/.alc-migrations/scripts/init_local_db.sql /vein/init_local_db.sql
COPY container/.alc-migrations/scripts/local_app_grants.sql /vein/local_app_grants.sql
COPY container/.alc-migrations/migrations /vein/migrations
COPY tests/seed.sql /vein/seed.sql
COPY container/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini
COPY container/start.sh /vein/start.sh
RUN chmod +x /vein/start.sh && chown -R postgres:postgres /vein /etc/pgbouncer

# start.sh が root なら gosu で postgres に降りる (どちらのユーザーで起動されても動く)
USER postgres
EXPOSE 6432
CMD ["/vein/start.sh"]
