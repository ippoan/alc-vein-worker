-- テナント漏れテスト・測定用の種 (superuser で流す。テナント・乗務員だけを作り、
-- vein_templates は worker の PUT で入れる)。何度流しても同じ状態に戻る。
--   psql -v tenant=<uuid> -v name=<text> -v employees=<件数> -f seed.sql
\set ON_ERROR_STOP on
-- scripts/init_local_db.sql + migrations だけのローカル DB では、employees (migration 001 の表)
-- に alc_api_app の GRANT が無い (migrations に GRANT ON ALL TABLES が無く、テストは superuser で
-- 繋ぐので表に出ない)。worker は alc_api_app で繋ぐので、vein が読む employees の SELECT だけ付ける
-- (vein_templates は migration 151 で GRANT 済み)。
GRANT SELECT ON alc_api.employees TO alc_api_app;
DELETE FROM alc_api.vein_templates WHERE tenant_id = :'tenant';
DELETE FROM alc_api.employees WHERE tenant_id = :'tenant';
INSERT INTO alc_api.tenants (id, name) VALUES (:'tenant', :'name') ON CONFLICT (id) DO NOTHING;
INSERT INTO alc_api.employees (id, tenant_id, name)
SELECT
    -- 乗務員 ID はテナントと番号から決まる (テストが期待値を組み立てられるように)
    md5(:'tenant' || '-' || g)::uuid,
    :'tenant',
    :'name' || '-' || g
FROM generate_series(1, :employees) AS g;
