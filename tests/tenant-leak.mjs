// テナント漏れテスト (Refs #680 / #683 / #691)。alc-vein (`wrangler dev` または staging) に、2 テナント (A / B) の
// リクエストを並列に交互に大量に投げ、全レスポンスが自テナントの件数・ID と完全一致する
// (他テナントの行が 1 つも混ざらない、かつ行ゼロでもない) ことを確かめる。
//
// 前提: seed.sql で A / B のテナントと乗務員を作ってある (run-local.sh が面倒を見る)。
// worker は RLS が効く alc_api_app で繋いでいること (superuser だと RLS を素通りする)。
//
//   VEIN_URL=http://127.0.0.1:8787 TENANT_A=<uuid> TENANT_B=<uuid> N_A=7 N_B=13 \
//     node tests/tenant-leak.mjs
//
// staging (Container の PgBouncer 経由、workers.dev は Cloudflare Access で保護) へは Access の
// service token を渡す。全リクエストに CF-Access-Client-Id / CF-Access-Client-Secret を付け、
// 加えて「Access のヘッダー無し・値違いは Worker に届かない (Access が 302 / 403 を返す)」も数える:
//
//   VEIN_URL="<wrangler deploy --env staging が出す URL>" \
//     CF_ACCESS_CLIENT_ID=... CF_ACCESS_CLIENT_SECRET=... \
//     TENANT_A=0a000000-0000-4000-8000-00000000000a TENANT_B=0b000000-0000-4000-8000-00000000000b \
//     N_A=7 N_B=13 node tests/tenant-leak.mjs
//
// SQL は RLS に加えて `WHERE tenant_id = $1` も持つので、GUC が別テナントへ漏れると
// 「他テナントの行」ではなく「行ゼロ」か 500 として現れる。だから件数が 0 でないことと
// 全件が 200 であることも数える。
import { createHash } from "node:crypto";
import { chara, hex } from "./synth.mjs";

const env = (k, d) => {
  const v = process.env[k] ?? d;
  if (v === undefined) {
    console.error(`${k} が要る`);
    process.exit(2);
  }
  return v;
};
const URL_ = env("VEIN_URL");
const TENANTS = {
  A: { id: env("TENANT_A"), n: Number(env("N_A")), seedBase: 0 },
  B: { id: env("TENANT_B"), n: Number(env("N_B")), seedBase: 100000 },
};
const PER_TENANT = Number(env("PER_TENANT", "200"));
const ACCESS_ID = process.env.CF_ACCESS_CLIENT_ID;
const ACCESS_SECRET = process.env.CF_ACCESS_CLIENT_SECRET;
if (Boolean(ACCESS_ID) !== Boolean(ACCESS_SECRET)) {
  console.error("CF_ACCESS_CLIENT_ID と CF_ACCESS_CLIENT_SECRET は両方要る");
  process.exit(2);
}
const gate = ACCESS_ID
  ? { "CF-Access-Client-Id": ACCESS_ID, "CF-Access-Client-Secret": ACCESS_SECRET }
  : {};
const CONCURRENCY = Number(env("CONCURRENCY", "20"));

// seed.sql の md5(tenant || '-' || g)::uuid と同じ
const employeeId = (tenant, g) => {
  const h = createHash("md5").update(`${tenant}-${g}`).digest("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
};
for (const t of Object.values(TENANTS)) {
  t.employees = Array.from({ length: t.n }, (_, i) => employeeId(t.id, i + 1));
  t.expected = new Set(t.employees);
}

// 管理者のヘッダー (auth-worker が付ける X-User-*)。3 つ揃わないと AuthUser が復元されず、承認系の口は 403
const ADMIN = {
  "X-User-ID": "ad000000-0000-4000-8000-0000000000ad",
  "X-User-Email": "admin@example.invalid",
  "X-User-Role": "admin",
};

async function call(method, path, tenant, body, extra = {}) {
  const res = await fetch(`${URL_}${path}`, {
    method,
    headers: { ...gate, ...extra, "X-Tenant-ID": tenant, "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }
  return { status: res.status, json, timing: res.headers.get("server-timing") };
}

async function pool(tasks, n) {
  const results = new Array(tasks.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: n }, async () => {
      while (next < tasks.length) {
        const i = next++;
        results[i] = await tasks[i]();
      }
    }),
  );
  return results;
}

const failures = [];
const stats = {};
const count = (key, ok, detail) => {
  stats[key] ??= { total: 0, ok: 0 };
  stats[key].total++;
  if (ok) stats[key].ok++;
  else if (failures.length < 20) failures.push({ key, ...detail });
};

// 1. 登録 (worker の PUT = upsert。RLS の INSERT も通る)。A と B を交互に並べる
const enroll = [];
for (let i = 0; i < Math.max(TENANTS.A.n, TENANTS.B.n); i++) {
  for (const [name, t] of Object.entries(TENANTS)) {
    if (i >= t.n) continue;
    const c = hex(chara(t.seedBase + i + 1));
    enroll.push(async () => {
      const r = await call("PUT", `/vein/templates/${t.employees[i]}`, t.id, { charas: [c, c] });
      count(`enroll ${name}`, r.status === 200 && r.json?.employee_id === t.employees[i], r);
    });
  }
}
await pool(enroll, CONCURRENCY);

// 1b. 登録は承認待ち: 承認するまで一覧に出ない。承認は管理者だけ・版を固定・自テナントの行だけ
const approval = [];
for (const [name, t] of Object.entries(TENANTS)) {
  approval.push(async () => {
    const r = await call("GET", "/vein/templates", t.id);
    count(`list ${name} before approval (0)`, r.status === 200 && r.json?.templates?.length === 0, r);
  });
}
await pool(approval, CONCURRENCY);
const pending = {};
for (const [name, t] of Object.entries(TENANTS)) {
  const none = await call("GET", "/vein/templates/pending", t.id);
  count(`pending ${name} without admin header (403)`, none.status === 403, none);
  const r = await call("GET", "/vein/templates/pending", t.id, undefined, ADMIN);
  const ids = (r.json ?? []).map((x) => x.employee_id);
  count(
    `pending ${name} (expect ${t.n})`,
    r.status === 200 && ids.length === t.n && ids.every((id) => t.expected.has(id)),
    r,
  );
  pending[name] = r.json ?? [];
  const body = { updated_at: pending[name][0]?.updated_at };
  const denied = await call("POST", `/vein/templates/${t.employees[0]}/approve`, t.id, body);
  count(`approve ${name} without admin header (403)`, denied.status === 403, denied);
}
// 他テナントの行は見えない: A の管理者が B の乗務員を承認しようとしても 409 (B の版を知っていても)
for (const [name, t, other, otherName] of [
  ["A", TENANTS.A, TENANTS.B, "B"],
  ["B", TENANTS.B, TENANTS.A, "A"],
]) {
  const body = { updated_at: pending[otherName][0]?.updated_at };
  const r = await call("POST", `/vein/templates/${other.employees[0]}/approve`, t.id, body, ADMIN);
  count(`cross-tenant approve ${name}→other (409)`, r.status === 409, r);
}
const approve = [];
for (const [name, t] of Object.entries(TENANTS)) {
  for (const p of pending[name]) {
    approve.push(async () => {
      const r = await call("POST", `/vein/templates/${p.employee_id}/approve`, t.id, { updated_at: p.updated_at }, ADMIN);
      count(`approve ${name} (204)`, r.status === 204, r);
    });
  }
}
await pool(approve, CONCURRENCY);
for (const [name, t] of Object.entries(TENANTS)) {
  const r = await call("GET", "/vein/templates/pending", t.id, undefined, ADMIN);
  count(`pending ${name} after approval (0)`, r.status === 200 && r.json?.length === 0, r);
}

// 2. 他テナントの乗務員へは登録も削除もできない (404)
const cross = [];
for (const [name, t, other] of [
  ["A", TENANTS.A, TENANTS.B],
  ["B", TENANTS.B, TENANTS.A],
]) {
  const c = hex(chara(1));
  cross.push(async () => {
    const r = await call("PUT", `/vein/templates/${other.employees[0]}`, t.id, { charas: [c] });
    count(`cross-tenant PUT ${name}→other (404)`, r.status === 404, r);
  });
  cross.push(async () => {
    const r = await call("DELETE", `/vein/templates/${other.employees[0]}`, t.id);
    count(`cross-tenant DELETE ${name}→other (404)`, r.status === 404, r);
  });
}
// tenant ヘッダーが無い・壊れている → 401 (alc-core-wasm の require_tenant_header)
for (const [label, tenant] of [["missing", undefined], ["invalid", "not-a-uuid"]]) {
  cross.push(async () => {
    const res = await fetch(`${URL_}/vein/templates`, {
      headers: tenant === undefined ? gate : { ...gate, "X-Tenant-ID": tenant },
    });
    count(`tenant header ${label} (401)`, res.status === 401, { status: res.status });
  });
}
// staging: Access の service token が無い・違うリクエストは、tenant が正しくても Worker に届かない
// (Access がログインへの 302 か 403 を返す。Worker の応答 = JSON や 401/404 は失敗)
if (ACCESS_ID) {
  for (const [label, headers] of [
    ["missing", {}],
    ["wrong", { "CF-Access-Client-Id": ACCESS_ID, "CF-Access-Client-Secret": `${ACCESS_SECRET}x` }],
  ]) {
    for (const [method, path] of [
      ["GET", "/vein/templates"],
      ["POST", "/vein/identify"],
      ["PUT", `/vein/templates/${TENANTS.A.employees[0]}`],
      ["DELETE", `/vein/templates/${TENANTS.A.employees[0]}`],
    ]) {
      cross.push(async () => {
        const res = await fetch(`${URL_}${path}`, {
          method,
          redirect: "manual",
          headers: { ...headers, "X-Tenant-ID": TENANTS.A.id, "content-type": "application/json" },
          body: method === "POST" || method === "PUT" ? "{}" : undefined,
        });
        await res.text();
        count(`access token ${label} ${method} (302/403 by Access)`, res.status === 302 || res.status === 403, {
          status: res.status,
          location: res.headers.get("location"),
        });
      });
    }
  }
}
await pool(cross, CONCURRENCY);

// 3. 一覧を A / B 交互に大量に並列で
const list = [];
for (let i = 0; i < PER_TENANT; i++) {
  for (const [name, t] of Object.entries(TENANTS)) {
    list.push(async () => {
      const r = await call("GET", "/vein/templates", t.id);
      const ids = (r.json?.templates ?? []).map((x) => x.employee_id);
      const foreign = ids.filter((id) => !t.expected.has(id));
      const ok =
        r.status === 200 &&
        ids.length === t.n &&
        ids.length > 0 &&
        foreign.length === 0 &&
        new Set(ids).size === t.n;
      count(`list ${name} (expect ${t.n})`, ok, { status: r.status, got: ids.length, foreign: foreign.length });
    });
  }
}
await pool(list, CONCURRENCY);

// 4. 照合を A / B 交互に: 自テナントの指は当たり、他テナントの指は外れる
const ident = [];
for (let i = 0; i < PER_TENANT; i++) {
  for (const [name, t, other] of [
    ["A", TENANTS.A, TENANTS.B],
    ["B", TENANTS.B, TENANTS.A],
  ]) {
    if (i % 2 === 0) {
      const k = (i / 2) % t.n;
      const c = hex(chara(t.seedBase + k + 1));
      ident.push(async () => {
        const r = await call("POST", "/vein/identify", t.id, { chara: c });
        count(`identify ${name} own finger (hit)`, r.status === 200 && r.json?.employee_id === t.employees[k], r);
      });
    } else {
      const k = ((i - 1) / 2) % other.n;
      const c = hex(chara(other.seedBase + k + 1));
      ident.push(async () => {
        const r = await call("POST", "/vein/identify", t.id, { chara: c });
        count(`identify ${name} other tenant's finger (miss)`, r.status === 200 && r.json?.employee_id === null, r);
      });
    }
  }
}
await pool(ident, CONCURRENCY);

// 5. 他テナントの削除の試みで B / A の件数が減っていない
for (const [name, t] of Object.entries(TENANTS)) {
  const r = await call("GET", "/vein/templates", t.id);
  count(`list ${name} after cross-tenant attempts`, r.status === 200 && r.json?.templates?.length === t.n, r);
}

let total = 0;
let ok = 0;
console.log(`tenant-leak: VEIN_URL=${URL_} per_tenant=${PER_TENANT} concurrency=${CONCURRENCY}`);
for (const [k, v] of Object.entries(stats)) {
  total += v.total;
  ok += v.ok;
  console.log(`  ${v.ok === v.total ? "OK  " : "FAIL"} ${k}: ${v.ok}/${v.total}`);
}
console.log(`tenant-leak: ${ok}/${total} matched`);
if (failures.length) console.log("failures (first 20):", JSON.stringify(failures, null, 1));
// 1 件も走っていない (= skip) のは失敗扱い
if (total === 0 || ok !== total || stats[`list A (expect ${TENANTS.A.n})`]?.total !== PER_TENANT) {
  process.exit(1);
}
