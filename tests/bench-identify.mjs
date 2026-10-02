// `/vein/identify` の CPU 時間の測定 (Refs #680 / #683)。worker は Date.now で
// `Server-Timing: connect;dur=.., db;dur=.., app;dur=..` を返す (db = repo の全メソッドの合計、
// app = 残り = 特徴量の検査・1:N 照合・JSON など Worker の CPU)。ここではそれを直列に R 回集めて
// min / 中央値 / p95 を出す。
//
// 前提: seed.sql で TENANT に N_EMPLOYEES 人の乗務員を作ってある。ENROLL 人ぶん PUT で登録する
// (照合の上限は 500 人なので、それを超える件数は bench-local.sh が SQL で複写して入れる)。
//
//   VEIN_URL=.. TENANT=<uuid> ENROLL=100 R=30 node tests/bench-identify.mjs
import { createHash } from "node:crypto";
import { chara, hex } from "./synth.mjs";

const URL_ = process.env.VEIN_URL;
const TENANT = process.env.TENANT;
const ENROLL = Number(process.env.ENROLL ?? "0");
const R = Number(process.env.R ?? "30");
const SEED_BASE = 200000;

const employeeId = (g) => {
  const h = createHash("md5").update(`${TENANT}-${g}`).digest("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
};

async function call(method, path, body) {
  const res = await fetch(`${URL_}${path}`, {
    method,
    headers: { "X-Tenant-ID": TENANT, "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, json: await res.json().catch(() => null), timing: res.headers.get("server-timing") };
}

// 登録 (並列 20)
let next = 1;
await Promise.all(
  Array.from({ length: 20 }, async () => {
    while (next <= ENROLL) {
      const g = next++;
      const c = hex(chara(SEED_BASE + g));
      const r = await call("PUT", `/vein/templates/${employeeId(g)}`, { charas: [c, c] });
      if (r.status !== 200) throw new Error(`enroll ${g}: ${r.status} ${JSON.stringify(r.json)}`);
    }
  }),
);

const db = [];
const app = [];
const wall = [];
let hits = 0;
let status = null;
for (let i = 0; i < R; i++) {
  // 登録済みの指 (ENROLL が 0 なら他人の指) で照合する
  const g = ENROLL > 0 ? (i % ENROLL) + 1 : 1;
  const t0 = performance.now();
  const r = await call("POST", "/vein/identify", { chara: hex(chara(SEED_BASE + g)) });
  wall.push(performance.now() - t0);
  status = r.status;
  if (r.json?.employee_id === employeeId(g)) hits++;
  const m = Object.fromEntries(
    (r.timing ?? "").split(",").map((s) => {
      const [k, v] = s.trim().split(";dur=");
      return [k, Number(v)];
    }),
  );
  if (m.db !== undefined) db.push(m.db);
  if (m.app !== undefined) app.push(m.app);
}

const q = (xs, p) => {
  if (!xs.length) return "-";
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(p * s.length))].toFixed(0);
};
const fmt = (xs) => `min=${q(xs, 0)} p50=${q(xs, 0.5)} p95=${q(xs, 0.95)}`;
console.log(
  `bench-identify: enrolled=${ENROLL} R=${R} last_status=${status} hits=${hits}/${R} | ` +
    `app_ms ${fmt(app)} | db_ms ${fmt(db)} | wall_ms ${fmt(wall)}`,
);
