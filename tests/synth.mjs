// crates/alc-vein/src/matcher.rs の `synth` (合成の特徴量) の JS 写し。実機の特徴量は
// private repo のものなので、0xBDBD 構造体 (magic・版 2・CheckNum・120×72 の packed bits)
// を種から組み立てる。同じ種なら Rust 側と同じバイト列になる。
const W = 120;
const H = 72;
const REC_LEN = 0x448;
const MASK = (1n << 64n) - 1n;

export function chara(seed) {
  let s = ((BigInt(seed) * 0x9e3779b97f4a7c15n) & MASK) | 1n;
  const next = () => {
    s ^= (s << 13n) & MASK;
    s ^= s >> 7n;
    s ^= (s << 17n) & MASK;
    return s;
  };
  const plane = new Uint8Array(W * H);
  for (let i = 0; i < 8; i++) {
    let y = Number(next() % BigInt(H - 8)) + 4;
    for (let x = 0; x < W; x++) {
      y = Math.min(Math.max(y + Number(next() % 3n) - 1, 1), H - 2);
      plane[y * W + x] = 1;
      plane[(y + 1) * W + x] = 1;
    }
  }
  const rec = new Uint8Array(REC_LEN);
  rec[0] = 0xbd;
  rec[1] = 0xbd;
  rec[3] = 2;
  rec[8] = W;
  rec[9] = H;
  plane.forEach((px, i) => {
    rec[0x10 + (i >> 3)] |= px << (7 - (i % 8));
  });
  rec[2] = rec.slice(4).reduce((a, v) => (a + v) & 0xff, 0);
  return rec;
}

// 端末が送ってくる形 (大文字の 16 進)。
export function hex(bytes) {
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0").toUpperCase()).join("");
}
