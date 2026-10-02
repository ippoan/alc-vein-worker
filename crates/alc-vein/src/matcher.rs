//! 指静脈の特徴量の検査・テンプレートの組み立て・1:N 照合 (DB を持たない部分)。
//!
//! 照合の本体は `vein-match-search` (XGComApi.dll と一致させた実装) で、ここは
//! サーバーでの使い方 (level / mode / 人数の上限) を 1 か所に決めるだけ。
//! オフライン照合 (alc-app 同梱の wasm = `vein-match-wasm`) と食い違わないよう、
//! テンプレートの mode と pad は wasm の `create_template_b64` / `get_enroll_template_b64`
//! と同じ (mode 1 = AES のみ・pad 空) にしてある。

use base64::{engine::general_purpose::STANDARD, Engine};
use vein_match::feature::MAX_HEX_CHARS;
use vein_match::{DecodeError, PLANE_LEN};
use vein_match_search::{create_template, fv_search_user, Library};

/// `fv_search_user` の level (照合の閾値)。DLL (`FV_SearchUser`) は 1..=5 の外を 2 として
/// 扱う (vein-match の `search_level`) ので、その既定の 2 を使う。オフライン照合 (wasm) も
/// 同じ値で呼ぶこと。
pub const SEARCH_LEVEL: i32 = 2;

/// テンプレートの組み立て mode。1 = AES のみ (`import_temp_b64` で読み戻せる。wasm と同じ)。
pub const TEMPLATE_MODE: u8 = 1;

/// 1:N の上限。`Library::new(n)` は `1 < n <= 500` (DLL と同じ)。分割して照合すると
/// 学習と MRU (検索順) が DLL と変わりうるので、超えたら分割せずにエラーにする。
pub const MAX_TEMPLATES: usize = 500;

/// 特徴量 (端末が `VEIN CHARA <hex>` で返したもの) を受け付けられない理由。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CharaError {
    /// 空・先頭が "BDBD" でない (外側の層・base64 等)。
    UnsupportedFormat,
    /// 奇数長・16 進でない文字を含む。
    InvalidHex,
    /// 3000 文字を超える (DLL の FV_CharaMatch と同じ上限)。
    TooLong,
    /// 0xBDBD だが版・CheckNum・大きさが合わない。
    Invalid,
}

impl CharaError {
    /// API の `error` に載せる機械可読なコード。
    pub fn code(self) -> &'static str {
        match self {
            CharaError::UnsupportedFormat => "unsupported_chara_format",
            CharaError::InvalidHex => "invalid_chara_hex",
            CharaError::TooLong => "chara_too_long",
            CharaError::Invalid => "invalid_chara",
        }
    }

    /// API の `message` に載せる説明。
    pub fn message(self) -> &'static str {
        match self {
            CharaError::UnsupportedFormat => {
                "未対応の形式です (先頭が BDBD の 16 進の特徴量だけを受け付けます)"
            }
            CharaError::InvalidHex => "特徴量の 16 進が壊れています (奇数長か 16 進でない文字)",
            CharaError::TooLong => "特徴量が長すぎます (16 進で 3000 文字まで)",
            CharaError::Invalid => "特徴量の中身が壊れています (版・検査和・大きさが合わない)",
        }
    }
}

fn decode_error(e: DecodeError) -> CharaError {
    match e {
        DecodeError::TooLong => CharaError::TooLong,
        DecodeError::Unsupported => CharaError::UnsupportedFormat,
        DecodeError::InvalidHex | DecodeError::BufferTooSmall => CharaError::InvalidHex,
    }
}

/// 16 進の特徴量をバイト列にし、`vein_match::load` が読めることまで確かめる。
/// 16 進の扱いは vein-match の `decode_hex` (wasm と同じ) に任せる。
pub fn decode_chara(hex: &str) -> Result<Vec<u8>, CharaError> {
    let mut buf = [0u8; MAX_HEX_CHARS / 2];
    let n = vein_match::decode_hex(hex.as_bytes(), &mut buf).map_err(decode_error)?;
    let mut plane = [0u8; PLANE_LEN];
    // "BDBD" で始まる 16 進なので外側の層 (Unsupported) には当たらず、失敗は版・CheckNum 等だけ
    vein_match::load(&buf[..n], n as u32, &mut plane).map_err(|_| CharaError::Invalid)?;
    Ok(buf[..n].to_vec())
}

/// 登録用の特徴量 (1..=6 件) から 1 件のテンプレート (base64) を組み立てる
/// (`FV_CreateVeinTemp` 相当)。件数が範囲外なら vein-match の戻り値 (-3) を返す。
pub fn enroll_template(charas: &[Vec<u8>]) -> Result<String, i32> {
    let refs: Vec<&[u8]> = charas.iter().map(Vec::as_slice).collect();
    create_template(&refs, 0, 0, TEMPLATE_MODE, &[]).map(|t| STANDARD.encode(t))
}

/// 1:N で当たった利用者。
#[derive(Debug, PartialEq, Eq)]
pub struct Hit {
    /// 渡したテンプレートの並びでの位置。
    pub index: usize,
    /// 学習後のテンプレート (base64)。
    pub learned: String,
}

/// [`identify`] の結果。
#[derive(Debug, PartialEq, Eq)]
pub struct Identify {
    pub hit: Option<Hit>,
    /// 読み込めずに照合から外したテンプレートの位置 (壊れた行を呼び出し側が記録する)。
    pub unreadable: Vec<usize>,
}

/// 人数が [`MAX_TEMPLATES`] を超えている。
#[derive(Debug, PartialEq, Eq)]
pub struct TooManyTemplates(pub usize);

/// `n` 人の登録で 1:N を組めるか。照合 ([`identify`]) と登録 (PUT) の両方がこれで判定する
/// (登録で上限を超えさせると、その瞬間にテナント全員の照合が止まるため)。
pub fn check_capacity(n: usize) -> Result<(), TooManyTemplates> {
    if n > MAX_TEMPLATES {
        return Err(TooManyTemplates(n));
    }
    Ok(())
}

/// テナントの全テンプレートで `Library` を組み、`chara` (検査済みの 0xBDBD 構造体) を
/// 1:N で照合する。当たれば学習 (`t8` = 今の unix 秒の下位 8 ビット) し、学習後の
/// テンプレートを取り出す。0〜1 人でも動くよう `Library` は `max(件数, 2)` 人ぶんで組む。
pub fn identify(templates: &[&str], chara: &[u8], t8: u8) -> Result<Identify, TooManyTemplates> {
    check_capacity(templates.len())?;
    let mut lib = Library::new(templates.len().max(2)).expect("2..=500 人は Library::new の範囲内");
    let mut unreadable = Vec::new();
    for (i, t) in templates.iter().enumerate() {
        if lib.import_temp_b64(i as u32 + 1, t) != 0 {
            unreadable.push(i);
        }
    }
    let (r, _) = fv_search_user(&mut lib, chara, SEARCH_LEVEL, Some(t8));
    let hit = (r > 0).then(|| {
        let index = (r - 1) as usize;
        // 当たった利用者には記録があり (get_enroll_plain が None にならない)、mode 1 の
        // 組み立ては失敗しない (EncodeUnsupported は mode の bit1/bit2 だけ) ので取り出せる
        let learned = lib
            .get_enroll_template(index, TEMPLATE_MODE, &[])
            .and_then(Result::ok)
            .expect("当たった利用者の学習後テンプレートは取り出せる");
        Hit {
            index,
            learned: STANDARD.encode(learned),
        }
    });
    Ok(Identify { hit, unreadable })
}

/// テスト用の合成の特徴量。実機の特徴量 (vein-match の固定データ) は private repo の
/// ものなので public のこの repo には置かず、0xBDBD 構造体 (magic・版 2・CheckNum・
/// 120×72 の packed bits) を種から組み立てる。平面は左端から右端へ伸びる 8 本の線
/// (静脈のような曲線) で、同じ種なら同じ平面になる。DB を使う統合テストからも使う。
pub mod synth {
    use vein_match::{HEIGHT, REC_LEN, WIDTH};

    /// 種 `seed` の合成の特徴量 (0x448 バイト)。
    pub fn chara(seed: u64) -> Vec<u8> {
        let (w, h) = (WIDTH as usize, HEIGHT as usize);
        let mut s = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let mut plane = vec![0u8; w * h];
        for _ in 0..8 {
            let mut y = (next() % (h as u64 - 8)) as i64 + 4;
            for x in 0..w {
                y = (y + (next() % 3) as i64 - 1).clamp(1, h as i64 - 2);
                plane[y as usize * w + x] = 1;
                plane[(y as usize + 1) * w + x] = 1;
            }
        }
        let mut rec = vec![0u8; REC_LEN];
        rec[0] = 0xbd;
        rec[1] = 0xbd;
        rec[3] = 2;
        rec[8] = WIDTH;
        rec[9] = HEIGHT;
        for (i, px) in plane.iter().enumerate() {
            rec[0x10 + i / 8] |= px << (7 - i % 8);
        }
        rec[2] = rec[4..].iter().fold(0u8, |a, &v| a.wrapping_add(v));
        rec
    }

    /// 端末が送ってくる形 (大文字の 16 進)。
    pub fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02X}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enrolled(seed: u64) -> String {
        enroll_template(&[synth::chara(seed), synth::chara(seed)]).unwrap()
    }

    #[test]
    fn decode_chara_accepts_synthetic_bdbd() {
        let c = synth::chara(1);
        assert_eq!(decode_chara(&synth::hex(&c)).unwrap(), c);
        // 小文字の "bdbd" も受ける (vein-match の decode_hex と同じ)
        assert_eq!(decode_chara(&synth::hex(&c).to_lowercase()).unwrap(), c);
    }

    #[test]
    fn decode_chara_rejects_format_errors() {
        assert_eq!(decode_chara(""), Err(CharaError::UnsupportedFormat));
        assert_eq!(decode_chara("9911AABB"), Err(CharaError::UnsupportedFormat));
        assert_eq!(decode_chara("BDBD0"), Err(CharaError::InvalidHex));
        assert_eq!(decode_chara("BDBDZZ"), Err(CharaError::InvalidHex));
        assert_eq!(
            decode_chara(&format!("BDBD{}", "0".repeat(3000))),
            Err(CharaError::TooLong)
        );
        // 0xBDBD だが CheckNum が合わない
        let mut c = synth::chara(1);
        c[2] = c[2].wrapping_add(1);
        assert_eq!(decode_chara(&synth::hex(&c)), Err(CharaError::Invalid));
    }

    #[test]
    fn chara_error_codes_and_messages_are_distinct() {
        let all = [
            CharaError::UnsupportedFormat,
            CharaError::InvalidHex,
            CharaError::TooLong,
            CharaError::Invalid,
        ];
        let codes: std::collections::HashSet<_> = all.iter().map(|e| e.code()).collect();
        let messages: std::collections::HashSet<_> = all.iter().map(|e| e.message()).collect();
        assert_eq!((codes.len(), messages.len()), (4, 4));
        assert!(CharaError::UnsupportedFormat
            .message()
            .contains("未対応の形式"));
    }

    #[test]
    fn enroll_template_rejects_bad_counts() {
        assert_eq!(enroll_template(&[]), Err(-3));
        assert_eq!(enroll_template(&vec![synth::chara(1); 7]), Err(-3));
    }

    #[test]
    fn enrolled_template_reads_back_with_import_temp_b64() {
        // mode 1 で組み立てたテンプレートが import_temp_b64 で読み戻せる (0 = 成功)
        let t = enrolled(1);
        let mut lib = Library::new(2).unwrap();
        assert_eq!(lib.import_temp_b64(1, &t), 0);
        assert_eq!(lib.counts()[0], 2);
    }

    #[test]
    fn identify_with_zero_templates_misses() {
        let got = identify(&[], &synth::chara(1), 5).unwrap();
        assert_eq!(
            got,
            Identify {
                hit: None,
                unreadable: vec![]
            }
        );
    }

    #[test]
    fn identify_with_one_template_hits_and_learns() {
        let t = enrolled(1);
        let got = identify(&[&t], &synth::chara(1), 5).unwrap();
        let hit = got.hit.unwrap();
        assert_eq!(hit.index, 0);
        // 学習後のテンプレートも import_temp_b64 で読み戻せる
        let learned = hit.learned;
        assert_ne!(learned, t, "学習で学習記録が足され、テンプレートが変わる");
        let mut lib = Library::new(2).unwrap();
        assert_eq!(lib.import_temp_b64(1, &learned), 0);
    }

    #[test]
    fn identify_with_two_templates_finds_the_right_one() {
        let (a, b) = (enrolled(1), enrolled(2));
        let got = identify(&[&a, &b], &synth::chara(2), 5).unwrap();
        assert_eq!(got.hit.unwrap().index, 1);
        let got = identify(&[&a, &b], &synth::chara(1), 5).unwrap();
        assert_eq!(got.hit.unwrap().index, 0);
    }

    #[test]
    fn identify_misses_an_unknown_finger() {
        let (a, b) = (enrolled(1), enrolled(2));
        let got = identify(&[&a, &b], &synth::chara(3), 5).unwrap();
        assert_eq!(got.hit, None);
    }

    #[test]
    fn identify_skips_unreadable_templates() {
        let a = enrolled(1);
        let got = identify(&["not-base64!", &a], &synth::chara(1), 5).unwrap();
        assert_eq!(got.unreadable, vec![0]);
        assert_eq!(got.hit.unwrap().index, 1);
    }

    #[test]
    fn check_capacity_allows_up_to_500() {
        assert_eq!(check_capacity(0), Ok(()));
        assert_eq!(check_capacity(MAX_TEMPLATES), Ok(()));
        assert_eq!(
            check_capacity(MAX_TEMPLATES + 1),
            Err(TooManyTemplates(501))
        );
    }

    #[test]
    fn identify_rejects_more_than_500_templates() {
        let a = enrolled(1);
        let many = vec![a.as_str(); MAX_TEMPLATES + 1];
        assert_eq!(
            identify(&many, &synth::chara(1), 5),
            Err(TooManyTemplates(501))
        );
        // ちょうど 500 人は組める
        let got = identify(&many[..MAX_TEMPLATES], &synth::chara(1), 5).unwrap();
        assert!(got.hit.is_some());
    }
}
