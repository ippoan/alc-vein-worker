//! 指静脈 (Waveshare Finger Vein Module) の登録と 1:N 照合をサーバーで回す crate
//! (Refs ippoan/vein-match#20)。
//!
//! 照合の本体は XGComApi.dll と一致させた Rust 実装 `vein-match-search`
//! (private repo ippoan/vein-match を git 依存で tag 固定) にあり、この crate は
//! alc-vein Worker (この repo の直下) から見た窓口になる。登録データ (テンプレート) の正本はサーバーの
//! `vein_templates` (migrations/151)。学習 (テンプレートの更新) はオンライン照合
//! (`POST /vein/identify`) のときだけで、オフライン (alc-app 同梱の wasm) では学習しない。
//!
//! - [`matcher`]: 特徴量の検査・テンプレートの組み立て・1:N 照合 (DB を持たない)
//! - [`repo`]: `vein_templates` の読み書き
//! - [`routes`]: 4 本の口

pub mod matcher;
pub mod repo;
pub mod routes;

use std::sync::Arc;

use crate::repo::VeinTemplatesRepository;

/// vein 用の最小 State。Worker は `routes::tenant_router().with_state(..)` で
/// tenant 系ルートに merge する。repo は trait object で持つ (テストが mock に差し替える)。
#[derive(Clone)]
pub struct VeinState {
    pub templates: Arc<dyn VeinTemplatesRepository>,
}
