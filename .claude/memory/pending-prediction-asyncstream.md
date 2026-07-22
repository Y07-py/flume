---
name: pending-prediction-asyncstream
description: 検証済み(2026-07-22)— 素の AsyncStream にはバックプレッシャーがないことをスパイクで実測。結論は DESIGN.md の ADR D6 に起票済み
metadata:
  type: project
---

**検証済み(2026-07-22)。** 予想は的中: 素の `AsyncStream`(デフォルト `.unbounded`)にはバックプレッシャーがなく、遅いコンシューマに対してプロデューサが先行完走して全アイテムがバッファに滞留する(スパイク `Sources/spike-backpressure` で実測)。`AsyncChannel` は `await send` がサスペンドするため footprint フラットを確認。キャンセル伝播も確認済み。

**Why:** Flume の憲法「メモリはデータ総量に比例しない」の成立条件を確定させるため。

**How to apply:** 結論と証拠は DESIGN.md の **ADR D6**(Channel の契約は「送信がサスペンドする有界チャネル」)が正。今後 Channel 実装の議論はこのメモリではなく D6 を参照すること。付録 A の `Channel` スケッチも D6 準拠に更新済み。
