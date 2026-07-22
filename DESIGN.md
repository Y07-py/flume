# Flume — Design Document

macOS ネイティブのストリーミング・ワークフローエンジン。個人探究プロジェクト。

---

## 1. ビジョン

**「エージェント時代のワークフローエンジンを、macOS ネイティブの一級市民として設計し直したらどうなるか」を確かめる。**

n8n のクローンではない。n8n が証明した「ノードを繋いで自動化を作る」体験を出発点に、
n8n が後付けで苦しんでいる 2 つの領域 — **メモリ効率**と**エージェント実行** — を、
最初から設計の中心に置いたエンジンを Swift でスクラッチする。

### 最終形のイメージ

- Mac 上で動く、軽くて静かなワークフローアプリ。Electron でも WebView でもなく SwiftUI
- ワークフローはローカルの JSON ファイル。実行もローカル完結。クラウド・アカウント不要
- 通常のワークフロー(HTTP → 変換 → 通知)と同じキャンバス上に、
  「LLM が実行時にツールを選んで動くエージェントノード」が違和感なく同居する

### 成功の定義

- 巨大なデータを流しても Activity Monitor のメモリグラフが平らなまま完走するデモがある
- 自作エンジンの上でエージェントがツールを選びながらタスクを完遂する瞬間を見る
- 設計判断(ストリーミングモデル、型の選択、動的グラフ)を自分の言葉で説明できる

---

## 2. 設計思想(3 つの柱)

### 柱 1: 流れる、溜めない

n8n はノードの全出力を実行終了までメモリに抱える。Flume ではノード間を
`AsyncSequence` が繋ぎ、アイテムは 1 件ずつ流れて消費された分から消えていく。
バッファするのは Sort / Aggregate のような「溜める必然があるノード」だけ。
値型 + copy-on-write により、分岐してもデータは実質コピーされない。

> **エンジンの憲法: メモリ使用量はワークフローの幅に比例し、流したデータ総量には比例しない。**

### 柱 2: エージェントはエンジンのプリミティブ

DAG(一方通行のグラフ)を前提にしない。上限付きループと
「ノードが実行時に次の実行対象を決める」動的ディスパッチを最初からコアに持つ。
エージェントとは「LLM ノードがツールノードを選んで回るループ」にすぎない、
という見立てをエンジンの型で表現する。

### 柱 3: エンジンは UI を知らない

コアは純粋な SwiftPM パッケージ。JSON のワークフロー定義を食べて実行するだけの存在。
テストと CLI だけで完全に動作検証できる。UI(キャンバス)は観測者・編集者であり、
エンジンの正しさは UI なしで証明され続ける。

---

## 3. スコープ(作らないもの)

- **ノードカタログの網羅** — n8n の 400+ 連携は追わない。HTTP・変換・ファイル・LLM など 10 個前後の本質的なノードのみ
- **マルチユーザー / サーバー運用 / 認証基盤** — ローカル・シングルユーザー専用
- **最初からの完璧な UI** — キャンバス編集は最後発。長期間「CLI + JSON 定義」で開発する

---

## 4. ドメイン語彙(ユビキタス言語)

「定義の世界」と「実行の世界」を明確に分ける。
n8n はここが曖昧(`Workflow` クラスが両方の顔を持つ)で読みにくさの一因になっている。

### 定義の世界 — 静的・Codable・ファイルに保存される

| 用語 | 定義 |
|------|------|
| **Workflow** | ノードと接続の集合。純粋なデータで、それ自体は何も実行しない |
| **NodeType** | ノードの種類(HTTP Request、Filter など)。入出力ポートの宣言と実行ロジックを持つ。カタログに登録される側 |
| **Node** | Workflow 内に置かれた NodeType のインスタンス。ID・設定値(パラメータ)・キャンバス上の位置を持つ |
| **Port** | ノードの入出力の口。NodeType が静的に宣言する。名前付き(`main`, `true`, `false` など) |
| **Connection** | あるノードの出力ポートから別のノードの入力ポートへの辺。**循環を許す**(ループはエンジンが実行時に上限管理) |

### 実行の世界 — 動的・メモリ上にだけ存在する

| 用語 | 定義 |
|------|------|
| **Run** | Workflow の 1 回の実行。開始〜終了までの状態とログの単位 |
| **Item** | ノード間を流れるデータの最小単位。`value: Value` + 最小限のメタデータ。**「Run はアイテムの流れである」がエンジンの世界観** |
| **Value** | JSON 相当の値型(enum)。バイナリは値としてコピーせず `BinaryRef`(参照)で持つ |
| **Channel** | Connection の実行時の姿。`AsyncStream<Item>`。上流が閉じたら下流に伝播する |
| **RunContext** | 実行中のノードに渡される環境(ログ、キャンセル確認、クレデンシャル取得、将来はツール呼び出しの発行) |

---

## 5. 設計判断記録(ADR)

各判断は「予想 → 実装 → 答え合わせ(n8n の対応実装を読む)」のサイクルで検証する。
答え合わせ欄は該当フェーズ完了時に記入する。

### D1. ノード実行の抽象は二層構造 — 最重要

**判断(採用・仮):**
- エンジンの正式な契約は**ストリーム単位**:
  `run(inputs: [PortName: Channel], outputs: [PortName: Sink], context: RunContext)`。
  集約ノードもループもこれで表現できる
- 大半のノードは 1 アイテム → N アイテムの変換なので、**アイテム単位の簡易プロトコル**
  (`transform(Item) async throws -> [Item]`)を上に被せる。簡易版は自動的にストリーム版に持ち上げる

**理由:** 「コアはストリーミング」という憲法を型で強制しつつ、ノードを書く負担は軽くする。

**答え合わせ(n8n / `packages/core` WorkflowExecute):** _未実施_

### D2. Item は出自(lineage)を持たない

**判断(採用・仮):** `Item` は `value` と生成元ノード ID 程度に留める。

**理由:** n8n の `pairedItem`(アイテムの出自追跡)は式から「対応する元アイテム」を
参照するための仕組みだが、実装が悪名高く複雑。必要性を実感してから足す。
ここは「答え合わせ」が一番面白くなる予想ポイント。

**答え合わせ(n8n の pairedItem 実装):** _未実施_

### D3. エラーもデータフローで表現する

**判断(採用・仮):** デフォルトは fail-fast(エラーで Run 停止)。
ただし語彙として全ノードに暗黙の `error` 出力ポートを予約し、
接続されている場合だけエラーがアイテムとしてそちらへ流れる。

**理由:** n8n の「Continue on Fail」設定より、エラーもデータフローで表現するほうが
Flume の世界観に合う。

**答え合わせ(n8n の Continue on Fail / error output):** _未実施_

### D4. 仕様の本体はゴールデンテスト

**判断(採用・仮):** ワークフロー定義は `schemaVersion` を持つ JSON(Codable)。
フェーズ 1 の成果物は Swift の型定義と同時に「サンプルワークフロー JSON 2〜3 個」。
これがそのままゴールデンテスト(JSON を食わせて期待出力を検証)の入力になる。
プローズの仕様書は最小限にし、テストを仕様の本体とする。

**答え合わせ(n8n のワークフロー JSON スキーマ):** _未実施_

### D5. エージェントへの布石は 2 点だけ確保し、詳細は凍結

**判断(採用・仮):** フェーズ 1 で確保するのは:
1. グラフが循環を許すこと
2. `RunContext` という「ノードがエンジンに何かを依頼する窓口」が存在すること

動的ディスパッチの具体設計はフェーズ 6 まで凍結。今は決めすぎない。

**答え合わせ(n8n の AI Agent ノード実装):** _未実施_

### D6. Channel の契約は「送信がサスペンドする有界チャネル」

**判断(採用):** Flume の `Channel` は自前の型として定義し、契約を次の 2 点とする:

1. `send` は受信側の消費を待ってサスペンドしうる(プロデューサはコンシューマを追い越せない)
2. バッファは有界(メモリ上限 = ワークフローの幅 × チャネル容量)

実装 v1 は swift-async-algorithms の `AsyncChannel`(容量ゼロ=完全ランデブーの特殊ケース)を
自前の型で包んで使う。外部パッケージの型はエンジン全体に漏らさず境界の 1 ファイルに閉じ込める。
スループットが実測で問題になったら有界バッファ版に差し替える(契約は不変)。

**理由(スパイクで実測、2026-07-22、`Sources/spike-backpressure`):**

- **実験 1**: 素の `AsyncStream`(デフォルト `.unbounded`)は、遅いコンシューマに対して
  プロデューサが 100 万件を先行完走し、全アイテムがバッファに滞留
  (footprint 1.6 → 74 MB に増加後、消費に伴い減少)。**憲法「メモリはデータ総量に
  比例しない」に違反**。代替ポリシー(`.bufferingNewest` 等)はアイテムを落とすため論外
- **実験 2**: キャンセルは伝播する。消費側 Task の `cancel()` →
  `onTermination(.cancelled)` 発火 → `for await` 脱出 → 上流の `yield` が
  `.terminated` を返して停止、まで教科書通りの連鎖を確認(良い方の予想も的中)
- **実験 3**: `AsyncChannel` では `await send` がコンシューマの受信までサスペンドし、
  produced/consumed がロックステップで進行、footprint は全区間フラット。
  バックプレッシャーの成立を確認

**副次的な学び:**

- top-level コードは MainActor 上で動き、素の `Task {}` はそのアクター文脈を継承する。
  サスペンションポイントのないループはアクターを独占する(starvation)。
  エンジンがノードのタスクをどの実行文脈で起動するかは独立した設計事項(フェーズ 3 で扱う)
- 同一の `String` ペイロードを 100 万アイテムに持たせても実コピーは発生しない(CoW)。
  柱 1「値型 + CoW なら分岐してもコピーされない」の裏付けを実測で確認

**答え合わせ(n8n のバッファリングモデル / binary data mode):** _未実施_

---

## 6. 探究テーマとしての問い

1. Swift の structured concurrency(TaskGroup / actor / AsyncStream)は、
   データフローエンジンのランタイムとしてどこまで気持ちよく書けるか。
   キャンセル・バックプレッシャー・エラー伝播は本当に「構造」から自然に導けるか
2. ストリーミング前提のエンジンは、n8n 型のバッファリングモデルに対して
   実測でどれだけメモリ優位か(例: 100 万アイテムを定数メモリで完走するか)
3. 「エージェント = 動的グラフ + ループ」という抽象は正しいか。
   エンジンプリミティブとして設計したとき、上物のエージェント層は本当に薄くなるか

---

## 7. フェーズ計画

各フェーズは「テストで検証可能な状態」で締める。
各フェーズの終わりに n8n の対応箇所を読み、ADR に答え合わせを記録する。

| フェーズ | 内容 | n8n 答え合わせ対象 |
|------|------|------|
| 1 | コア型定義: `Value`、`Item`、`NodeType` プロトコル、ワークフロー定義の Codable 表現 | `packages/workflow`(INodeType, INodeExecutionData) |
| 2 | 直列実行エンジン: 分岐なし・3 ノード(Trigger → Transform → Log)を CLI で実行 | `packages/core`(WorkflowExecute) |
| 3 | ストリーミング + 並行化: AsyncStream 配線、分岐・合流、キャンセル | 同上(実行スタックとの対比) |
| 4 | ループとエラー処理: リトライ、ループノード、実行ログ | エラールーティング、部分実行 |
| 5 | HTTP ノード + LLM ノード | 実ノード実装、クレデンシャル管理 |
| 6 | エージェントループ: LLM がツールノードを動的に選んで実行 | AI Agent ノード |
| — | UI はフェーズ 3〜5 の間に最小のグラフビューア(編集不可)から | `packages/editor-ui` |

### n8n を読む際の規律

n8n を読むタイミングは**必ず自分の設計を決めた後**にする。
先に読むと TypeScript のバッファリング型モデルが無意識の「正解」として刷り込まれ、
ストリーミング型という探究テーマ自体が n8n に引っ張られる。
n8n は参考書ではなく「答え合わせ相手」。

---

## 付録 A: Swift シグネチャスケッチ(フェーズ 1 のたたき台)

実装時に変わり得る。確定版はコードとテストが正となる。

```swift
/// JSON-equivalent value. Binary data is held by reference, never copied as a value.
enum Value: Sendable, Codable, Equatable {
    case null
    case bool(Bool)
    case number(Double)
    case string(String)
    case array([Value])
    case object([String: Value])
    case binary(BinaryRef)
}

/// The smallest unit of data flowing between nodes.
struct Item: Sendable {
    var value: Value
    let sourceNodeID: NodeID   // D2: minimal metadata, no lineage tracking
}

/// A node type's static declaration: identity and ports.
protocol NodeType: Sendable {
    static var typeName: String { get }
    static var inputPorts: [PortSpec] { get }
    static var outputPorts: [PortSpec] { get }

    init(parameters: Value) throws

    /// Stream-level contract — the engine's canonical execution API (D1).
    func run(
        inputs: [PortName: Channel],
        outputs: [PortName: Sink],
        context: RunContext
    ) async throws
}

/// Convenience layer for the common 1-item-in, N-items-out case (D1).
/// Conforming types get `run` for free via a default implementation.
protocol TransformNodeType: NodeType {
    func transform(_ item: Item, context: RunContext) async throws -> [Item]
}

/// Runtime form of a Connection (D6): a bounded channel whose `send`
/// suspends until the consumer catches up. v1 wraps AsyncChannel<Item>;
/// the wrapper keeps the external dependency out of the rest of the engine.
struct Channel { /* wraps AsyncChannel<Item> (v1) — contract: bounded, send suspends */ }
```

## 付録 B: サンプルワークフロー JSON(スケッチ)

```json
{
  "schemaVersion": 1,
  "name": "hello-flume",
  "nodes": [
    { "id": "trigger", "type": "manual-trigger", "parameters": {} },
    { "id": "upper",   "type": "transform.uppercase", "parameters": { "field": "message" } },
    { "id": "log",     "type": "log", "parameters": {} }
  ],
  "connections": [
    { "from": { "node": "trigger", "port": "main" }, "to": { "node": "upper", "port": "main" } },
    { "from": { "node": "upper",   "port": "main" }, "to": { "node": "log",   "port": "main" } }
  ]
}
```
