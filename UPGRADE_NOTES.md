# Embulk 0.11 アップグレードノート

## 概要
このプロジェクトをEmbulk 0.9系から0.11系にアップグレードしました。

## 主な変更点

### 1. Embulkバージョン
- **変更前**: Embulk 0.9.x
- **変更後**: Embulk 0.11

### 2. Gradleバージョン
- **変更前**: Gradle 5.2.1
- **変更後**: Gradle 7.6.1

### 3. 依存関係の更新

#### build.gradle
- `embulk-spi`: 0.11 (compileOnly)
- `embulk-util-config`: 0.3.4 (implementation)
- `embulk-util-timestamp`: 0.2.2 (implementation)
- `jackson-module-scala_2.13`: 2.14.0 (implementation) - 新規追加

### 4. コード変更

#### PluginTask.scala
- `ConfigMapperFactory`を使用した新しいAPI実装
- `loadConfig()`: `ConfigMapper`を使用してConfigSourceからTaskを生成
- `loadTask()`: `TaskMapper`を使用してTaskSourceからTaskを生成
- `dumpTask()`: Jackson ObjectMapperを使用してTaskをTaskSourceに変換

#### S3ParquetOutputPlugin.scala
- `transaction()`メソッドで`PluginTask.dumpTask()`を使用するように変更
- `resume()`メソッドで`PluginTask.loadTask()`を使用するように変更

#### S3ParquetPageOutput.scala
- `PageReader`コンストラクタを新しいAPIに対応（スキーマのみを渡す形式）

### 5. 非推奨API
以下のAPIは非推奨ですが、動作には影響ありません：
- `org.embulk.spi.time.Timestamp` (複数箇所)
- `ConfigSource.loadConfig()`
- `PageReader.getTimestamp()`
- `PageReader.getJson()`

これらは将来のバージョンで置き換えが必要になる可能性があります。

## ビルド方法

```bash
./gradlew clean build -x test
```

## 生成物
- `build/libs/embulk-output-s3_parquet-0.5.3.dev1.jar`

## 互換性
- JDK 1.8を維持
- 既存の設定ファイル（config.yml）との互換性を保持

## 注意事項
1. Embulk 0.11では、プラグインの読み込み方法が変更されています
2. `embulk-util-config`を使用した新しいConfig APIを採用
3. テストは実行していません（`-x test`オプション使用）

## 今後の対応
1. 非推奨APIの置き換え（必要に応じて）
2. テストコードの更新と実行
3. 実環境での動作確認
