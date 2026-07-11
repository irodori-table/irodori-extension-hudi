<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# コネクタメタデータ

`connector.source.json`は各拡張機能の人間が編集可能な真実の情報源です。
`connector.config.json`と`irodori.extension.json`は、生成されたパッケージング
アーティファクトであり、現在のネイティブABIとマーケットプレイスのレイアウトとの互換性を保つために保持されています。

共有のコネクタメタデータジェネレーターは`irodori-table`
コーディネーターリポジトリにあります。この拡張リポジトリは、生成されたアーティファクトと
ローカルのREADMEヘルパーのみを保持しています。

## コマンド

生成されたコネクタメタデータから英語のREADMEファイルを再生成します：

```sh
python3 tools/generate_readmes.py
```