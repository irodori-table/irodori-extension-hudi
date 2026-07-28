<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# Hudiコネクタ

Hudi用のネイティブいろどりテーブルコネクタ拡張機能。

このクレートは、コネクタのメタデータ、ネイティブABIエクスポート、およびIrodori拡張マーケットプレイスで使用されるドライバ実装をパッケージ化しています。

## コネクタ

- 拡張ID: `irodori.hudi`
- エンジンID: `hudi`
- ワイヤープロトコル: `lakehouse`
- デフォルトポート: `443`
- ネイティブABI: `irodori.connector.native.v1`
- ドライバ連携: `はい`
- マーケットプレイスの表示設定: `公開`
- パッケージバージョン: `0.1.3`

このパッケージはコネクタのメタデータとネイティブドライバを直接使用し、デスクトップアダプターのスナップショットは必要ありません。

コネクタのメタデータは `connector.config.json` と `irodori.extension.json` に格納されています。
Rustクレートは `src/lib.rs` からネイティブABIをエクスポートし、`irodori-connector-abi` を共有JSON/バッファヘルパーとして使用し、コネクタの動作は `src/driver.rs` に保持しています。

## 接続メタデータ

- エンドポイントモード: `catalog`, `objectStorage`, `jdbc`, `connectionString`
- トランスポートモード: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS対応: `はい`
- TLS必須（デフォルト）: `はい`
- カスタムドライバオプション: `はい`

### エンドポイントフィールド

| フィールド | ラベル | 型 | 必須 |
| --- | --- | --- | --- |
| `catalogType` | カタログタイプ | `string` | はい |
| `catalogUri` | カタログURI | `uri` | いいえ |
| `warehouse` | ウェアハウスパス | `string` | いいえ |
| `tablePath` | テーブルパス | `string` | いいえ |
| `storageBackend` | ストレージバックエンド | `string` | いいえ |
| `region` | クラウドリージョン | `string` | いいえ |
| `credentialVending` | 認証情報販売 | `boolean` | いいえ |

## 認証

コネクタはこれらの認証モードを公開しており、クライアントは適切な資格情報フィールドをレンダリングできます。必要に応じて、ドライバ固有またはプロバイダー固有の値も `options` を通じて渡すことが可能です。

| 認証方法 | ラベル | 種類 | シークレットの用途 |
| --- | --- | --- | --- |
| `none` | 認証なし | `none` | なし |
| `connectionString` | 接続文字列 / DSN | `connectionString` | なし |
| `awsDefaultCredentialsChain` | AWSデフォルト資格情報チェーン | `iam` | なし |
| `awsSigV4` | AWS SigV4 | `iam` | `token` |
| `awsProfile` | AWS共有設定プロファイル | `iam` | なし |
| `awsSso` | AWS IAMアイデンティティセンター / SSO | `iam` | `token` |
| `webIdentity` | AWS Webアイデンティティ | `iam` | `token` |
| `awsAssumeRole` | AWS STSロール引き受け | `iam` | `token` |
| `sessionToken` | AWSセッショントークン | `token` | `token` |
| `oauth2` | OAuth 2.0 | `oauth2` | `token` |
| `catalogBearerToken` | カタログベアラートークン | `token` | `token` |
| `catalogPassword` | カタログユーザ/パスワード | `userPassword` | `password` |
| `serviceAccountJson` | サービスアカウントJSON | `serviceAccount` | `privateKey` |
| `serviceAccountJwt` | サービスアカウントJWT秘密鍵 | `privateKey` | `privateKey`, `privateKeyPassphrase` |
| `serviceAccountImpersonation` | サービスアカウントのなりすまし | `iam` | `token` |
| `googleApplicationDefaultCredentials` | アプリケーションデフォルト資格情報 | `iam` | なし |
| `workloadIdentity` | ワークロードアイデンティティ連合 | `iam` | `token` |
| `azureAd` | Azure AD / Entra ID | `azureAd` | `token` |
| `servicePrincipal` | サービスプリンシパル | `oauth2` | `token` |
| `servicePrincipalCertificate` | サービスプリンシパル証明書 | `oauth2` | `privateKey`, `privateKeyPassphrase` |
| `managedIdentity` | マネージドアイデンティティ | `managedIdentity` | なし |
| `sasToken` | SASトークン | `token` | `token` |
| `customDriverOptions` | カスタムドライバオプション | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## ネイティブABI呼び出し

| メソッド | 応答 |
| --- | --- |
| `health` | コネクタのヘルス状態、エンジンID、ABIバージョン、ドライバの状態を返します。 |
| `describe` | 埋め込みマニフェストとコネクタ設定を返します。 |
| `manifest` | 生の `irodori.extension.json` を返します。 |
| `config` | 生の `connector.config.json` を返します。 |
| `connect` | ネイティブコネクタ接続を開き、検証します。 |
| `query` | コネクタクエリを実行し、構造化された行またはJSON結果を返します。 |
| `metadata` | スキーマ、テーブル、列、インデックス、コレクション、または同等のメタデータを読み取ります。 |
| `close` | キャッシュされたネイティブ接続を閉じて削除します。 |

## 開発

このチェックアウト内のすべての拡張クレートは `../target` を共有しており、依存関係は一度だけコンパイルされます。

```sh
make check
make build
```

リリースパッケージはプラットフォーム固有のネイティブアーティファクトを `dist/native` に配置します。

## ライセンス

0BSD。このプロジェクトはほぼすべての目的で使用、コピー、修正、配布できます。