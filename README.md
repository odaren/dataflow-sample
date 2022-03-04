# dataflow-sample-code
Dataflowのサンプルコード
テキストファイルに含まれる文章を、単語に分け、各単語がいくつ含まれているか集計する。

### 手順
1.ライブラリのインストール
```
pip install 'apache-beam[gcp]'
```
2.実行(--outputには、GCSを指定)
```
python3 wordcount.py --outout gs://バケット名/ファイル名
```
