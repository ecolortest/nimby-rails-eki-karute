# NIMBY Rails Ridership画面 抽出・増分集計ツール

NIMBY Rails の **Stations > Ridership** 画面向けに、以下を行うCLIです。

1. 映像から各時点の待機人数（スナップショット）をOCR抽出
2. スナップショット差分から「+1人が発生した時刻」の増分イベントを生成
3. 方向（上り/下り）・駅・時間帯で集計

> 重要: 画面の最初に表示されていた待機客を除外したい場合、`events` で `--ignore-initial` を指定します。

## 仕様（今回の要件対応）

- 時刻は秒まで扱います（`06:21:33` 形式）
- 列車到着で人数が減る（画面から消える）挙動は「減少イベント」としては集計せず、以降の増加のみ集計を継続します
- 方向分類は以下:
  - 乗車駅コード（例 `4-10`）より小さい番号の駅: `up`
  - 大きい番号の駅: `down`
  - 番号不明駅: 既定 `down`（`--unknown-policy` で変更可）
  - 駅名ごとの個別上書き: `--force-up`, `--force-down`

## セットアップ

```bash
pip install opencv-python pytesseract pytest
```

> `extract` の利用にはOS側の Tesseract 本体も必要です。

## 使い方

### 1) 映像からスナップショットCSV抽出

```bash
python nimby_passenger_tool.py extract \
  --video capture.mp4 \
  --output out/snapshots.csv \
  --sample-interval 1.0 \
  --lang eng
```

### 2) スナップショット差分を増分イベント化

```bash
python nimby_passenger_tool.py events \
  --input out/snapshots.csv \
  --output out/events.csv \
  --ignore-initial \
  --force-up "Pasawahan" \
  --unknown-policy down
```

### 3) 時間帯・方向で集計

```bash
python nimby_passenger_tool.py query \
  --input out/events.csv \
  --line-id L-20.1 \
  --direction-bucket up \
  --start-time 06:22:00 \
  --end-time 06:29:00
```

## 例（ユーザー説明のケース）

- 06:21:33 時点に最初から表示されている `(4-3) Kolonel Masturi` と `(4-6) Isola` は `--ignore-initial` で除外
- 06:24:32 の上り列車到着で Isola/Geger Kalong 行きが画面から消えても、以降の再出現（例 06:28:44 Geger Kalong）を増分として再度計上
- これにより「06:22:00〜06:29:00 の上り方面」などの時間帯集計が可能

## 注意

- OCR誤認識対策として、解像度/UI倍率が違う場合は `extract_records_from_video` のROI比率を調整してください。
