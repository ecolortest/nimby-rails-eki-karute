# NIMBY Rails 旅客分析ツール（CLI版）

GUI化の前段として、以下の業務データをCLIで作成・管理できるツールです。

- 路線データ作成（駅、駅間の上り/下り所要時分）
- 列車ダイヤ作成（運行日区分、始発、発車時刻、終着、車種）
- 車両設定（車種名と定員、追加・削除）
- Ridership画面の旅客データ抽出/増分化/集計
- 画面上の曜日・日付検知（`月～木 / 金 / 土 / 日` 自動判定）

## インストール

## 起動方法（どのファイルを開くか）

起動に使うファイルは **`nimby_passenger_tool.py`** です。

- GUI起動（起動時の `New / Select File` 画面）:
  ```bash
  python nimby_passenger_tool.py
  ```
- GUIを明示的に開く:
  ```bash
  python nimby_passenger_tool.py gui
  ```
- CLIで直接サブコマンド実行（例）:
  ```bash
  python nimby_passenger_tool.py show-db
  ```

```bash
pip install opencv-python pytesseract pytest
```

> `extract` の利用には OS 側の Tesseract 本体も必要です。

## ファイル構成（機能別 / 画面別）

- `nimby_passenger_tool.py`
  - CLI全体の機能（抽出 / 集計 / 路線・車両・ダイヤ管理）
  - サブコマンドの定義とメインエントリ
- `startup_screen.py`
  - 起動時の画面（`New` / `Select File`）
  - 画面起動判定ロジック（引数なしでGUI表示）

## 旅客分析までの標準フロー

1. 路線データを作成
2. 各駅ごとに旅客乗車人数データを入力（映像 submit）
   - NIMBY Rails画面日付を検知し、`〇曜日データ 集計日：YYYY年MM月DD日` 表示
   - 曜日から `mon_thu / fri / sat / sun` を自動分類
3. 車両データを作成
4. 列車ダイヤを作成

これにより「旅客乗車人数 + 車両定員」ベースで列車別の乗車率分析に進めます。

---

## 既存: Ridership 抽出・集計

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
  --ignore-initial
```

### 3) 方向・駅・時間帯集計

```bash
python nimby_passenger_tool.py query \
  --input out/events.csv \
  --line-id L-20.1 \
  --direction-bucket up \
  --start-time 06:22:00 \
  --end-time 06:29:00
```

---

## 新規: 路線/車両/ダイヤ管理

デフォルトDBは `out/planning_db.json`。

### 路線データ作成

```bash
python nimby_passenger_tool.py line-init \
  --line-id L-20.1 \
  --station-name "Karang Setra" --station-code "4-10" \
  --station-name "Isola" --station-code "4-6" \
  --station-name "Bandung" --station-code "4-15"
```

### 駅間の上り下り所要時分

```bash
python nimby_passenger_tool.py line-segment \
  --line-id L-20.1 \
  --from-station "Karang Setra" \
  --to-station "Bandung" \
  --up-minutes 14 \
  --down-minutes 12
```

### 車両設定（追加/削除）

```bash
python nimby_passenger_tool.py vehicle-add --name "6cars_local" --capacity 820
python nimby_passenger_tool.py vehicle-remove --name "6cars_local"
```

### 列車ダイヤ作成

```bash
python nimby_passenger_tool.py timetable-add \
  --line-id L-20.1 \
  --train-id T1001 \
  --service-days mon_thu fri \
  --origin "Karang Setra" \
  --departure 06:24:00 \
  --destination "Bandung" \
  --vehicle-type "6cars_local"
```

- 始発・終着の駅順から `up/down` は自動判定されます。

### 曜日データ検知

```bash
python nimby_passenger_tool.py detect-day \
  --header-text "Thursday July 16, 2026 06:21:33"
```

### DB確認

```bash
python nimby_passenger_tool.py show-db
```
