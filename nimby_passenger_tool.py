#!/usr/bin/env python3
"""NIMBY Rails Ridership画面向けの旅客データ抽出・集計ツール。"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Iterator, Optional


@dataclasses.dataclass(frozen=True)
class PassengerRecord:
    timestamp: dt.datetime
    line_id: str
    boarding_station: str
    boarding_station_code: Optional[str]
    alighting_station: str
    alighting_station_code: Optional[str]
    passengers: int
    source_frame: int


@dataclasses.dataclass(frozen=True)
class PassengerEvent:
    timestamp: dt.datetime
    line_id: str
    boarding_station: str
    boarding_station_code: Optional[str]
    alighting_station: str
    alighting_station_code: Optional[str]
    direction_bucket: str
    delta_passengers: int
    source_frame: int


CLOCK_PATTERN = re.compile(r"(?P<hour>\d{1,2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?")
TITLE_PATTERN = re.compile(r"\((?P<code>[^)]+)\)\s*(?P<station>[^|]+)")
ROW_PATTERN = re.compile(
    r"(?P<passengers>\d+)\s*pax\s+"
    r"(?P<line>[A-Za-z0-9_.\-]+)\s+\[\d+\]\s+\(\d+m\)\s+"
    r"(?:\((?P<alight_code>\d+-\d+)\)\s+)?"
    r"(?P<next_stop>.+?)\s*"
    r"(?:New|Boarding|Waiting)?$"
)
STATION_CODE_PATTERN = re.compile(r"(?P<major>\d+)-(?P<minor>\d+)")


def normalize_spaces(text: str) -> str:
    return " ".join(text.split())


def parse_time_from_header(header_text: str, base_date: dt.date) -> Optional[dt.datetime]:
    m = CLOCK_PATTERN.search(header_text)
    if not m:
        return None
    return dt.datetime.combine(
        base_date,
        dt.time(
            hour=int(m.group("hour")),
            minute=int(m.group("minute")),
            second=int(m.group("second") or 0),
        ),
    )


def parse_boarding_station_from_title(title_text: str) -> tuple[Optional[str], Optional[str]]:
    cleaned = normalize_spaces(title_text)
    m = TITLE_PATTERN.search(cleaned)
    if not m:
        return None, None
    station_name = m.group("station").strip()
    station_code = normalize_station_code(m.group("code"))
    return station_name, station_code


def parse_station_code(station_code: Optional[str]) -> Optional[tuple[int, int]]:
    if not station_code:
        return None
    m = STATION_CODE_PATTERN.search(station_code)
    if not m:
        return None
    return int(m.group("major")), int(m.group("minor"))


def normalize_station_code(text: str) -> str:
    return text.strip().replace(" ", "")


def classify_direction_bucket(
    alighting_station: str,
    alighting_station_code: Optional[str],
    boarding_station_code: Optional[str],
    force_up: set[str],
    force_down: set[str],
    unknown_policy: str = "down",
) -> str:
    if alighting_station in force_up:
        return "up"
    if alighting_station in force_down:
        return "down"

    boarding_parsed = parse_station_code(boarding_station_code)
    alighting_parsed = parse_station_code(alighting_station_code)
    if not boarding_parsed or not alighting_parsed:
        return unknown_policy

    if boarding_parsed[0] != alighting_parsed[0]:
        return unknown_policy
    if alighting_parsed[1] < boarding_parsed[1]:
        return "up"
    if alighting_parsed[1] > boarding_parsed[1]:
        return "down"
    return "same"


def parse_ridership_rows(
    lines: Iterable[str],
    timestamp: dt.datetime,
    boarding_station: str,
    boarding_station_code: Optional[str],
    source_frame: int,
) -> list[PassengerRecord]:
    records: list[PassengerRecord] = []
    for raw in lines:
        line = normalize_spaces(raw)
        m = ROW_PATTERN.match(line)
        if not m:
            continue
        records.append(
            PassengerRecord(
                timestamp=timestamp,
                line_id=m.group("line"),
                boarding_station=boarding_station,
                boarding_station_code=boarding_station_code,
                alighting_station=m.group("next_stop").strip(),
                alighting_station_code=(normalize_station_code(m.group("alight_code")) if m.group("alight_code") else None),
                passengers=int(m.group("passengers")),
                source_frame=source_frame,
            )
        )
    return records


def extract_records_from_video(
    video_path: Path,
    sample_interval_sec: float = 1.0,
    tesseract_lang: str = "eng",
    base_date: Optional[dt.date] = None,
) -> Iterator[PassengerRecord]:
    try:
        import cv2  # type: ignore
        import pytesseract  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "opencv-python と pytesseract が必要です。"
            "pip install opencv-python pytesseract で導入してください。"
        ) from exc

    if base_date is None:
        base_date = dt.date.today()

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        raise RuntimeError(f"動画を開けません: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frame_step = max(1, int(fps * sample_interval_sec))
    frame_idx = 0

    while True:
        ok, frame = cap.read()
        if not ok:
            break

        if frame_idx % frame_step != 0:
            frame_idx += 1
            continue

        h, w = frame.shape[:2]

        def crop_ratio(x1: float, y1: float, x2: float, y2: float):
            return frame[int(h * y1) : int(h * y2), int(w * x1) : int(w * x2)]

        topbar = crop_ratio(0.33, 0.0, 0.64, 0.06)
        title = crop_ratio(0.30, 0.24, 0.74, 0.29)
        ridership_table = crop_ratio(0.30, 0.38, 0.74, 0.74)

        def prep(img):
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            blur = cv2.GaussianBlur(gray, (3, 3), 0)
            return cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]

        topbar_text = pytesseract.image_to_string(prep(topbar), lang=tesseract_lang, config="--psm 6")
        title_text = pytesseract.image_to_string(prep(title), lang=tesseract_lang, config="--psm 7")
        table_text = pytesseract.image_to_string(prep(ridership_table), lang=tesseract_lang, config="--psm 6")

        timestamp = parse_time_from_header(topbar_text, base_date)
        boarding_station, boarding_station_code = parse_boarding_station_from_title(title_text)

        if timestamp and boarding_station:
            lines = [l for l in table_text.splitlines() if l.strip()]
            for rec in parse_ridership_rows(lines, timestamp, boarding_station, boarding_station_code, frame_idx):
                yield rec

        frame_idx += 1

    cap.release()


def records_to_increment_events(
    records: Iterable[PassengerRecord],
    ignore_initial_state: bool,
    force_up: set[str],
    force_down: set[str],
    unknown_policy: str = "down",
) -> list[PassengerEvent]:
    last_counts: dict[tuple[str, str, str], int] = {}
    events: list[PassengerEvent] = []

    sorted_records = sorted(records, key=lambda r: (r.timestamp, r.source_frame, r.line_id, r.alighting_station))
    baseline_time = sorted_records[0].timestamp if sorted_records else None
    for rec in sorted_records:
        key = (rec.line_id, rec.boarding_station, rec.alighting_station)
        prev = last_counts.get(key)
        if prev is None:
            last_counts[key] = rec.passengers
            is_baseline_observation = baseline_time is not None and rec.timestamp == baseline_time
            if rec.passengers > 0 and (not ignore_initial_state or not is_baseline_observation):
                bucket = classify_direction_bucket(
                    rec.alighting_station,
                    rec.alighting_station_code,
                    rec.boarding_station_code,
                    force_up,
                    force_down,
                    unknown_policy,
                )
                events.append(
                    PassengerEvent(
                        timestamp=rec.timestamp,
                        line_id=rec.line_id,
                        boarding_station=rec.boarding_station,
                        boarding_station_code=rec.boarding_station_code,
                        alighting_station=rec.alighting_station,
                        alighting_station_code=rec.alighting_station_code,
                        direction_bucket=bucket,
                        delta_passengers=rec.passengers,
                        source_frame=rec.source_frame,
                    )
                )
            continue

        delta = rec.passengers - prev
        last_counts[key] = rec.passengers
        if delta <= 0:
            continue

        bucket = classify_direction_bucket(
            rec.alighting_station,
            rec.alighting_station_code,
            rec.boarding_station_code,
            force_up,
            force_down,
            unknown_policy,
        )
        events.append(
            PassengerEvent(
                timestamp=rec.timestamp,
                line_id=rec.line_id,
                boarding_station=rec.boarding_station,
                boarding_station_code=rec.boarding_station_code,
                alighting_station=rec.alighting_station,
                alighting_station_code=rec.alighting_station_code,
                direction_bucket=bucket,
                delta_passengers=delta,
                source_frame=rec.source_frame,
            )
        )

    return events


def write_records_csv(records: Iterable[PassengerRecord], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp",
                "line_id",
                "boarding_station",
                "boarding_station_code",
                "alighting_station",
                "alighting_station_code",
                "passengers",
                "source_frame",
            ]
        )
        for r in records:
            writer.writerow(
                [
                    r.timestamp.isoformat(timespec="seconds"),
                    r.line_id,
                    r.boarding_station,
                    r.boarding_station_code or "",
                    r.alighting_station,
                    r.alighting_station_code or "",
                    r.passengers,
                    r.source_frame,
                ]
            )


def write_events_csv(events: Iterable[PassengerEvent], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp",
                "line_id",
                "boarding_station",
                "boarding_station_code",
                "alighting_station",
                "alighting_station_code",
                "direction_bucket",
                "delta_passengers",
                "source_frame",
            ]
        )
        for e in events:
            writer.writerow(
                [
                    e.timestamp.isoformat(timespec="seconds"),
                    e.line_id,
                    e.boarding_station,
                    e.boarding_station_code or "",
                    e.alighting_station,
                    e.alighting_station_code or "",
                    e.direction_bucket,
                    e.delta_passengers,
                    e.source_frame,
                ]
            )


def read_records_csv(path: Path) -> list[PassengerRecord]:
    rows: list[PassengerRecord] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                PassengerRecord(
                    timestamp=dt.datetime.fromisoformat(row["timestamp"]),
                    line_id=row["line_id"],
                    boarding_station=row["boarding_station"],
                    boarding_station_code=row.get("boarding_station_code") or None,
                    alighting_station=row["alighting_station"],
                    alighting_station_code=row.get("alighting_station_code") or None,
                    passengers=int(row["passengers"]),
                    source_frame=int(row.get("source_frame") or -1),
                )
            )
    return rows


def read_events_csv(path: Path) -> list[PassengerEvent]:
    rows: list[PassengerEvent] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                PassengerEvent(
                    timestamp=dt.datetime.fromisoformat(row["timestamp"]),
                    line_id=row["line_id"],
                    boarding_station=row["boarding_station"],
                    boarding_station_code=row.get("boarding_station_code") or None,
                    alighting_station=row["alighting_station"],
                    alighting_station_code=row.get("alighting_station_code") or None,
                    direction_bucket=row["direction_bucket"],
                    delta_passengers=int(row["delta_passengers"]),
                    source_frame=int(row.get("source_frame") or -1),
                )
            )
    return rows


def parse_hhmmss(value: Optional[str]) -> Optional[dt.time]:
    if not value:
        return None
    parts = [int(x) for x in value.split(":")]
    if len(parts) == 2:
        return dt.time(hour=parts[0], minute=parts[1], second=0)
    if len(parts) == 3:
        return dt.time(hour=parts[0], minute=parts[1], second=parts[2])
    raise ValueError("時刻は HH:MM または HH:MM:SS")


def filter_events(
    events: Iterable[PassengerEvent],
    line_id: Optional[str] = None,
    station: Optional[str] = None,
    direction_bucket: Optional[str] = None,
    start_time: Optional[dt.time] = None,
    end_time: Optional[dt.time] = None,
) -> list[PassengerEvent]:
    out: list[PassengerEvent] = []
    for e in events:
        if line_id and e.line_id != line_id:
            continue
        if station and station not in {e.boarding_station, e.alighting_station}:
            continue
        if direction_bucket and e.direction_bucket != direction_bucket:
            continue
        t = e.timestamp.time()
        if start_time and t < start_time:
            continue
        if end_time and t > end_time:
            continue
        out.append(e)
    return out


def aggregate_events(events: Iterable[PassengerEvent]) -> dict[tuple[str, str, str, str], int]:
    totals: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for e in events:
        key = (e.line_id, e.direction_bucket, e.boarding_station, e.alighting_station)
        totals[key] += e.delta_passengers
    return dict(sorted(totals.items()))


def cmd_extract(args: argparse.Namespace) -> int:
    records = list(
        extract_records_from_video(
            Path(args.video),
            sample_interval_sec=args.sample_interval,
            tesseract_lang=args.lang,
        )
    )
    write_records_csv(records, Path(args.output))
    print(f"{len(records)} snapshot records written: {args.output}")
    return 0


def cmd_events(args: argparse.Namespace) -> int:
    records = read_records_csv(Path(args.input))
    force_up = set(args.force_up or [])
    force_down = set(args.force_down or [])
    events = records_to_increment_events(
        records,
        ignore_initial_state=args.ignore_initial,
        force_up=force_up,
        force_down=force_down,
        unknown_policy=args.unknown_policy,
    )
    write_events_csv(events, Path(args.output))
    print(f"{len(events)} increment events written: {args.output}")
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    events = read_events_csv(Path(args.input))
    filtered = filter_events(
        events,
        line_id=args.line_id,
        station=args.station,
        direction_bucket=args.direction_bucket,
        start_time=parse_hhmmss(args.start_time),
        end_time=parse_hhmmss(args.end_time),
    )

    summary = aggregate_events(filtered)
    if not summary:
        print("対象データがありません。")
        return 0

    print("line_id,direction_bucket,boarding_station,alighting_station,delta_passengers")
    for (line_id, bucket, boarding, alighting), passengers in summary.items():
        print(f"{line_id},{bucket},{boarding},{alighting},{passengers}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="NIMBY Rails Ridership分析ツール")
    sub = p.add_subparsers(dest="command", required=True)

    extract = sub.add_parser("extract", help="映像のRidership画面からスナップショットCSV抽出")
    extract.add_argument("--video", required=True, help="入力動画ファイル")
    extract.add_argument("--output", required=True, help="出力CSV")
    extract.add_argument("--sample-interval", type=float, default=1.0, help="フレーム抽出間隔(秒)")
    extract.add_argument("--lang", default="eng", help="Tesseract言語")
    extract.set_defaults(func=cmd_extract)

    events = sub.add_parser("events", help="スナップショットCSVから増分イベントCSVを作成")
    events.add_argument("--input", required=True, help="extractで作成した入力CSV")
    events.add_argument("--output", required=True, help="増分イベントCSV")
    events.add_argument("--ignore-initial", action="store_true", help="最初の表示人数を集計対象外にする")
    events.add_argument("--force-up", action="append", help="駅名で強制的に上りに分類 (複数指定可)")
    events.add_argument("--force-down", action="append", help="駅名で強制的に下りに分類 (複数指定可)")
    events.add_argument("--unknown-policy", choices=["down", "up", "same"], default="down", help="駅番号が無い場合の方向分類")
    events.set_defaults(func=cmd_events)

    query = sub.add_parser("query", help="増分イベントを方向・駅・時間帯で集計")
    query.add_argument("--input", required=True, help="eventsで作成したCSV")
    query.add_argument("--line-id", help="路線IDで絞り込み")
    query.add_argument("--station", help="乗車駅または降車駅で絞り込み")
    query.add_argument("--direction-bucket", choices=["up", "down", "same"], help="方向区分で絞り込み")
    query.add_argument("--start-time", help="開始時刻 HH:MM または HH:MM:SS")
    query.add_argument("--end-time", help="終了時刻 HH:MM または HH:MM:SS")
    query.set_defaults(func=cmd_query)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
