#!/usr/bin/env python3
"""NIMBY Rails Ridership画面向けの旅客データ抽出・集計ツール。"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Iterator, Optional

from startup_screen import run_startup_screen, should_launch_startup_screen


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
HEADER_DATE_PATTERN = re.compile(
    r"(?P<weekday>Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+"
    r"(?P<month>[A-Za-z]+)\s+"
    r"(?P<day>\d{1,2}),\s+"
    r"(?P<year>\d{4})",
    re.IGNORECASE,
)

DAY_BUCKET_LABELS = {
    "mon_thu": "月～木",
    "fri": "金",
    "sat": "土",
    "sun": "日",
}


@dataclasses.dataclass
class StationDefinition:
    name: str
    code: Optional[str] = None


@dataclasses.dataclass
class TrainScheduleDefinition:
    train_id: str
    service_days: list[str]
    origin_station: str
    departure_time: str
    destination_station: str
    direction: str
    vehicle_type: str


@dataclasses.dataclass
class LineDefinition:
    line_id: str
    stations: list[StationDefinition]
    segment_minutes: dict[str, dict[str, int]]
    schedules: list[TrainScheduleDefinition]


@dataclasses.dataclass
class ToolDatabase:
    lines: dict[str, LineDefinition]
    vehicle_types: dict[str, int]


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


def classify_day_bucket(target_date: dt.date) -> str:
    weekday = target_date.weekday()
    if weekday <= 3:
        return "mon_thu"
    if weekday == 4:
        return "fri"
    if weekday == 5:
        return "sat"
    return "sun"


def parse_day_context_from_header(header_text: str) -> Optional[dict[str, str]]:
    m = HEADER_DATE_PATTERN.search(header_text)
    if not m:
        return None
    header_date = dt.datetime.strptime(
        f"{m.group('weekday').title()} {m.group('month').title()} {m.group('day')} {m.group('year')}",
        "%A %B %d %Y",
    ).date()
    bucket = classify_day_bucket(header_date)
    return {
        "weekday": m.group("weekday").title(),
        "date": header_date.isoformat(),
        "bucket": bucket,
        "bucket_label": DAY_BUCKET_LABELS[bucket],
        "display": f"{DAY_BUCKET_LABELS[bucket]}データ 集計日：{header_date.year}年{header_date.month:02d}月{header_date.day:02d}日",
    }


def default_database() -> ToolDatabase:
    return ToolDatabase(lines={}, vehicle_types={})


def open_database_connection(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_database_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS lines (
            line_id TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS stations (
            line_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            name TEXT NOT NULL,
            code TEXT,
            PRIMARY KEY (line_id, position),
            FOREIGN KEY (line_id) REFERENCES lines(line_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS line_segments (
            line_id TEXT NOT NULL,
            from_station TEXT NOT NULL,
            to_station TEXT NOT NULL,
            up_minutes INTEGER NOT NULL,
            down_minutes INTEGER NOT NULL,
            PRIMARY KEY (line_id, from_station, to_station),
            FOREIGN KEY (line_id) REFERENCES lines(line_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            line_id TEXT NOT NULL,
            train_id TEXT NOT NULL,
            service_days TEXT NOT NULL,
            origin_station TEXT NOT NULL,
            departure_time TEXT NOT NULL,
            destination_station TEXT NOT NULL,
            direction TEXT NOT NULL,
            vehicle_type TEXT NOT NULL,
            FOREIGN KEY (line_id) REFERENCES lines(line_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS vehicle_types (
            name TEXT PRIMARY KEY,
            capacity INTEGER NOT NULL
        );
        """
    )


def load_database(path: Path) -> ToolDatabase:
    if not path.exists():
        return default_database()

    with open_database_connection(path) as conn:
        ensure_database_schema(conn)
        lines: dict[str, LineDefinition] = {}

        line_rows = conn.execute("SELECT line_id FROM lines ORDER BY line_id").fetchall()
        for line_row in line_rows:
            line_id = line_row["line_id"]
            station_rows = conn.execute(
                "SELECT name, code FROM stations WHERE line_id = ? ORDER BY position",
                (line_id,),
            ).fetchall()
            segment_rows = conn.execute(
                """
                SELECT from_station, to_station, up_minutes, down_minutes
                FROM line_segments
                WHERE line_id = ?
                ORDER BY from_station, to_station
                """,
                (line_id,),
            ).fetchall()
            schedule_rows = conn.execute(
                """
                SELECT train_id, service_days, origin_station, departure_time,
                       destination_station, direction, vehicle_type
                FROM schedules
                WHERE line_id = ?
                ORDER BY id
                """,
                (line_id,),
            ).fetchall()

            lines[line_id] = LineDefinition(
                line_id=line_id,
                stations=[StationDefinition(name=row["name"], code=row["code"]) for row in station_rows],
                segment_minutes={
                    f"{row['from_station']}->{row['to_station']}": {
                        "up": row["up_minutes"],
                        "down": row["down_minutes"],
                    }
                    for row in segment_rows
                },
                schedules=[
                    TrainScheduleDefinition(
                        train_id=row["train_id"],
                        service_days=json.loads(row["service_days"]),
                        origin_station=row["origin_station"],
                        departure_time=row["departure_time"],
                        destination_station=row["destination_station"],
                        direction=row["direction"],
                        vehicle_type=row["vehicle_type"],
                    )
                    for row in schedule_rows
                ],
            )

        vehicle_rows = conn.execute(
            "SELECT name, capacity FROM vehicle_types ORDER BY name"
        ).fetchall()

    return ToolDatabase(
        lines=lines,
        vehicle_types={row["name"]: row["capacity"] for row in vehicle_rows},
    )


def save_database(path: Path, db: ToolDatabase) -> None:
    with open_database_connection(path) as conn:
        ensure_database_schema(conn)
        with conn:
            conn.execute("DELETE FROM schedules")
            conn.execute("DELETE FROM line_segments")
            conn.execute("DELETE FROM stations")
            conn.execute("DELETE FROM lines")
            conn.execute("DELETE FROM vehicle_types")

            for line_id, line in db.lines.items():
                conn.execute("INSERT INTO lines(line_id) VALUES (?)", (line_id,))
                for position, station in enumerate(line.stations):
                    conn.execute(
                        """
                        INSERT INTO stations(line_id, position, name, code)
                        VALUES (?, ?, ?, ?)
                        """,
                        (line_id, position, station.name, station.code),
                    )
                for segment_key, minutes in line.segment_minutes.items():
                    from_station, to_station = segment_key.split("->", 1)
                    conn.execute(
                        """
                        INSERT INTO line_segments(line_id, from_station, to_station, up_minutes, down_minutes)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (line_id, from_station, to_station, minutes["up"], minutes["down"]),
                    )
                for schedule in line.schedules:
                    conn.execute(
                        """
                        INSERT INTO schedules(
                            line_id, train_id, service_days, origin_station, departure_time,
                            destination_station, direction, vehicle_type
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            line_id,
                            schedule.train_id,
                            json.dumps(schedule.service_days, ensure_ascii=False),
                            schedule.origin_station,
                            schedule.departure_time,
                            schedule.destination_station,
                            schedule.direction,
                            schedule.vehicle_type,
                        ),
                    )

            for name, capacity in db.vehicle_types.items():
                conn.execute(
                    "INSERT INTO vehicle_types(name, capacity) VALUES (?, ?)",
                    (name, capacity),
                )


def get_station_order(line: LineDefinition, station: str) -> int:
    for idx, definition in enumerate(line.stations):
        if definition.name == station:
            return idx
    raise ValueError(f"駅が見つかりません: {station}")


def infer_direction(line: LineDefinition, origin: str, destination: str) -> str:
    origin_idx = get_station_order(line, origin)
    dest_idx = get_station_order(line, destination)
    if origin_idx == dest_idx:
        return "same"
    return "down" if dest_idx > origin_idx else "up"


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




def cmd_line_init(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    db = load_database(db_path)
    if args.line_id in db.lines:
        raise ValueError(f"路線が既に存在します: {args.line_id}")

    if len(args.station_name) != len(args.station_code):
        raise ValueError("--station-name と --station-code は同数指定してください")
    stations = [StationDefinition(name=normalize_spaces(name), code=(normalize_station_code(code) if code else None)) for name, code in zip(args.station_name, args.station_code)]
    db.lines[args.line_id] = LineDefinition(
        line_id=args.line_id,
        stations=stations,
        segment_minutes={},
        schedules=[],
    )
    save_database(db_path, db)
    print(f"路線を作成しました: {args.line_id} ({len(stations)}駅)")
    return 0


def cmd_line_segment(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    db = load_database(db_path)
    line = db.lines.get(args.line_id)
    if not line:
        raise ValueError(f"路線が見つかりません: {args.line_id}")
    get_station_order(line, args.from_station)
    get_station_order(line, args.to_station)

    key = f"{args.from_station}->{args.to_station}"
    reverse_key = f"{args.to_station}->{args.from_station}"
    line.segment_minutes[key] = {"up": args.up_minutes, "down": args.down_minutes}
    line.segment_minutes[reverse_key] = {"up": args.down_minutes, "down": args.up_minutes}
    save_database(db_path, db)
    print(f"駅間時分を保存しました: {key} (up={args.up_minutes}, down={args.down_minutes})")
    return 0


def cmd_vehicle_add(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    db = load_database(db_path)
    db.vehicle_types[args.name] = args.capacity
    save_database(db_path, db)
    print(f"車種を追加/更新しました: {args.name} 定員={args.capacity}")
    return 0


def cmd_vehicle_remove(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    db = load_database(db_path)
    removed = db.vehicle_types.pop(args.name, None)
    save_database(db_path, db)
    if removed is None:
        print(f"車種は未登録です: {args.name}")
    else:
        print(f"車種を削除しました: {args.name}")
    return 0


def cmd_timetable_add(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    db = load_database(db_path)
    line = db.lines.get(args.line_id)
    if not line:
        raise ValueError(f"路線が見つかりません: {args.line_id}")
    if args.vehicle_type not in db.vehicle_types:
        raise ValueError(f"車種が見つかりません: {args.vehicle_type}")

    # validate station existence
    get_station_order(line, args.origin)
    get_station_order(line, args.destination)
    parse_hhmmss(args.departure)

    direction = infer_direction(line, args.origin, args.destination)
    schedule = TrainScheduleDefinition(
        train_id=args.train_id,
        service_days=args.service_days,
        origin_station=args.origin,
        departure_time=args.departure,
        destination_station=args.destination,
        direction=direction,
        vehicle_type=args.vehicle_type,
    )
    line.schedules.append(schedule)
    save_database(db_path, db)
    print(f"列車ダイヤを追加しました: {args.train_id} direction={direction}")
    return 0


def cmd_context_from_header(args: argparse.Namespace) -> int:
    context = parse_day_context_from_header(args.header_text)
    if not context:
        print("ヘッダーから曜日/日付を検知できませんでした。")
        return 1
    print(context["display"])
    print(f"分類キー: {context['bucket']}")
    return 0


def cmd_show_db(args: argparse.Namespace) -> int:
    db = load_database(Path(args.db))
    payload = {
        "lines": {
            line_id: {
                "stations": [dataclasses.asdict(s) for s in line.stations],
                "segment_minutes": line.segment_minutes,
                "schedules": [dataclasses.asdict(s) for s in line.schedules],
            }
            for line_id, line in db.lines.items()
        },
        "vehicle_types": db.vehicle_types,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="NIMBY Rails Ridership分析ツール")
    sub = p.add_subparsers(dest="command", required=True)

    gui = sub.add_parser("gui", help="起動用GUI画面を表示")
    gui.set_defaults(func=lambda _args: run_startup_screen())

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

    line_init = sub.add_parser("line-init", help="路線データを作成")
    line_init.add_argument("--db", default="out/planning_db.sqlite3", help="SQLite DBパス")
    line_init.add_argument("--line-id", required=True, help="路線ID")
    line_init.add_argument("--station-name", action="append", required=True, help="駅名 (並び順で複数指定)")
    line_init.add_argument("--station-code", action="append", required=True, help="駅コード (駅名と同数指定)")
    line_init.set_defaults(func=cmd_line_init)

    seg = sub.add_parser("line-segment", help="駅間の上り下り所要時分を設定")
    seg.add_argument("--db", default="out/planning_db.sqlite3", help="SQLite DBパス")
    seg.add_argument("--line-id", required=True)
    seg.add_argument("--from-station", required=True)
    seg.add_argument("--to-station", required=True)
    seg.add_argument("--up-minutes", type=int, required=True)
    seg.add_argument("--down-minutes", type=int, required=True)
    seg.set_defaults(func=cmd_line_segment)

    vehicle_add = sub.add_parser("vehicle-add", help="車種を追加")
    vehicle_add.add_argument("--db", default="out/planning_db.sqlite3", help="SQLite DBパス")
    vehicle_add.add_argument("--name", required=True, help="車種名")
    vehicle_add.add_argument("--capacity", type=int, required=True, help="車両定員")
    vehicle_add.set_defaults(func=cmd_vehicle_add)

    vehicle_remove = sub.add_parser("vehicle-remove", help="車種を削除")
    vehicle_remove.add_argument("--db", default="out/planning_db.sqlite3", help="SQLite DBパス")
    vehicle_remove.add_argument("--name", required=True, help="車種名")
    vehicle_remove.set_defaults(func=cmd_vehicle_remove)

    tt = sub.add_parser("timetable-add", help="列車ダイヤを追加")
    tt.add_argument("--db", default="out/planning_db.sqlite3", help="SQLite DBパス")
    tt.add_argument("--line-id", required=True)
    tt.add_argument("--train-id", required=True)
    tt.add_argument("--service-days", nargs="+", choices=["mon_thu", "fri", "sat", "sun"], required=True)
    tt.add_argument("--origin", required=True, help="始発駅")
    tt.add_argument("--departure", required=True, help="始発発車時刻 HH:MM(:SS)")
    tt.add_argument("--destination", required=True, help="終着駅")
    tt.add_argument("--vehicle-type", required=True, help="車種")
    tt.set_defaults(func=cmd_timetable_add)

    ctx = sub.add_parser("detect-day", help="ヘッダ文字列から曜日分類を検知")
    ctx.add_argument("--header-text", required=True, help="NIMBY Rails上部の日時文字列")
    ctx.set_defaults(func=cmd_context_from_header)

    show = sub.add_parser("show-db", help="現在のDB内容を表示")
    show.add_argument("--db", default="out/planning_db.sqlite3", help="SQLite DBパス")
    show.set_defaults(func=cmd_show_db)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if should_launch_startup_screen(argv):
        return run_startup_screen()

    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
