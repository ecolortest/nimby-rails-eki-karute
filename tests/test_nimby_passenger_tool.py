from pathlib import Path
import datetime as dt

from nimby_passenger_tool import (
    LineDefinition,
    PassengerRecord,
    StationDefinition,
    ToolDatabase,
    TrainScheduleDefinition,
    build_parser,
    classify_direction_bucket,
    classify_day_bucket,
    infer_direction,
    load_database,
    parse_boarding_station_from_title,
    parse_day_context_from_header,
    parse_ridership_rows,
    parse_time_from_header,
    records_to_increment_events,
    save_database,
)


def test_parse_time_from_header_with_seconds():
    header = "$1,003,227,043 Thursday Jul 16, 2026 06:21:33"
    parsed = parse_time_from_header(header, dt.date(2026, 7, 16))
    assert parsed == dt.datetime(2026, 7, 16, 6, 21, 33)


def test_parse_boarding_station_from_title_with_code():
    title = "(4-10) Karang Setra | Gegerkalong"
    station, code = parse_boarding_station_from_title(title)
    assert station == "Karang Setra"
    assert code == "4-10"


def test_parse_ridership_rows_from_target_screen_format():
    rows = [
        "11 pax L-20.1 [37] (3m) (4-13) Hasan Sadikin New",
        "6 pax L-20.1 [37] (3m) (4-15) Bandung New",
        "1 pax L-20.1 [37] (3m) Moch.Toha New",
    ]
    ts = dt.datetime(2026, 7, 16, 5, 46, 51)
    records = parse_ridership_rows(
        rows,
        timestamp=ts,
        boarding_station="Karang Setra",
        boarding_station_code="4-10",
        source_frame=120,
    )

    assert len(records) == 3
    assert records[0].line_id == "L-20.1"
    assert records[0].alighting_station_code == "4-13"
    assert records[0].alighting_station == "Hasan Sadikin"
    assert records[2].alighting_station_code is None


def test_records_to_increment_events_ignore_initial_and_reset_behavior():
    records = [
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 21, 33), "L-20.1", "Karang Setra", "4-10", "Isola", "4-6", 1, 1),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 21, 33), "L-20.1", "Karang Setra", "4-10", "Hasan Sadikin", "4-13", 0, 1),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 21, 51), "L-20.1", "Karang Setra", "4-10", "Hasan Sadikin", "4-13", 1, 2),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 21, 56), "L-20.1", "Karang Setra", "4-10", "Hasan Sadikin", "4-13", 2, 3),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 24, 32), "L-20.1", "Karang Setra", "4-10", "Isola", "4-6", 0, 4),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 28, 44), "L-20.1", "Karang Setra", "4-10", "Geger Kalong", "4-9", 1, 5),
    ]

    events = records_to_increment_events(
        records,
        ignore_initial_state=True,
        force_up=set(),
        force_down=set(),
        unknown_policy="down",
    )

    assert [(e.timestamp.time(), e.alighting_station, e.delta_passengers) for e in events] == [
        (dt.time(6, 21, 51), "Hasan Sadikin", 1),
        (dt.time(6, 21, 56), "Hasan Sadikin", 1),
        (dt.time(6, 28, 44), "Geger Kalong", 1),
    ]


def test_classify_direction_bucket_default_and_override():
    assert classify_direction_bucket("Isola", "4-6", "4-10", set(), set(), "down") == "up"
    assert classify_direction_bucket("Bandung", "4-15", "4-10", set(), set(), "down") == "down"
    assert classify_direction_bucket("Pasawahan", None, "4-10", set(), set(), "down") == "down"
    assert classify_direction_bucket("Pasawahan", None, "4-10", {"Pasawahan"}, set(), "down") == "up"


def test_user_case_like_direction_totals():
    records = [
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 21, 33), "L-20.1", "Karang Setra", "4-10", "Kolonel Masturi", "4-3", 1, 1),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 21, 33), "L-20.1", "Karang Setra", "4-10", "Isola", "4-6", 1, 1),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 21, 51), "L-20.1", "Karang Setra", "4-10", "Hasan Sadikin", "4-13", 1, 2),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 22, 4), "L-20.1", "Karang Setra", "4-10", "Isola", "4-6", 2, 3),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 22, 27), "L-20.1", "Karang Setra", "4-10", "Geger Kalong", "4-9", 1, 4),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 24, 32), "L-20.1", "Karang Setra", "4-10", "Isola", "4-6", 0, 5),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 24, 32), "L-20.1", "Karang Setra", "4-10", "Kolonel Masturi", "4-3", 0, 5),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 28, 4), "L-20.1", "Karang Setra", "4-10", "Kolonel Masturi", "4-3", 1, 6),
        PassengerRecord(dt.datetime(2026, 7, 16, 6, 28, 44), "L-20.1", "Karang Setra", "4-10", "Geger Kalong", "4-9", 2, 7),
    ]
    events = records_to_increment_events(records, True, set(), set(), "down")
    up_total = sum(e.delta_passengers for e in events if e.direction_bucket == "up")
    assert up_total == 4


def test_parse_day_context_from_header_and_bucket():
    context = parse_day_context_from_header("Thursday July 16, 2026 06:21:33")
    assert context is not None
    assert context["weekday"] == "Thursday"
    assert context["bucket"] == "mon_thu"
    assert "月～木データ 集計日：2026年07月16日" in context["display"]


def test_classify_day_bucket():
    assert classify_day_bucket(dt.date(2026, 7, 13)) == "mon_thu"  # Monday
    assert classify_day_bucket(dt.date(2026, 7, 17)) == "fri"
    assert classify_day_bucket(dt.date(2026, 7, 18)) == "sat"
    assert classify_day_bucket(dt.date(2026, 7, 19)) == "sun"


def test_infer_direction_from_station_order():
    line = LineDefinition(
        line_id="L-20.1",
        stations=[
            StationDefinition(name="A", code="1-1"),
            StationDefinition(name="B", code="1-2"),
            StationDefinition(name="C", code="1-3"),
        ],
        segment_minutes={},
        schedules=[],
    )
    assert infer_direction(line, "A", "C") == "down"
    assert infer_direction(line, "C", "B") == "up"


def test_should_launch_startup_screen():
    from startup_screen import should_launch_startup_screen

    assert should_launch_startup_screen([]) is True
    assert should_launch_startup_screen(["query"]) is False


def test_build_line_selection_title_uses_db_filename_only():
    from line_selection_screen import build_line_selection_title

    title = build_line_selection_title(Path("/tmp/projects/sample.db"))
    assert title == "路線選択画面（DBファイル名: sample.db）"


def test_startup_line_storage_roundtrip(tmp_path):
    from line_selection_screen import add_line, load_line_ids

    db_path = tmp_path / "startup.db"
    add_line(db_path, "Bandung Line 4")
    add_line(db_path, "Bandung Line 3")
    assert load_line_ids(db_path) == ["Bandung Line 3", "Bandung Line 4"]


def test_planning_db_sqlite_roundtrip(tmp_path):
    db_path = tmp_path / "planning_db.sqlite3"
    db = ToolDatabase(
        lines={
            "L-20.1": LineDefinition(
                line_id="L-20.1",
                stations=[
                    StationDefinition(name="Karang Setra", code="4-10"),
                    StationDefinition(name="Bandung", code="4-15"),
                ],
                segment_minutes={"Karang Setra->Bandung": {"up": 14, "down": 12}},
                schedules=[
                    TrainScheduleDefinition(
                        train_id="T1001",
                        service_days=["mon_thu", "fri"],
                        origin_station="Karang Setra",
                        departure_time="06:24:00",
                        destination_station="Bandung",
                        direction="down",
                        vehicle_type="6cars_local",
                    )
                ],
            )
        },
        vehicle_types={"6cars_local": 820},
    )

    save_database(db_path, db)
    loaded = load_database(db_path)

    assert loaded.vehicle_types == {"6cars_local": 820}
    assert loaded.lines["L-20.1"].stations[0].name == "Karang Setra"
    assert loaded.lines["L-20.1"].segment_minutes["Karang Setra->Bandung"] == {"up": 14, "down": 12}
    assert loaded.lines["L-20.1"].schedules[0].service_days == ["mon_thu", "fri"]


def test_planning_command_default_db_path_is_sqlite():
    parser = build_parser()
    args = parser.parse_args(["show-db"])
    assert args.db == "out/planning_db.sqlite3"
