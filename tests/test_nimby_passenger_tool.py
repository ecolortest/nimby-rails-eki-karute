import datetime as dt

from nimby_passenger_tool import (
    PassengerRecord,
    classify_direction_bucket,
    parse_boarding_station_from_title,
    parse_ridership_rows,
    parse_time_from_header,
    records_to_increment_events,
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
