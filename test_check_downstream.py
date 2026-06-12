from collections import namedtuple

from check_downstream import build_eligible_units, find_flagged_units


Row = namedtuple("Row", "dsttype runnumber segment events filename")


REQUIRED = ["DST_TRIGGERED_EVENT_seb18", "DST_TRIGGERED_EVENT_seb20"]


def test_matching_input_and_output_is_not_flagged():
    inputs = [
        Row("DST_TRIGGERED_EVENT_seb18", 82703, 0, 1000, "in18.root"),
        Row("DST_TRIGGERED_EVENT_seb20", 82703, 0, 1000, "in20.root"),
    ]
    outputs = [Row("DST_CALOFITTING", 82703, 0, 1000, "out.root")]

    eligible = build_eligible_units(inputs, REQUIRED)
    assert find_flagged_units(eligible, outputs, ratio_cut=0.9) == []


def test_missing_input_stream_is_not_eligible():
    inputs = [Row("DST_TRIGGERED_EVENT_seb18", 82703, 0, 1000, "in18.root")]

    eligible = build_eligible_units(inputs, REQUIRED)
    assert eligible == {}


def test_missing_output_is_flagged():
    inputs = [
        Row("DST_TRIGGERED_EVENT_seb18", 82703, 0, 1000, "in18.root"),
        Row("DST_TRIGGERED_EVENT_seb20", 82703, 0, 1000, "in20.root"),
    ]

    eligible = build_eligible_units(inputs, REQUIRED)
    flagged = find_flagged_units(eligible, [], ratio_cut=0.9)

    assert len(flagged) == 1
    assert flagged[0].reasons == ("missing_output",)
    assert flagged[0].report_line() == "00082703 00000 missing_output 1000 -1"


def test_low_output_events_is_flagged():
    inputs = [
        Row("DST_TRIGGERED_EVENT_seb18", 82703, 0, 1000, "in18.root"),
        Row("DST_TRIGGERED_EVENT_seb20", 82703, 0, 1000, "in20.root"),
    ]
    outputs = [Row("DST_CALOFITTING", 82703, 0, 800, "out.root")]

    eligible = build_eligible_units(inputs, REQUIRED)
    flagged = find_flagged_units(eligible, outputs, ratio_cut=0.9)

    assert len(flagged) == 1
    assert flagged[0].reasons == ("low_output_events",)


def test_input_mismatch_is_flagged():
    inputs = [
        Row("DST_TRIGGERED_EVENT_seb18", 82703, 0, 1000, "in18.root"),
        Row("DST_TRIGGERED_EVENT_seb20", 82703, 0, 999, "in20.root"),
    ]
    outputs = [Row("DST_CALOFITTING", 82703, 0, 1000, "out.root")]

    eligible = build_eligible_units(inputs, REQUIRED)
    flagged = find_flagged_units(eligible, outputs, ratio_cut=0.9)

    assert len(flagged) == 1
    assert flagged[0].reasons == ("input_mismatch",)
    assert flagged[0].input_events == -1


def test_cut_segment_skips_non_divisible_segments():
    inputs = [
        Row("DST_TRIGGERED_EVENT_seb18", 82703, 1, 1000, "in18.root"),
        Row("DST_TRIGGERED_EVENT_seb20", 82703, 1, 1000, "in20.root"),
        Row("DST_TRIGGERED_EVENT_seb18", 82703, 2, 1000, "in18.root"),
        Row("DST_TRIGGERED_EVENT_seb20", 82703, 2, 1000, "in20.root"),
    ]

    eligible = build_eligible_units(inputs, REQUIRED, cut_segment=2)

    assert sorted(eligible) == [(82703, 2)]
