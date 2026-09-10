#!/usr/bin/env python3
"""
generate_report.py

Generate a run-level CSV report for sPHENIX production rules.

This supports raw/event-combiner rules and a first-pass downstream report.
Downstream reporting starts with run-level possible-vs-present events and segments
from production_jobs and the FileCatalog.
"""

import cProfile
import csv
import math
import pstats
import sys
from collections import defaultdict
from pathlib import Path

from argparsing import submission_args
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixmatching import MatchConfig
from sphenixdbutils import cnxn_string_map, dbQuery
from sphenixmisc import human_event_count



CSV_COLUMNS = [
    "rule_name",
    "runnumber",
    "possible_daqhosts",
    "total_daqhosts",
    "missing_daqhosts",
    "possible_segments",
    "total_segments",
    "missing_segments",
    "possible_events",
    "total_events",
    "missing_events",
    "error_codes",
    "incomplete_reasons",
    "status",
]


def daqhost_to_dst_leaf(daqhost, match):
    if isinstance(match.input_stem, dict):
        for leaf, raw_daqhost in match.input_stem.items():
            if raw_daqhost == daqhost:
                return leaf
    return str(daqhost).replace(":", "_")


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def csv_join(values):
    return ";".join(str(value) for value in sorted(values, key=str))


def sql_in_list(values):
    return "(" + ",".join(sql_literal(value) for value in values) + ")"




def output_path(args):
    return Path(args.output) if args.output else Path(f"{args.rulename}.csv")


def load_rule_and_match(args):
    param_overrides = {}
    param_overrides["runs"] = args.runs
    param_overrides["runlist"] = args.runlist
    param_overrides["nevents"] = args.nevents
    param_overrides["prodmode"] = "production"
    if args.physicsmode:
        param_overrides["physicsmode"] = args.physicsmode

    try:
        rule = RuleConfig.from_yaml_file(
            yaml_file=args.config,
            rule_name=args.rulename,
            param_overrides=param_overrides,
        )
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error loading rule configuration: {e}")
        sys.exit(2)

    return rule, MatchConfig.from_rule_config(rule)



def query_error_codes(args, match, run_condition):
    # what about ExitCode is Null?
    query = f"""
        SELECT runnumber, ExitCode
        FROM production_jobs
        WHERE rulename={sql_literal(args.rulename)}
          AND dataset={sql_literal(match.dataset)}
          AND tag={sql_literal(match.outtriplet)}
          AND dsttype like {sql_literal(match.dst_type_template)}
          AND {run_condition}
          AND ExitCode IS NOT NULL
          AND ExitCode != 0
        ORDER BY runnumber, ExitCode
    """
    rows = dbQuery(cnxn_string_map["statr"], query).fetchall()
    codes_by_run = defaultdict(set)
    for runnumber, exit_code in rows:
        codes_by_run[int(runnumber)].add(int(exit_code))
    INFO(f"{sum(len(codes) for codes in codes_by_run.values())} production job exit codes found.")
    return codes_by_run


def add_reason(reasons_by_run, runnumber, reason):
    reasons_by_run[int(runnumber)].add(reason)


def write_csv_report(
    path,
    args,
    runnumbers,
    possible_daqhosts_by_run,
    total_daqhosts_by_run,
    possible_segments_by_run,
    total_segments_by_run,
    possible_events_by_run,
    total_events_by_run,
    error_codes_by_run,
    reasons_by_run,
    missing_event_tolerance_by_run=None,
):
    rows_written = 0
    complete_runs = 0
    status_counts = defaultdict(int)
    missing_event_tolerance_by_run = missing_event_tolerance_by_run or {}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for runnumber in sorted(runnumbers):
            possible_daqhosts = int(possible_daqhosts_by_run.get(runnumber, 0))
            total_daqhosts = int(total_daqhosts_by_run.get(runnumber, 0))
            missing_daqhosts = max(possible_daqhosts - total_daqhosts, 0)
            possible_segments = int(possible_segments_by_run.get(runnumber, 0))
            total_segments = int(total_segments_by_run.get(runnumber, 0))
            missing_segments = max(possible_segments - total_segments, 0)
            possible_events = int(possible_events_by_run.get(runnumber, 0))
            total_events = int(total_events_by_run.get(runnumber, 0))
            missing_event_tolerance = int(missing_event_tolerance_by_run.get(runnumber, 0))
            missing_events = max(possible_events - total_events - missing_event_tolerance, 0)
            error_codes = error_codes_by_run.get(runnumber, set())

            reasons = set(reasons_by_run.get(runnumber, set()))
            status_reasons = set(reasons)
            if missing_segments:
                reasons.add("missing_segments")
                status_reasons.add("missing_segments")
            if error_codes:
                reasons.add("error_codes")
                status_reasons.add("error_codes")

            if missing_events:
                status = "partial" if total_events else "missing"
            else:
                status = "questionable" if status_reasons - {"missing_production_jobs"} else "complete"
            status_counts[status] += 1
            if status == "complete":
                complete_runs += 1

            writer.writerow({
                "rule_name": args.rulename,
                "runnumber": runnumber,
                "possible_daqhosts": possible_daqhosts,
                "total_daqhosts": total_daqhosts,
                "missing_daqhosts": missing_daqhosts,
                "possible_segments": possible_segments,
                "total_segments": total_segments,
                "missing_segments": missing_segments,
                "possible_events": possible_events,
                "total_events": total_events,
                "missing_events": missing_events,
                "error_codes": csv_join(error_codes),
                "incomplete_reasons": csv_join(reasons),
                "status": status,
            })
            rows_written += 1

    INFO(f"Wrote {rows_written} run-level rows to {path}")
    remainder = rows_written - complete_runs
    INFO(
        f"Complete runs: {complete_runs}; remainder: {remainder} "
        f"(partial: {status_counts['partial']}; "
        f"missing: {status_counts['missing']}; "
        f"questionable: {status_counts['questionable']})"
    )


def normalized_neventsper(job_config):
    neventsper = getattr(job_config, "neventsper", None)
    try:
        return int(neventsper) if neventsper is not None else 0
    except (TypeError, ValueError):
        return 0



def segment_set(value):
    if value is None:
        return set()
    if isinstance(value, str):
        return {int(part) for part in value.strip("{}").split(",") if part.strip()}
    return {int(segment) for segment in value if segment is not None}


def add_partial_segment_analysis(
    possible_segments_by_run,
    possible_events_by_run,
    total_events_by_run,
    present_segments_by_run,
    missing_event_tolerance_by_run,
    reasons_by_run,
    example_limit,
):
    counts = defaultdict(int)
    examples = defaultdict(list)
    for runnumber in sorted(set(possible_events_by_run) | set(total_events_by_run)):
        possible_events = int(possible_events_by_run.get(runnumber, 0))
        total_events = int(total_events_by_run.get(runnumber, 0))
        tolerance = int(missing_event_tolerance_by_run.get(runnumber, 0))
        missing_events = max(possible_events - total_events - tolerance, 0)
        if not missing_events or not total_events:
            continue

        possible_segments = int(possible_segments_by_run.get(runnumber, 0))
        present_segments = set(present_segments_by_run.get(runnumber, set()))
        if not possible_segments or not present_segments:
            continue

        expected_start = 0
        expected_last = expected_start + possible_segments - 1
        contiguous_end = expected_start - 1
        while contiguous_end + 1 in present_segments:
            contiguous_end += 1

        max_present = max(present_segments)
        if contiguous_end < max_present:
            reason = "missing_segment_gaps"
        elif max_present < expected_last:
            reason = "missing_segment_tail"
        else:
            reason = "short_segment_events"
            ERROR(
                f"short_segment_events: run={runnumber}, "
                f"possible_events={possible_events}, total_events={total_events}, "
                f"missing_events={missing_events}, tolerance={tolerance}, "
                f"possible_segments={possible_segments}, contiguous_end={contiguous_end}, "
                f"max_present={max_present}, expected_last={expected_last}"
            )

        add_reason(reasons_by_run, runnumber, reason)
        counts[reason] += 1
        if len(examples[reason]) < example_limit:
            examples[reason].append((runnumber, contiguous_end, max_present, expected_last))

    if not counts:
        INFO("Partial segment analysis: no partial runs with segment lists to classify.")
        return

    INFO(
        "Partial segment analysis: "
        f"tail={counts['missing_segment_tail']}, "
        f"gaps={counts['missing_segment_gaps']}, "
        f"short_events={counts['short_segment_events']}"
    )
    for reason in ("missing_segment_tail", "missing_segment_gaps", "short_segment_events"):
        for runnumber, contiguous_end, max_present, expected_last in examples.get(reason, []):
            DEBUG(
                f"  {reason}: run={runnumber}, contiguous_end={contiguous_end}, "
                f"max_present={max_present}, expected_last={expected_last}"
            )


def generate_eventcombiner_report(args, rule, match, report_path):
    daqhosts_dict, eventsinrun_by_run = match.daqhosts_for_combining()
    if not eventsinrun_by_run:
        INFO("No runs pass run quality cuts; no report written.")
        return False

    neventsper = normalized_neventsper(rule.job_config)
    if neventsper:
        total_expected_outputs = sum(
            math.ceil(eventsinrun / neventsper)
            for eventsinrun in eventsinrun_by_run.values()
            if eventsinrun
        )
        INFO(f"{total_expected_outputs} expected downstream output files from events/neventsper={neventsper}.")

    daqhost_types = [host for host in match.in_types if host != "gl1daq"]
    n_ideal = sum(sum(1 for host in hosts if host != "gl1daq") for hosts in daqhosts_dict.values())
    INFO(f"{n_ideal} (run, daqhost) combinations have all segments on lustre.")

    run_condition = match._run_condition(list(eventsinrun_by_run))

    total_query = f"""
        SELECT DISTINCT runnumber, daqhost FROM datasets
        WHERE {run_condition}
          AND daqhost IN {tuple(daqhost_types)}
        ORDER BY runnumber, daqhost
    """
    all_combos = dbQuery(cnxn_string_map["rawr"], total_query).fetchall()
    INFO(f"{len(all_combos)} (run, daqhost) combinations found in the raw DB.")

    not_on_lustre = [(int(r), h) for r, h in all_combos if h not in daqhosts_dict.get(int(r), set())]
    INFO(f"{len(not_on_lustre)} (run, daqhost) combinations are in the DB but not fully on lustre.")
    for run, daqhost in not_on_lustre[:args.example_limit]:
        DEBUG(f"  Not fully on lustre: Run {run} {daqhost}")

    runs_without_gl1daq = {run for run, hosts in daqhosts_dict.items() if "gl1daq" not in hosts}
    for run in sorted(runs_without_gl1daq):
        WARN(f"Run {run}: gl1daq not complete on lustre - run will not be submitted.")

    lustre_combos = [
        (run, daqhost)
        for run, hosts in daqhosts_dict.items()
        if "gl1daq" in hosts
        for daqhost in hosts
        if daqhost != "gl1daq"
    ]

    lastevent_query = f"""
        SELECT runnumber, dsttype, max(lastevent)
        FROM datasets
        WHERE dataset='{match.dataset}'
          AND tag='{match.outtriplet}'
          AND dsttype like '{match.dst_type_template}'
          AND {run_condition}
        GROUP BY runnumber, dsttype
        ORDER BY runnumber, dsttype
    """
    rows = dbQuery(cnxn_string_map["fcr"], lastevent_query).fetchall()
    INFO(f"{len(rows)} (run, dsttype) combinations have existing output in the FileCatalog.")

    fc_dsttypes_by_run = defaultdict(list)
    total_events_by_run = defaultdict(int)
    for runnumber, dsttype, lastevent in rows:
        runnumber = int(runnumber)
        fc_dsttypes_by_run[runnumber].append(dsttype)
        total_events_by_run[runnumber] += int(lastevent or 0)

    lustre_no_fc = [
        (run, daqhost)
        for run, daqhost in lustre_combos
        if not any(daqhost_to_dst_leaf(daqhost, match) in dsttype for dsttype in fc_dsttypes_by_run.get(run, []))
    ]
    INFO(f"{len(lustre_no_fc)} lustre combos have no FileCatalog entry.")

    all_no_fc = [
        (int(r), host)
        for r, host in all_combos
        if not any(daqhost_to_dst_leaf(host, match) in dsttype for dsttype in fc_dsttypes_by_run.get(int(r), []))
    ]
    if all_no_fc:
        WARN(f"{len(all_no_fc)} raw DB combos (lustre or not) have no FileCatalog entry. Check for corruption?")
        for run, daqhost in sorted(all_no_fc)[:args.example_limit]:
            DEBUG(f"  Run {run} {daqhost}")

    INFO(f"Checking for combinations flagged below ratio cut {args.ratio_cut}...")
    flagged = []
    for runnumber, dsttype, lastevent in rows:
        runnumber = int(runnumber)
        eventsinrun = eventsinrun_by_run.get(runnumber)
        if not eventsinrun:
            WARN(f"Run {runnumber} {dsttype}: eventsinrun=0, cannot compute ratio.")
            continue

        ratio = lastevent / eventsinrun
        msg = f"Run {runnumber} {dsttype}: lastevent={lastevent}, eventsinrun={eventsinrun}, ratio={ratio:.3f}"
        if ratio < args.ratio_cut:
            DEBUG(msg)
            flagged.append((runnumber, dsttype))
        elif ratio < 0.999:
            CHATTY(msg)

    flagged = sorted(set(flagged))
    INFO(f"{len(flagged)} (run, dsttype) combinations flagged below ratio cut {args.ratio_cut}.")

    possible_daqhost_sets_by_run = defaultdict(set)
    possible_daqhosts_by_run = defaultdict(int)
    total_daqhosts_by_run = defaultdict(int)
    possible_events_by_run = defaultdict(int)
    for runnumber, daqhost in all_combos:
        runnumber = int(runnumber)
        possible_daqhost_sets_by_run[runnumber].add(daqhost)
        possible_daqhosts_by_run[runnumber] += 1
        possible_events_by_run[runnumber] += int(eventsinrun_by_run.get(runnumber, 0))

    missing_daqhost_sets_by_run = defaultdict(set)
    for runnumber, daqhost in all_no_fc:
        missing_daqhost_sets_by_run[int(runnumber)].add(daqhost)
    for runnumber, possible_daqhosts in possible_daqhosts_by_run.items():
        total_daqhosts_by_run[runnumber] = max(
            possible_daqhosts - len(missing_daqhost_sets_by_run.get(runnumber, set())),
            0,
        )

    possible_segments_by_run = defaultdict(int)
    if neventsper:
        for runnumber, eventsinrun in eventsinrun_by_run.items():
            possible_segments_by_run[int(runnumber)] = math.ceil(int(eventsinrun or 0) / neventsper)

    output_events_by_run_host = defaultdict(dict)
    for runnumber, dsttype, lastevent in rows:
        runnumber = int(runnumber)
        for daqhost in possible_daqhost_sets_by_run.get(runnumber, set()):
            if daqhost_to_dst_leaf(daqhost, match) in dsttype:
                output_events_by_run_host[runnumber][daqhost] = max(
                    output_events_by_run_host[runnumber].get(daqhost, 0),
                    int(lastevent or 0),
                )

    total_segments_by_run = defaultdict(int)
    for runnumber, possible_hosts in possible_daqhost_sets_by_run.items():
        possible_segments = possible_segments_by_run.get(runnumber, 0)
        if not neventsper or not possible_segments or not possible_hosts:
            continue
        segment_depths = []
        for daqhost in possible_hosts:
            output_events = output_events_by_run_host.get(runnumber, {}).get(daqhost, 0)
            adjusted_events = output_events + args.missing_event_tolerance if output_events else 0
            segment_depths.append(min(possible_segments, math.ceil(adjusted_events / neventsper)))
        total_segments_by_run[runnumber] = min(segment_depths) if segment_depths else 0

    reasons_by_run = defaultdict(set)
    for runnumber, _ in flagged:
        add_reason(reasons_by_run, runnumber, "low_event_ratio")
    for runnumber, _ in not_on_lustre:
        add_reason(reasons_by_run, runnumber, "not_on_lustre")
    for runnumber in runs_without_gl1daq:
        add_reason(reasons_by_run, runnumber, "missing_gl1daq")
    for runnumber, possible_events in possible_events_by_run.items():
        total_events = total_events_by_run.get(runnumber, 0)
        missing_event_tolerance = args.missing_event_tolerance
        if possible_events and (total_events + missing_event_tolerance) / possible_events < args.ratio_cut:
            add_reason(reasons_by_run, runnumber, "low_run_event_ratio")

    missing_event_tolerance_by_run = {
        runnumber: args.missing_event_tolerance
        for runnumber in possible_daqhosts_by_run
    }
    error_codes_by_run = query_error_codes(args, match, run_condition)

    all_report_runs = set(eventsinrun_by_run) | set(possible_events_by_run) | set(total_events_by_run)
    write_csv_report(
        report_path,
        args,
        all_report_runs,
        possible_daqhosts_by_run,
        total_daqhosts_by_run,
        possible_segments_by_run,
        total_segments_by_run,
        possible_events_by_run,
        total_events_by_run,
        error_codes_by_run,
        reasons_by_run,
        missing_event_tolerance_by_run,
    )

    files_db_events = sum(total_events_by_run.values())
    raw_combo_events = sum(possible_events_by_run.values())
    event_pct = 100.0 * files_db_events / raw_combo_events if raw_combo_events else 0.0
    INFO(
        f"Summary: FileCatalog has {human_event_count(files_db_events)}/"
        f"{human_event_count(raw_combo_events)} possible events from raw DB combos "
        f"({event_pct:.1f}%)."
    )
    INFO(f"Available: {raw_combo_events} \t Done {files_db_events}")
    return True


def generate_downstream_report(args, rule, match, report_path):
    goodruns = match.good_runlist()
    if not goodruns:
        INFO("No runs pass run quality cuts; no report written.")
        return False

    run_condition = match._run_condition(list(goodruns))
    neventsper_default = normalized_neventsper(rule.job_config)

    possible_query = f"""
        SELECT runnumber,
               MAX(eventsinrun) AS eventsinrun,
               MAX(neventsper) AS neventsper,
               MAX(maxjobsexpected) AS maxjobsexpected
        FROM production_jobs
        WHERE rulename={sql_literal(args.rulename)}
          AND dataset={sql_literal(match.dataset)}
          AND tag={sql_literal(match.outtriplet)}
          AND dsttype={sql_literal(match.dsttype)}
          AND {run_condition}
        GROUP BY runnumber
        ORDER BY runnumber
    """
    possible_rows = dbQuery(cnxn_string_map["statr"], possible_query).fetchall()
    INFO(f"{len(possible_rows)} runs found in production_jobs for downstream possible counts.")

    possible_events_by_run = defaultdict(int)
    possible_segments_by_run = defaultdict(int)
    neventsper_by_run = {}
    for row in possible_rows:
        runnumber = int(getattr(row, "runnumber", row[0]))
        eventsinrun = getattr(row, "eventsinrun", row[1])
        neventsper = getattr(row, "neventsper", row[2])
        maxjobsexpected = getattr(row, "maxjobsexpected", row[3])

        if eventsinrun is not None:
            possible_events_by_run[runnumber] = int(eventsinrun)
        if neventsper is not None:
            neventsper_by_run[runnumber] = int(neventsper)
        elif neventsper_default:
            neventsper_by_run[runnumber] = neventsper_default
        if maxjobsexpected is not None:
            possible_segments_by_run[runnumber] = int(maxjobsexpected)

    for runnumber, eventsinrun in goodruns.items():
        if not possible_events_by_run.get(runnumber) and eventsinrun is not None:
            possible_events_by_run[runnumber] = int(eventsinrun)
        neventsper = neventsper_by_run.get(runnumber, neventsper_default)
        if not possible_segments_by_run.get(runnumber) and possible_events_by_run.get(runnumber) and neventsper:
            possible_segments_by_run[runnumber] = math.ceil(possible_events_by_run[runnumber] / neventsper)

    input_dsttypes = list(match.in_types or [])
    possible_daqhosts_by_run = defaultdict(int, {int(runnumber): len(input_dsttypes) for runnumber in goodruns})
    total_daqhosts_by_run = defaultdict(int)
    if input_dsttypes:
        input_tag_clause = (
            f"AND tag={sql_literal(match.input_config.intriplet)}"
            if match.input_config.intriplet
            else ""
        )
        input_constraints = match.input_config.infile_query_constraints or ""
        input_coverage_query = f"""
            SELECT runnumber,
                   COUNT(DISTINCT dsttype) AS total_daqhosts
            FROM {match.input_config.table}
            WHERE dsttype IN {sql_in_list(input_dsttypes)}
              {input_tag_clause}
              AND {run_condition}
              {input_constraints}
            GROUP BY runnumber
            ORDER BY runnumber
        """
        input_coverage_rows = dbQuery(cnxn_string_map[match.input_config.db], input_coverage_query).fetchall()
        INFO(f"{len(input_coverage_rows)} runs found in FileCatalog for downstream input dsttype coverage.")
        for row in input_coverage_rows:
            runnumber = int(getattr(row, "runnumber", row[0]))
            total_daqhosts = getattr(row, "total_daqhosts", row[1])
            total_daqhosts_by_run[runnumber] = int(total_daqhosts or 0)
    else:
        WARN("No input dsttypes configured for downstream input coverage counts.")

    output_query = f"""
        SELECT runnumber,
               COUNT(DISTINCT segment) AS total_segments,
               SUM(events) AS total_events,
               ARRAY_AGG(DISTINCT segment ORDER BY segment) AS segments
        FROM datasets
        WHERE dataset={sql_literal(match.dataset)}
          AND tag={sql_literal(match.outtriplet)}
          AND dsttype={sql_literal(match.dsttype)}
          AND {run_condition}
        GROUP BY runnumber
        ORDER BY runnumber
    """
    output_rows = dbQuery(cnxn_string_map["fcr"], output_query).fetchall()
    INFO(f"{len(output_rows)} runs found in FileCatalog for downstream output counts.")

    total_segments_by_run = defaultdict(int)
    total_events_by_run = defaultdict(int)
    present_segments_by_run = defaultdict(set)
    for row in output_rows:
        runnumber = int(getattr(row, "runnumber", row[0]))
        total_segments = getattr(row, "total_segments", row[1])
        total_events = getattr(row, "total_events", row[2])
        segments = getattr(row, "segments", row[3])
        total_segments_by_run[runnumber] = int(total_segments or 0)
        total_events_by_run[runnumber] = int(total_events or 0)
        present_segments_by_run[runnumber] = segment_set(segments)

    reasons_by_run = defaultdict(set)
    prod_runs = {int(getattr(row, "runnumber", row[0])) for row in possible_rows}
    for runnumber in sorted(set(goodruns) - prod_runs):
        add_reason(reasons_by_run, runnumber, "missing_production_jobs")
    for runnumber, possible_events in possible_events_by_run.items():
        total_events = total_events_by_run.get(runnumber, 0)
        if possible_events and (total_events + args.missing_event_tolerance) / possible_events < args.ratio_cut:
            add_reason(reasons_by_run, runnumber, "low_run_event_ratio")

    missing_event_tolerance_by_run = {
        runnumber: args.missing_event_tolerance
        for runnumber in set(goodruns) | set(possible_events_by_run) | set(total_events_by_run)
    }
    add_partial_segment_analysis(
        possible_segments_by_run,
        possible_events_by_run,
        total_events_by_run,
        present_segments_by_run,
        missing_event_tolerance_by_run,
        reasons_by_run,
        args.example_limit,
    )
    error_codes_by_run = query_error_codes(args, match, run_condition)

    all_report_runs = set(goodruns) | set(possible_events_by_run) | set(total_events_by_run)
    write_csv_report(
        report_path,
        args,
        all_report_runs,
        possible_daqhosts_by_run,
        total_daqhosts_by_run,
        possible_segments_by_run,
        total_segments_by_run,
        possible_events_by_run,
        total_events_by_run,
        error_codes_by_run,
        reasons_by_run,
        missing_event_tolerance_by_run,
    )

    files_db_events = sum(total_events_by_run.values())
    possible_events = sum(possible_events_by_run.values())
    event_pct = 100.0 * files_db_events / possible_events if possible_events else 0.0
    INFO(
        f"Summary: FileCatalog has {human_event_count(files_db_events)}/"
        f"{human_event_count(possible_events)} possible downstream events "
        f"({event_pct:.1f}%)."
    )
    INFO(f"Available: {possible_events} \t Done {files_db_events}")
    return True


def main():
    args = submission_args()
    args.example_limit = max(0, args.example_limit)

    from simpleLogger import slogger, set_log_timestamps_enabled
    import logging
    set_log_timestamps_enabled(False)
    slogger.setLevel(logging.getLevelName(args.loglevel))

    profiler = None
    if args.profile:
        DEBUG("Profiling is ENABLED.")
        profiler = cProfile.Profile()
        profiler.enable()

    rule, match = load_rule_and_match(args)
    report_path = output_path(args)

    if "raw" in match.input_config.db:
        wrote_report = generate_eventcombiner_report(args, rule, match, report_path)
    else:
        wrote_report = generate_downstream_report(args, rule, match, report_path)

    if args.report != "none":
        WARN("--report is accepted for argument compatibility but ignored by generate_report.py; CSV was written instead.")
    if args.delete:
        WARN("--delete is accepted for argument compatibility but ignored by generate_report.py.")

    if wrote_report:
        print(f"Result written to: {report_path}")

    if profiler:
        profiler.disable()
        DEBUG("Profiling finished. Printing stats...")
        stats = pstats.Stats(profiler)
        stats.strip_dirs().sort_stats("time").print_stats(20)


if __name__ == "__main__":
    main()
