#!/usr/bin/env python3
"""
generate_report.py

Generate a run-level CSV report for sPHENIX production rules.

For now this supports raw/event-combiner rules. Downstream rules are detected
and rejected explicitly so the macro name and CLI can be reused later.
"""

import csv
import math
import sys
from collections import defaultdict
from pathlib import Path

from argparsing import submission_args
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixmatching import MatchConfig
from sphenixdbutils import cnxn_string_map, dbQuery
from sphenixmisc import human_event_count


EXPECTED_SKIPPED_EVENTS_PER_DAQHOST = 2


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


def write_csv_report(path, args, runnumbers, possible_daqhosts_by_run,
                     missing_daqhosts_by_run, possible_segments_by_run,
                     total_segments_by_run, possible_events_by_run, total_events_by_run, error_codes_by_run, flagged_by_run,
                     not_on_lustre_by_run, runs_without_gl1daq):
    rows_written = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for runnumber in sorted(runnumbers):
            possible_daqhosts = int(possible_daqhosts_by_run.get(runnumber, 0))
            missing_daqhosts = len(missing_daqhosts_by_run.get(runnumber, set()))
            total_daqhosts = max(possible_daqhosts - missing_daqhosts, 0)
            possible_segments = int(possible_segments_by_run.get(runnumber, 0))
            total_segments = int(total_segments_by_run.get(runnumber, 0))
            missing_segments = max(possible_segments - total_segments, 0)
            possible_events = int(possible_events_by_run.get(runnumber, 0))
            total_events = int(total_events_by_run.get(runnumber, 0))
            expected_skipped_events = EXPECTED_SKIPPED_EVENTS_PER_DAQHOST * possible_daqhosts
            missing_events = max(possible_events - total_events - expected_skipped_events, 0)
            error_codes = error_codes_by_run.get(runnumber, set())

            reasons = set()
            if missing_daqhosts:
                reasons.add("missing_daqhosts")
            if missing_segments:
                reasons.add("missing_segments")
            if runnumber in flagged_by_run:
                reasons.add("low_event_ratio")
            if error_codes:
                reasons.add("error_codes")
            if runnumber in not_on_lustre_by_run:
                reasons.add("not_on_lustre")
            if runnumber in runs_without_gl1daq:
                reasons.add("missing_gl1daq")
            if possible_events and total_events / possible_events < args.ratio_cut:
                reasons.add("low_run_event_ratio")

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
                "status": "incomplete" if missing_events else ("questionable" if reasons else "complete"),
            })
            rows_written += 1

    INFO(f"Wrote {rows_written} run-level rows to {path}")


def main():
    args = submission_args()
    args.example_limit = max(0, args.example_limit)

    from simpleLogger import slogger, set_log_timestamps_enabled
    import logging
    set_log_timestamps_enabled(False)
    slogger.setLevel(logging.getLevelName(args.loglevel))

    rule, match = load_rule_and_match(args)

    if "raw" not in match.input_config.db:
        ERROR(
            f"Rule '{args.rulename}' is a downstream rule (db={match.input_config.db}). "
            "generate_report.py only supports event-combiner/raw rules for now."
        )
        sys.exit(2)

    daqhosts_dict, eventsinrun_by_run = match.daqhosts_for_combining()
    if not eventsinrun_by_run:
        INFO("No runs pass run quality cuts; no report written.")
        sys.exit(0)

    neventsper = getattr(rule.job_config, "neventsper", None)
    try:
        neventsper = int(neventsper) if neventsper is not None else 0
    except Exception:
        neventsper = 0
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
            WARN(msg)
            flagged.append((runnumber, dsttype))
        elif ratio < 0.999:
            CHATTY(msg)

    flagged = sorted(set(flagged))
    INFO(f"{len(flagged)} (run, dsttype) combinations flagged below ratio cut {args.ratio_cut}.")

    possible_daqhost_sets_by_run = defaultdict(set)
    possible_daqhosts_by_run = defaultdict(int)
    possible_events_by_run = defaultdict(int)
    for runnumber, daqhost in all_combos:
        runnumber = int(runnumber)
        possible_daqhost_sets_by_run[runnumber].add(daqhost)
        possible_daqhosts_by_run[runnumber] += 1
        possible_events_by_run[runnumber] += int(eventsinrun_by_run.get(runnumber, 0))

    missing_daqhosts_by_run = defaultdict(set)
    for runnumber, daqhost in all_no_fc:
        missing_daqhosts_by_run[int(runnumber)].add(daqhost)

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
            adjusted_events = output_events + EXPECTED_SKIPPED_EVENTS_PER_DAQHOST if output_events else 0
            segment_depths.append(min(possible_segments, math.ceil(adjusted_events / neventsper)))
        total_segments_by_run[runnumber] = min(segment_depths) if segment_depths else 0

    flagged_by_run = defaultdict(set)
    for runnumber, dsttype in flagged:
        flagged_by_run[int(runnumber)].add(dsttype)

    not_on_lustre_by_run = defaultdict(set)
    for runnumber, daqhost in not_on_lustre:
        not_on_lustre_by_run[int(runnumber)].add(daqhost)

    error_codes_by_run = query_error_codes(args, match, run_condition)

    all_report_runs = set(eventsinrun_by_run) | set(possible_events_by_run) | set(total_events_by_run)
    report_path = output_path(args)
    write_csv_report(
        report_path,
        args,
        all_report_runs,
        possible_daqhosts_by_run,
        missing_daqhosts_by_run,
        possible_segments_by_run,
        total_segments_by_run,
        possible_events_by_run,
        total_events_by_run,
        error_codes_by_run,
        flagged_by_run,
        not_on_lustre_by_run,
        runs_without_gl1daq,
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

    if args.report != "none":
        WARN("--report is accepted for argument compatibility but ignored by generate_report.py; CSV was written instead.")
    if args.delete:
        WARN("--delete is accepted for argument compatibility but ignored by generate_report.py.")

    print(f"Report written to: {report_path}")


if __name__ == "__main__":
    main()
