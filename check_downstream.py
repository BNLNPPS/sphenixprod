#!/usr/bin/env python3
"""
check_downstream.py

For downstream productions, compare required input DSTs against the primary
output DST at (run, segment) granularity.  The checker is read-only: it reports
missing or short primary outputs and leaves cleanup/resubmission to other tools.
"""

import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

from argparsing import submission_args
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR  # noqa: F401
from sphenixjobdicts import required_seb_hosts as required_daqhosts
from sphenixprodrules import pRUNFMT, pSEGFMT
from sphenixmisc import human_event_count


@dataclass(frozen=True)
class DatasetInfo:
    dsttype: str
    runnumber: int
    segment: int
    events: int
    filename: str = ""


@dataclass(frozen=True)
class FlaggedWorkUnit:
    runnumber: int
    segment: int
    reasons: Tuple[str, ...]
    input_events: int
    output_events: int

    def report_line(self) -> str:
        return (
            f"{self.runnumber:{pRUNFMT}} {self.segment:{pSEGFMT}} "
            f"{','.join(self.reasons)} {self.input_events} {self.output_events}"
        )


@dataclass(frozen=True)
class RequiredDaqhostFilterResult:
    allowed_runs: Set[int]
    failing_runs: Set[int]
    failing_units: List[Tuple[int, int]]


def _row_value(row: Any, name: str, index: int) -> Any:
    if hasattr(row, name):
        return getattr(row, name)
    return row[index]


def _dataset_info_from_row(row: Any) -> DatasetInfo:
    return DatasetInfo(
        dsttype=str(_row_value(row, "dsttype", 0)),
        runnumber=int(_row_value(row, "runnumber", 1)),
        segment=int(_row_value(row, "segment", 2)),
        events=int(_row_value(row, "events", 3)),
        filename=str(_row_value(row, "filename", 4)),
    )


def _sql_in(values: Iterable[str]) -> str:
    quoted = ",".join(f"'{value}'" for value in values)
    return f"({quoted})"


def build_eligible_units(
    input_rows: Iterable[Any],
    required_input_types: Iterable[str],
    cut_segment: int = 1,
) -> Dict[Tuple[int, int], List[DatasetInfo]]:
    """Return run/segment units that have every required input dsttype."""
    required = set(required_input_types)
    by_unit: Dict[Tuple[int, int], Dict[str, DatasetInfo]] = defaultdict(dict)

    for row in input_rows:
        info = _dataset_info_from_row(row)
        if cut_segment and info.segment % cut_segment != 0:
            continue
        if info.dsttype not in required:
            continue
        # Duplicate catalog rows for the same type/run/segment should not change
        # eligibility.  Keep the highest event count for conservative checking.
        previous = by_unit[(info.runnumber, info.segment)].get(info.dsttype)
        if previous is None or info.events > previous.events:
            by_unit[(info.runnumber, info.segment)][info.dsttype] = info

    eligible = {}
    for unit, rows_by_type in by_unit.items():
        if set(rows_by_type) == required:
            eligible[unit] = [rows_by_type[dsttype] for dsttype in sorted(required)]
    return eligible


def filter_runs_by_required_daqhosts(
    input_rows: Iterable[Any],
    required_hosts: Set[str],
    min_hosts: int,
    raw_daqhosts_by_run: Dict[int, Set[str]],
    cut_segment: int = 1,
    example_limit: int = 5,
) -> RequiredDaqhostFilterResult:
    """Apply the required-daqhost run-level availability check."""
    if not required_hosts:
        return RequiredDaqhostFilterResult(allowed_runs=set(), failing_runs=set(), failing_units=[])

    present_by_run: Dict[int, Set[str]] = defaultdict(set)
    units_by_run: Dict[int, Set[int]] = defaultdict(set)
    prefix = "DST_TRIGGERED_EVENT_"
    for row in input_rows:
        info = _dataset_info_from_row(row)
        if not cut_segment or info.segment % cut_segment == 0:
            units_by_run[info.runnumber].add(info.segment)
        if not info.dsttype.startswith(prefix):
            continue
        host = info.dsttype[len(prefix):]
        if host in required_hosts:
            present_by_run[info.runnumber].add(host)

    allowed = set()
    failing = set()
    all_runs = set(raw_daqhosts_by_run) | set(present_by_run)
    failure_examples = 0
    for runnumber in all_runs:
        available_required = raw_daqhosts_by_run.get(runnumber, set()).intersection(required_hosts)
        present_required = present_by_run.get(runnumber, set())
        if len(available_required) >= min_hosts and len(present_required) >= min_hosts:
            allowed.add(runnumber)
        else:
            failing.add(runnumber)
            failure_examples += 1
            if failure_examples <= example_limit:
                DEBUG(
                    f"Run {runnumber}: required daqhost availability failed "
                    f"(raw={len(available_required)}, catalog={len(present_required)}, min={min_hosts})."
                )
            elif failure_examples == example_limit + 1:
                DEBUG(f"Additional required daqhost availability failures suppressed after {example_limit} examples.")
    failing_units = sorted(
        (runnumber, segment)
        for runnumber in failing
        for segment in units_by_run.get(runnumber, set())
    )
    return RequiredDaqhostFilterResult(
        allowed_runs=allowed,
        failing_runs=failing,
        failing_units=failing_units,
    )


def find_flagged_units(
    eligible_units: Dict[Tuple[int, int], List[DatasetInfo]],
    output_rows: Iterable[Any],
    ratio_cut: float,
) -> List[FlaggedWorkUnit]:
    """Classify eligible units with missing, inconsistent, or short outputs."""
    outputs_by_unit: Dict[Tuple[int, int], DatasetInfo] = {}
    for row in output_rows:
        info = _dataset_info_from_row(row)
        previous = outputs_by_unit.get((info.runnumber, info.segment))
        if previous is None or info.events > previous.events:
            outputs_by_unit[(info.runnumber, info.segment)] = info

    flagged = []
    for (runnumber, segment), inputs in sorted(eligible_units.items()):
        input_event_counts = {info.events for info in inputs}
        input_events = inputs[0].events if len(input_event_counts) == 1 else -1
        output = outputs_by_unit.get((runnumber, segment))
        output_events = output.events if output else -1

        reasons = []
        if len(input_event_counts) != 1:
            reasons.append("input_mismatch")
        if output is None:
            reasons.append("missing_output")
        elif input_events > 0 and output.events / input_events < ratio_cut:
            reasons.append("low_output_events")

        if reasons:
            flagged.append(
                FlaggedWorkUnit(
                    runnumber=runnumber,
                    segment=segment,
                    reasons=tuple(reasons),
                    input_events=input_events,
                    output_events=output_events,
                )
            )
    return flagged


def check_run_level_coverage(
    eligible_units: Dict[Tuple[int, int], List[DatasetInfo]],
    output_rows: Iterable[Any],
    ratio_cut: float,
    example_limit: int = 5,
) -> Set[int]:
    """Check whether summed output events reach summed input events per run.

    Returns the set of runnumbers that fail the coverage check.
    Logs up to example_limit failing examples and a summary if more are found.
    """
    outputs_by_unit: Dict[Tuple[int, int], DatasetInfo] = {}
    for row in output_rows:
        info = _dataset_info_from_row(row)
        previous = outputs_by_unit.get((info.runnumber, info.segment))
        if previous is None or info.events > previous.events:
            outputs_by_unit[(info.runnumber, info.segment)] = info

    input_sum_by_run: Dict[int, int] = defaultdict(int)
    for (runnumber, _), inputs in eligible_units.items():
        # conservatively count the largest input event count for the unit
        input_sum_by_run[runnumber] += max(info.events for info in inputs)

    output_sum_by_run: Dict[int, int] = defaultdict(int)
    for (runnumber, _), info in outputs_by_unit.items():
        output_sum_by_run[runnumber] += info.events

    failing_runs: Set[int] = set()
    examples = 0
    no_input_examples = 0
    all_runs = set(input_sum_by_run) | set(output_sum_by_run)
    for run in sorted(all_runs):
        input_sum = input_sum_by_run.get(run, 0)
        output_sum = output_sum_by_run.get(run, 0)
        if input_sum <= 0:
            no_input_examples += 1
            if no_input_examples <= example_limit:
                DEBUG(f"Run {run}: no input events to check run-level coverage.")
            elif no_input_examples == example_limit + 1:
                DEBUG(f"Additional no-input coverage examples suppressed after {example_limit} examples.")
            continue
        ratio = output_sum / input_sum if input_sum else 0.0
        if ratio < ratio_cut:
            failing_runs.add(run)
            examples += 1
            if examples <= example_limit:
                WARN(
                    f"Run {run}: run-level coverage failed (output={output_sum}, input={input_sum}, ratio={ratio:.3f}, min={ratio_cut})."
                )
            elif examples == example_limit + 1:
                WARN(f"Additional run-level coverage failures suppressed after {example_limit} examples.")

    INFO(f"{len(failing_runs)} runs fail run-level coverage (ratio < {ratio_cut}).")
    return failing_runs


def sum_output_events(output_rows: Iterable[Any]) -> int:
    outputs_by_unit: Dict[Tuple[int, int], DatasetInfo] = {}
    for row in output_rows:
        info = _dataset_info_from_row(row)
        previous = outputs_by_unit.get((info.runnumber, info.segment))
        if previous is None or info.events > previous.events:
            outputs_by_unit[(info.runnumber, info.segment)] = info
    return sum(info.events for info in outputs_by_unit.values())

def check_coverage_against_raw(
    output_rows: Iterable[Any],
    raw_input_by_run: Dict[int, int],
    ratio_cut: float,
    example_limit: int = 5,
) -> Set[int]:
    """Compare summed outputs (files DB) against per-run reference events.

    Logs up to example_limit failing runs and returns the set of failing runnumbers.
    """
    outputs_by_unit: Dict[Tuple[int, int], DatasetInfo] = {}
    for row in output_rows:
        info = _dataset_info_from_row(row)
        previous = outputs_by_unit.get((info.runnumber, info.segment))
        if previous is None or info.events > previous.events:
            outputs_by_unit[(info.runnumber, info.segment)] = info

    output_sum_by_run: Dict[int, int] = defaultdict(int)
    for (runnumber, _), info in outputs_by_unit.items():
        output_sum_by_run[runnumber] += info.events

    failing = set()
    examples = 0
    for run in sorted(raw_input_by_run):
        raw_events = raw_input_by_run.get(run, 0)
        out_events = output_sum_by_run.get(run, 0)
        if raw_events <= 0:
            CHATTY(f"Run {run}: reference events=0, skipping coverage check.")
            continue
        ratio = out_events / raw_events
        if ratio < ratio_cut:
            failing.add(run)
            examples += 1
            if examples <= example_limit:
                WARN(
                    f"Run {run}: files DB covers {out_events}/{raw_events} reference events "
                    f"({ratio:.3f} < {ratio_cut})"
                )
            elif examples == example_limit + 1:
                WARN(f"Additional files-vs-raw coverage failures suppressed after {example_limit} examples.")

    INFO(f"{len(failing)} runs fail files-vs-reference coverage (ratio < {ratio_cut}).")
    return failing


def write_report(flagged: List[FlaggedWorkUnit], output: str = None) -> None:
    lines = [unit.report_line() for unit in flagged]
    text = "\n".join(lines)
    if text:
        text += "\n"

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(text)
        INFO(f"Flagged downstream work units written to {output}")


REASON_SUMMARY_TEXT = {
    "input_mismatch": "combinations had all required input DST types present, but the input files did not agree on the event count",
    "missing_output": "had the required inputs, but did not produce primary output",
}


def _load_rule_and_match(args) -> Tuple[Any, Any]:
    from sphenixmatching import MatchConfig
    from sphenixprodrules import RuleConfig

    param_overrides = {
        "runs": args.runs,
        "runlist": args.runlist,
        "nevents": args.nevents,
        "prodmode": "production",
        "check_legacy": args.check_legacy,
        "cut_segment": args.cut_segment,
    }
    if args.mangle_dirpath:
        param_overrides["prodmode"] = args.mangle_dirpath
    if args.physicsmode:
        param_overrides["physicsmode"] = args.physicsmode

    rule = RuleConfig.from_yaml_file(
        yaml_file=args.config,
        rule_name=args.rulename,
        param_overrides=param_overrides,
    )
    return rule, MatchConfig.from_rule_config(rule)


def _query_raw_daqhosts(runnumbers: Iterable[int]) -> Dict[int, Set[str]]:
    from sphenixdbutils import cnxn_string_map, dbQuery, list_to_condition

    run_condition = list_to_condition(list(runnumbers))
    if not run_condition:
        return {}
    query = f"""
        SELECT DISTINCT runnumber, daqhost
        FROM datasets
        WHERE {run_condition}
    """
    rows = dbQuery(cnxn_string_map["rawr"], query).fetchall()
    hosts_by_run: Dict[int, Set[str]] = defaultdict(set)
    for row in rows:
        hosts_by_run[int(_row_value(row, "runnumber", 0))].add(str(_row_value(row, "daqhost", 1)))
    return hosts_by_run

def _query_inputs(match: Any, runnumbers: Iterable[int]) -> List[Any]:
    from sphenixdbutils import cnxn_string_map, dbQuery, list_to_condition

    run_condition = list_to_condition(list(runnumbers))
    if not run_condition:
        return []

    query = f"""
        SELECT dsttype, runnumber, segment, events, filename
        FROM {match.input_config.table}
        WHERE dataset='{match.dataset}'
          AND tag='{match.input_config.intriplet}'
          AND dsttype IN {_sql_in(match.in_types)}
          AND {run_condition}
          {match.input_config.infile_query_constraints}
        ORDER BY runnumber, segment, dsttype
    """
    return dbQuery(cnxn_string_map[match.input_config.db], query).fetchall()


def _query_outputs(match: Any, runnumbers: Iterable[int]) -> List[Any]:
    from sphenixdbutils import cnxn_string_map, dbQuery, list_to_condition

    run_condition = list_to_condition(list(runnumbers))
    if not run_condition:
        return []

    query = f"""
        SELECT dsttype, runnumber, segment, events, filename
        FROM datasets
        WHERE dataset='{match.dataset}'
          AND tag='{match.outtriplet}'
          AND dsttype='{match.dsttype}'
          AND {run_condition}
        ORDER BY runnumber, segment
    """
    return dbQuery(cnxn_string_map["fcr"], query).fetchall()


def main():
    args = submission_args()
    args.example_limit = max(0, args.example_limit)

    from simpleLogger import slogger
    import logging
    slogger.setLevel(logging.getLevelName(args.loglevel))

    rule, match = _load_rule_and_match(args)

    if "raw" in match.input_config.db:
        ERROR(
            f"Rule '{args.rulename}' is a raw/combining rule "
            f"(db={match.input_config.db}). Use check_eventcombiner.py instead."
        )
        sys.exit(2)

    goodruns = match.good_runlist()
    if not goodruns:
        INFO("No runs pass run quality cuts.")
        sys.exit(0)
    runnumbers = sorted(goodruns)

    neventsper = getattr(match.job_config, "neventsper", None)
    if neventsper is not None:
        try:
            neventsper = int(neventsper)
        except (TypeError, ValueError):
            neventsper = None
    if neventsper:
        total_expected_outputs = sum(
            (goodruns[run] + neventsper - 1) // neventsper
            for run in runnumbers
            if goodruns.get(run, 0) > 0
        )
        INFO(
            f"{total_expected_outputs} expected downstream output files "
            f"from events/neventsper={neventsper}."
        )

    input_rows = _query_inputs(match, runnumbers)
    INFO(f"{len(input_rows)} available input FileCatalog rows found.")

    required_hosts = required_daqhosts(match.dsttype)
    daqhost_failed_runs: Set[int] = set()
    daqhost_failed_units: List[Tuple[int, int]] = []
    if required_hosts:
        raw_daqhosts_by_run = _query_raw_daqhosts(runnumbers)
        daqhost_filter = filter_runs_by_required_daqhosts(
            input_rows=input_rows,
            required_hosts=required_hosts,
            min_hosts=match.input_config.min_seb,
            raw_daqhosts_by_run=raw_daqhosts_by_run,
            cut_segment=match.input_config.cut_segment,
            example_limit=args.example_limit,
        )
        allowed_runs = daqhost_filter.allowed_runs
        daqhost_failed_runs = daqhost_filter.failing_runs
        daqhost_failed_units = daqhost_filter.failing_units
        input_rows = [
            row for row in input_rows
            if int(_row_value(row, "runnumber", 1)) in allowed_runs
        ]
        INFO(f"{len(allowed_runs)} runs pass required daqhost availability checks.")
        INFO(f"{len(daqhost_failed_runs)} runs fail required daqhost availability checks.")
        INFO(f"{len(daqhost_failed_units)} run-segment combinations fail required daqhost availability checks.")

    eligible_units = build_eligible_units(
        input_rows=input_rows,
        required_input_types=match.in_types,
        cut_segment=match.input_config.cut_segment,
    )
    INFO(f"{len(eligible_units)} available input combinations found.")

    output_rows = _query_outputs(match, runnumbers)
    INFO(f"{len(output_rows)} primary output FileCatalog rows found.")

    # Run-level coverage check: compare summed input events to summed outputs
    failing_runs = check_run_level_coverage(
        eligible_units=eligible_units,
        output_rows=output_rows,
        ratio_cut=args.ratio_cut,
        example_limit=args.example_limit,
    )

    # Also compare files DB outputs against eventsinrun from the DAQ DB.
    daq_events_by_run = {run: events for run, events in goodruns.items() if events}
    raw_coverage_summary = None
    raw_event_summary = None
    if daq_events_by_run:
        failing_daq = check_coverage_against_raw(
            output_rows=output_rows,
            raw_input_by_run=daq_events_by_run,
            ratio_cut=args.ratio_cut,
            example_limit=args.example_limit,
        )
        INFO(f"{len(failing_daq)} runs fail coverage against DAQ eventsinrun.")
        daq_runs = len(daq_events_by_run)
        complete_runs = daq_runs - len(failing_daq)
        pct_daq = 100.0 * complete_runs / daq_runs if daq_runs else 0.0
        files_db_events = sum_output_events(output_rows)
        daq_events = sum(daq_events_by_run.values())
        event_pct = 100.0 * files_db_events / daq_events if daq_events else 0.0
        raw_coverage_summary = (
            f"Summary: {complete_runs}/{daq_runs} runs have downstream FileCatalog coverage "
            f"above threshold relative to DAQ eventsinrun ({pct_daq:.1f}%)."
        )
        raw_event_summary = (
            f"Summary: FileCatalog has {human_event_count(files_db_events)}/"
            f"{human_event_count(daq_events)} possible events from DAQ eventsinrun "
            f"({event_pct:.1f}%)."
        )
        raw_event_counts = f"Available: {daq_events} \t Done {files_db_events}"

    flagged = find_flagged_units(
        eligible_units=eligible_units,
        output_rows=output_rows,
        ratio_cut=args.ratio_cut,
    )

    reason_counts = Counter(reason for unit in flagged for reason in unit.reasons)
    INFO(f"{len(flagged)} downstream work units flagged below ratio cut {args.ratio_cut}.")
    for reason, count in sorted(reason_counts.items()):
        INFO(f"{count} {REASON_SUMMARY_TEXT.get(reason, reason)}")

    if flagged:
        write_report(flagged, args.output)

    if args.delete:
        WARN("--delete is ignored by check_downstream.py; this checker is read-only.")

    if raw_coverage_summary is not None:
        INFO(raw_coverage_summary)
        INFO(raw_event_summary)
        INFO(raw_event_counts)
    else:
        n_eligible = len(eligible_units)
        above_threshold = n_eligible - len(flagged)
        pct = 100.0 * above_threshold / n_eligible if n_eligible else 0.0
        INFO(f"Summary: {above_threshold}/{n_eligible} eligible units have output above threshold ({pct:.1f}%).")

    print_report(args.report, flagged, daqhost_failed_runs, daqhost_failed_units)


def print_report(
    report: str,
    flagged: List[FlaggedWorkUnit],
    daqhost_failed_runs: Set[int],
    daqhost_failed_units: List[Tuple[int, int]],
) -> None:
    if report == "none":
        return

    if report == "flagged":
        for unit in flagged:
            print(unit.report_line())
        return

    if report == "input_mismatch":
        for unit in flagged:
            if "input_mismatch" in unit.reasons:
                print(f"{unit.runnumber} {unit.segment}")
        return

    if report == "missing_output":
        for unit in flagged:
            if "missing_output" in unit.reasons:
                print(f"{unit.runnumber} {unit.segment}")
        return

    if report == "daqhost":
        for runnumber, segment in daqhost_failed_units:
            print(f"{runnumber} {segment}")
        return

    if report == "daqhost_runs":
        for runnumber in sorted(daqhost_failed_runs):
            print(runnumber)
        return

    WARN(f"--report {report} is not supported by check_downstream.py; no report printed.")


if __name__ == "__main__":
    main()
