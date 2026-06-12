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
from sphenixjobdicts import required_seb_hosts
from sphenixprodrules import pRUNFMT, pSEGFMT


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


def filter_runs_by_required_seb(
    input_rows: Iterable[Any],
    required_seb: Set[str],
    min_seb: int,
    raw_daqhosts_by_run: Dict[int, Set[str]],
) -> Set[int]:
    """Apply the calo required-SEB run-level availability check."""
    if not required_seb:
        return set()

    present_by_run: Dict[int, Set[str]] = defaultdict(set)
    prefix = "DST_TRIGGERED_EVENT_"
    for row in input_rows:
        info = _dataset_info_from_row(row)
        if not info.dsttype.startswith(prefix):
            continue
        host = info.dsttype[len(prefix):]
        if host in required_seb:
            present_by_run[info.runnumber].add(host)

    allowed = set()
    all_runs = set(raw_daqhosts_by_run) | set(present_by_run)
    for runnumber in all_runs:
        available_required = raw_daqhosts_by_run.get(runnumber, set()).intersection(required_seb)
        present_required = present_by_run.get(runnumber, set())
        if len(available_required) >= min_seb and len(present_required) >= min_seb:
            allowed.add(runnumber)
        else:
            DEBUG(
                f"Run {runnumber}: required SEB availability failed "
                f"(raw={len(available_required)}, catalog={len(present_required)}, min={min_seb})."
            )
    return allowed


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


def write_report(flagged: List[FlaggedWorkUnit], output: str = None) -> None:
    lines = [unit.report_line() for unit in flagged]
    text = "\n".join(lines)
    if text:
        text += "\n"

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(text)
        INFO(f"Flagged downstream work units written to {output}")
    else:
        print(text, end="")


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

    from simpleLogger import slogger
    import logging
    slogger.setLevel(getattr(logging, args.loglevel))

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
    INFO(f"{len(runnumbers)} runs pass run quality cuts.")

    input_rows = _query_inputs(match, runnumbers)
    INFO(f"{len(input_rows)} input FileCatalog rows found.")

    required_seb = required_seb_hosts(match.dsttype)
    if required_seb:
        raw_daqhosts_by_run = _query_raw_daqhosts(runnumbers)
        allowed_runs = filter_runs_by_required_seb(
            input_rows=input_rows,
            required_seb=required_seb,
            min_seb=match.input_config.min_seb,
            raw_daqhosts_by_run=raw_daqhosts_by_run,
        )
        input_rows = [
            row for row in input_rows
            if int(_row_value(row, "runnumber", 1)) in allowed_runs
        ]
        INFO(f"{len(allowed_runs)} runs pass required SEB availability checks.")

    eligible_units = build_eligible_units(
        input_rows=input_rows,
        required_input_types=match.in_types,
        cut_segment=match.input_config.cut_segment,
    )
    INFO(f"{len(eligible_units)} eligible downstream run/segment units found.")

    output_rows = _query_outputs(match, runnumbers)
    INFO(f"{len(output_rows)} primary output FileCatalog rows found.")

    flagged = find_flagged_units(
        eligible_units=eligible_units,
        output_rows=output_rows,
        ratio_cut=args.ratio_cut,
    )

    reason_counts = Counter(reason for unit in flagged for reason in unit.reasons)
    INFO(f"{len(flagged)} downstream work units flagged below ratio cut {args.ratio_cut}.")
    for reason, count in sorted(reason_counts.items()):
        INFO(f"{reason}: {count}")

    if flagged:
        write_report(flagged, args.output)

    if args.delete:
        WARN("--delete is ignored by check_downstream.py; this checker is read-only.")


if __name__ == "__main__":
    main()
