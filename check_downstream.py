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
    raw_available_by_run: Dict[int, int]
    catalog_available_by_run: Dict[int, int]


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
        return RequiredDaqhostFilterResult(allowed_runs=set(), failing_runs=set(), failing_units=[], raw_available_by_run={}, catalog_available_by_run={})

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
    raw_available_by_run: Dict[int, int] = {}
    catalog_available_by_run: Dict[int, int] = {}
    failure_examples = 0
    for runnumber in all_runs:
        available_required = raw_daqhosts_by_run.get(runnumber, set()).intersection(required_hosts)
        raw_available_by_run[runnumber] = len(available_required)
        present_required = present_by_run.get(runnumber, set())
        catalog_available_by_run[runnumber] = len(present_required)
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
        raw_available_by_run=raw_available_by_run,
        catalog_available_by_run=catalog_available_by_run,
    )


def find_flagged_units(
    eligible_units: Dict[Tuple[int, int], List[DatasetInfo]],
    output_rows: Iterable[Any],
    ratio_cut: float,
    example_limit: int = 5,
) -> List[FlaggedWorkUnit]:
    """Classify eligible units with missing, inconsistent, or short outputs."""
    outputs_by_unit: Dict[Tuple[int, int], DatasetInfo] = {}
    for row in output_rows:
        info = _dataset_info_from_row(row)
        previous = outputs_by_unit.get((info.runnumber, info.segment))
        if previous is None or info.events > previous.events:
            outputs_by_unit[(info.runnumber, info.segment)] = info

    flagged = []
    mismatch_examples = 0
    tolerated_mismatch_examples = 0
    for (runnumber, segment), inputs in sorted(eligible_units.items()):
        input_event_counts = {info.events for info in inputs}
        sorted_event_counts = sorted(input_event_counts)
        if len(sorted_event_counts) == 1:
            input_events = sorted_event_counts[0]
        elif sorted_event_counts[-1] - sorted_event_counts[0] <= 2:
            input_events = sorted_event_counts[0]
            tolerated_mismatch_examples += 1
            if tolerated_mismatch_examples <= example_limit:
                DEBUG(
                    f"Run {runnumber}, segment {segment}: input event counts differ slightly; "
                    f"using minimum {input_events} from {sorted_event_counts}."
                )
            elif tolerated_mismatch_examples == example_limit + 1:
                DEBUG(f"Additional tolerated input event count mismatches suppressed after {example_limit} examples.")
        else:
            input_events = -1
        output = outputs_by_unit.get((runnumber, segment))
        output_events = output.events if output else -1

        reasons = []
        if len(sorted_event_counts) != 1 and input_events < 0:
            reasons.append("input_mismatch")
            mismatch_examples += 1
            if mismatch_examples <= example_limit:
                DEBUG(
                    f"Run {runnumber}, segment {segment}: input event counts mismatch "
                    f"{sorted_event_counts}."
                )
            elif mismatch_examples == example_limit + 1:
                DEBUG(f"Additional input event count mismatches suppressed after {example_limit} examples.")
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



REASON_SUMMARY_TEXT = {
    "input_mismatch": "combinations had all required input DST types present, but the input files did not agree on the event count",
    "missing_output": "had the required inputs, but did not produce primary output",
    "low_output_events": "produced primary output below the event ratio cut",
}


def _expected_unit_counts_by_run(
    events_by_run: Dict[int, int],
    events_per_job: int,
    cut_segment: int,
) -> Dict[int, int]:
    if not events_per_job:
        return {}
    expected = {}
    for runnumber, events in events_by_run.items():
        if events <= 0:
            continue
        expected_segments = (events + events_per_job - 1) // events_per_job
        if cut_segment and cut_segment > 1:
            expected_segments = (expected_segments + cut_segment - 1) // cut_segment
        expected[runnumber] = expected_segments
    return expected


def _estimate_events_from_unit_counts(
    unit_counts_by_run: Dict[int, int],
    events_by_run: Dict[int, int],
    events_per_job: int,
) -> float:
    if not events_per_job:
        return 0.0
    return sum(
        min(unit_count * events_per_job, events_by_run.get(runnumber, 0))
        for runnumber, unit_count in unit_counts_by_run.items()
    )


def _expected_events_for_unit(
    events_by_run: Dict[int, int],
    runnumber: int,
    segment: int,
    events_per_job: int,
) -> int:
    if not events_per_job:
        return 0
    run_events = events_by_run.get(runnumber, 0)
    segment_start = segment * events_per_job
    if run_events <= 0 or segment_start >= run_events:
        return 0
    return min(events_per_job, run_events - segment_start)


def _input_events_for_eligible_unit(inputs: List[DatasetInfo]) -> int:
    if not inputs:
        return 0
    return max(min(info.events for info in inputs), 0)


def _format_event_fraction(events: float, total_events: int) -> str:
    pct = 100.0 * events / total_events if total_events else 0.0
    return f"{human_event_count(round(events))} events ({pct:.2f}% of total)"


def _format_run_segment_examples(units: List[Tuple[int, int]]) -> str:
    return ", ".join(f"{runnumber}/{segment}" for runnumber, segment in units)


def _work_units_from_rows(rows: Iterable[Any], cut_segment: int = 1) -> Set[Tuple[int, int]]:
    units = set()
    for row in rows:
        info = _dataset_info_from_row(row)
        if cut_segment and info.segment % cut_segment != 0:
            continue
        units.add((info.runnumber, info.segment))
    return units


def _possible_unit_count(goodruns: Dict[int, int], events_per_job: int, cut_segment: int) -> int:
    return sum(_expected_unit_counts_by_run(goodruns, events_per_job, cut_segment).values())


def _format_file_count_ratio(numerator: int, denominator: int) -> str:
    pct = 100.0 * numerator / denominator if denominator else 0.0
    return f"{numerator} / {denominator} files ({pct:.2f}%)"


def count_failed_jobs_for_units(
    units: Set[Tuple[int, int]],
    match: Any,
    batch_size: int = 500,
) -> int:
    if not units:
        return 0

    quote = chr(39)
    total = 0
    sorted_units = sorted(units)
    for start in range(0, len(sorted_units), batch_size):
        batch = sorted_units[start:start + batch_size]
        query = f"""
            SELECT COUNT(*)
            FROM production_jobs
            WHERE dataset={quote}{match.dataset}{quote}
              AND tag={quote}{match.outtriplet}{quote}
              AND dsttype={quote}{match.dsttype}{quote}
              AND status={quote}failed{quote}
              AND {_unit_tuple_condition(batch)}
        """
        total += _count_query_result(query, "statr")
    return total


def log_single_input_file_summary(
    goodruns: Dict[int, int],
    input_rows: Iterable[Any],
    output_rows: Iterable[Any],
    input_type: str,
    match: Any,
    events_per_job: int,
    cut_segment: int,
    example_limit: int = 5,
) -> None:
    """Summarize one-input downstream jobs by file-count coverage."""
    available_input_units = _work_units_from_rows(input_rows, cut_segment)
    output_units = _work_units_from_rows(output_rows, cut_segment)
    outputs_for_available_inputs = output_units.intersection(available_input_units)
    missing_available_inputs = available_input_units - output_units
    unmatched_outputs = output_units - available_input_units

    INFO(
        f"Summary: single-input downstream shortcut for input dsttype={input_type}; "
        "using file-count coverage."
    )
    INFO(
        f"Summary: primary output files vs available input files: "
        f"{_format_file_count_ratio(len(outputs_for_available_inputs), len(available_input_units))}."
    )

    if events_per_job:
        possible_inputs = _possible_unit_count(goodruns, events_per_job, cut_segment)
        INFO(
            f"Summary: available input files vs DAQ-possible input files: "
            f"{_format_file_count_ratio(len(available_input_units), possible_inputs)}."
        )
        INFO(
            f"Summary: primary output files vs DAQ-possible input files: "
            f"{_format_file_count_ratio(len(output_units), possible_inputs)}."
        )
    else:
        INFO("Summary: DAQ-possible input file count unavailable because events/neventsper is not configured.")

    INFO(
        f"Summary: {len(missing_available_inputs)} available input files do not have "
        f"a matching primary output file."
    )
    failed_missing_jobs = count_failed_jobs_for_units(missing_available_inputs, match)
    INFO(
        f"Summary: {failed_missing_jobs} production_jobs rows for input files without output "
        "are in status failed."
    )
    if example_limit and missing_available_inputs:
        examples = sorted(missing_available_inputs)[:example_limit]
        DEBUG(
            f"Examples: available input files without primary output "
            f"(run/segment): {_format_run_segment_examples(examples)}."
        )
        if len(missing_available_inputs) > len(examples):
            DEBUG(f"Additional missing-output examples suppressed after {len(examples)} examples.")

    if unmatched_outputs:
        INFO(
            f"Summary: {len(unmatched_outputs)} primary output files do not match "
            f"an available input file."
        )
        if example_limit:
            examples = sorted(unmatched_outputs)[:example_limit]
            DEBUG(
                f"Examples: primary outputs without available input "
                f"(run/segment): {_format_run_segment_examples(examples)}."
            )
            if len(unmatched_outputs) > len(examples):
                DEBUG(f"Additional unmatched-output examples suppressed after {len(examples)} examples.")


def log_event_summary(
    goodruns: Dict[int, int],
    output_rows: Iterable[Any],
    daqhost_failed_runs: Set[int],
    daqhost_failed_units: List[Tuple[int, int]],
    eligible_units: Dict[Tuple[int, int], List[DatasetInfo]],
    flagged: List[FlaggedWorkUnit],
    ratio_cut: float,
    events_per_job: int,
    cut_segment: int,
    failing_daq_runs: Set[int],
    example_limit: int = 5,
) -> None:
    daq_runs = len([events for events in goodruns.values() if events])
    if daq_runs:
        complete_runs = daq_runs - len(failing_daq_runs)
        pct_complete = 100.0 * complete_runs / daq_runs
        INFO(
            f"Summary: {complete_runs}/{daq_runs} runs have downstream FileCatalog coverage "
            f"above threshold relative to DAQ eventsinrun ({pct_complete:.1f}%)."
        )

    total_possible_events = sum(goodruns.values())
    produced_events = sum_output_events(output_rows)
    produced_pct = 100.0 * produced_events / total_possible_events if total_possible_events else 0.0

    outputs_by_unit: Dict[Tuple[int, int], DatasetInfo] = {}
    for row in output_rows:
        info = _dataset_info_from_row(row)
        previous = outputs_by_unit.get((info.runnumber, info.segment))
        if previous is None or info.events > previous.events:
            outputs_by_unit[(info.runnumber, info.segment)] = info

    eligible_expected_events = 0
    eligible_input_events = 0
    eligible_output_events = 0
    short_input_units = 0
    output_deficit_units = 0
    for (runnumber, segment), inputs in eligible_units.items():
        expected_events = _expected_events_for_unit(goodruns, runnumber, segment, events_per_job)
        input_events = _input_events_for_eligible_unit(inputs)
        output = outputs_by_unit.get((runnumber, segment))
        output_events = output.events if output else 0
        eligible_expected_events += expected_events
        eligible_input_events += input_events
        eligible_output_events += max(output_events, 0)
        if expected_events > input_events:
            short_input_units += 1
        if input_events > output_events:
            output_deficit_units += 1

    input_event_deficit = max(eligible_expected_events - eligible_input_events, 0)
    output_event_deficit = max(eligible_input_events - eligible_output_events, 0)
    eligible_output_pct = 100.0 * eligible_output_events / eligible_input_events if eligible_input_events else 0.0
    eligible_missing_pct = 100.0 * output_event_deficit / eligible_input_events if eligible_input_events else 0.0

    INFO(
        f"Summary: All primary output FileCatalog events: {human_event_count(produced_events)} / "
        f"{human_event_count(total_possible_events)} DAQ possible events "
        f"({produced_pct:.2f}%)."
    )
    INFO(f"Summary:                    FileCatalog events: {produced_events} / {total_possible_events} ")
    INFO(
        f"Summary: Eligible downstream universe: {human_event_count(eligible_output_events)} / "
        f"{human_event_count(eligible_input_events)} eligible input events covered "
        f"({eligible_output_pct:.2f}%)."
    )
    INFO(
        f"Summary: Eligible downstream missing: {human_event_count(output_event_deficit)} events "
        f"({eligible_missing_pct:.2f}% of eligible input events)."
    )

    if not events_per_job:
        INFO("Summary: Missing-event breakdown estimates unavailable because events/neventsper is not configured.")
        return

    INFO("Summary: Estimated missing-event breakdown:")
    breakdown_lines = 0
    estimated_breakdown_events = 0.0
    eligible_downstream_categorized_events = 0.0
    expected_units_by_run = _expected_unit_counts_by_run(goodruns, events_per_job, cut_segment)

    if daqhost_failed_units:
        failed_units_by_run = Counter(runnumber for runnumber, _ in daqhost_failed_units)
        estimated_events = _estimate_events_from_unit_counts(
            failed_units_by_run,
            goodruns,
            events_per_job,
        )
        INFO(
            f"Summary: required daqhost availability: {len(daqhost_failed_runs)} runs, "
            f"{len(daqhost_failed_units)} observed run-segment combinations, "
            f"estimated {_format_event_fraction(estimated_events, total_possible_events)}."
        )
        breakdown_lines += 1
        estimated_breakdown_events += estimated_events

    eligible_units_by_run = Counter(runnumber for runnumber, _ in eligible_units)
    input_check_runs = set(expected_units_by_run) - daqhost_failed_runs
    missing_required_inputs_by_run = {}
    for runnumber in input_check_runs:
        missing_units = expected_units_by_run.get(runnumber, 0) - eligible_units_by_run.get(runnumber, 0)
        if missing_units > 0:
            missing_required_inputs_by_run[runnumber] = missing_units

    if missing_required_inputs_by_run:
        missing_required_inputs = sum(missing_required_inputs_by_run.values())
        estimated_events = _estimate_events_from_unit_counts(
            missing_required_inputs_by_run,
            goodruns,
            events_per_job,
        )
        INFO(
            f"Summary: missing required input combinations: {missing_required_inputs} expected "
            f"run-segment combinations from {len(missing_required_inputs_by_run)} runs, "
            f"estimated {_format_event_fraction(estimated_events, total_possible_events)}."
        )
        breakdown_lines += 1
        estimated_breakdown_events += estimated_events

    if flagged:
        flagged_units_by_run = Counter(unit.runnumber for unit in flagged)
        estimated_events = _estimate_events_from_unit_counts(
            flagged_units_by_run,
            goodruns,
            events_per_job,
        )
        INFO(
            f"Summary: downstream work units below ratio cut {ratio_cut}: {len(flagged)} work units, "
            f"estimated {_format_event_fraction(estimated_events, total_possible_events)}."
        )
        if example_limit:
            flagged_examples = sorted(
                {(unit.runnumber, unit.segment) for unit in flagged}
            )[:example_limit]
            DEBUG(
                f"Examples: downstream work units below ratio cut {ratio_cut} "
                f"(run/segment): {_format_run_segment_examples(flagged_examples)}."
            )
            if len(flagged) > len(flagged_examples):
                DEBUG(
                    f"Additional downstream work unit examples suppressed after "
                    f"{len(flagged_examples)} examples."
                )
        breakdown_lines += 1
        estimated_breakdown_events += estimated_events

        eligible_downstream_categorized_events += estimated_events

        reason_counts = Counter(reason for unit in flagged for reason in unit.reasons)
        for reason, count in sorted(reason_counts.items()):
            units_by_run = Counter(unit.runnumber for unit in flagged if reason in unit.reasons)
            estimated_events = _estimate_events_from_unit_counts(
                units_by_run,
                goodruns,
                events_per_job,
            )
            INFO(
                f"Summary: {REASON_SUMMARY_TEXT.get(reason, reason)}: {count} work units "
                f"from {len(units_by_run)} runs, "
                f"estimated {_format_event_fraction(estimated_events, total_possible_events)}."
            )
            breakdown_lines += 1

    daqhost_failed_events = _estimate_events_from_unit_counts(
        Counter(runnumber for runnumber, _ in daqhost_failed_units),
        goodruns,
        events_per_job,
    )
    daqhost_passed_events = sum(
        events for runnumber, events in goodruns.items()
        if runnumber not in daqhost_failed_runs
    )
    unmatched_output_events = max(produced_events - eligible_output_events, 0)

    if breakdown_lines:
        INFO(
            f"Summary: total estimated categorized loss across breakdown categories: "
            f"{_format_event_fraction(estimated_breakdown_events, total_possible_events)}."
        )
    else:
        INFO("Summary: no estimated missing-event categories found.")

    eligible_downstream_uncategorized = max(
        output_event_deficit - eligible_downstream_categorized_events,
        0,
    )
    eligible_downstream_uncategorized_pct = (
        100.0 * eligible_downstream_uncategorized / eligible_input_events
        if eligible_input_events else 0.0
    )
    INFO(
        f"Summary: eligible downstream uncategorized after flagged work units: "
        f"{human_event_count(eligible_downstream_uncategorized)} events "
        f"({eligible_downstream_uncategorized_pct:.2f}% of eligible input events)."
    )

    INFO("Summary: Additional missing-event diagnostics:")
    INFO(
        f"Summary: eligible input coverage vs DAQ expectation: "
        f"{human_event_count(eligible_input_events)} / {human_event_count(eligible_expected_events)} "
        f"expected events in eligible combinations, deficit "
        f"{_format_event_fraction(input_event_deficit, total_possible_events)} "
        f"across {short_input_units} work units."
    )
    INFO(
        f"Summary: downstream output coverage vs eligible inputs: "
        f"{human_event_count(eligible_output_events)} / {human_event_count(eligible_input_events)} "
        f"eligible input events, deficit "
        f"{_format_event_fraction(output_event_deficit, total_possible_events)} "
        f"across {output_deficit_units} work units."
    )
    INFO("Summary: Event-universe sanity check:")
    INFO(
        f"Summary: DAQ possible events: "
        f"{_format_event_fraction(total_possible_events, total_possible_events)}."
    )
    INFO(
        f"Summary: events in observed daqhost-failed combinations: "
        f"{_format_event_fraction(daqhost_failed_events, total_possible_events)}."
    )
    INFO(
        f"Summary: DAQ events in runs passing daqhost gate: "
        f"{_format_event_fraction(daqhost_passed_events, total_possible_events)}."
    )
    INFO(
        f"Summary: expected events in eligible input combinations: "
        f"{_format_event_fraction(eligible_expected_events, total_possible_events)}."
    )
    INFO(
        f"Summary: input events in eligible combinations: "
        f"{_format_event_fraction(eligible_input_events, total_possible_events)}."
    )
    INFO(
        f"Summary: output events matched to eligible combinations: "
        f"{_format_event_fraction(eligible_output_events, total_possible_events)}."
    )
    INFO(
        f"Summary: all primary output FileCatalog events: "
        f"{_format_event_fraction(produced_events, total_possible_events)}."
    )
    INFO(
        f"Summary: primary output events not matched to eligible combinations: "
        f"{_format_event_fraction(unmatched_output_events, total_possible_events)}."
    )

    _log_flagged_input_availability(flagged, eligible_units)


def _flagged_input_availability(
    flagged: List[FlaggedWorkUnit],
    eligible_units: Dict[Tuple[int, int], List[DatasetInfo]],
) -> Tuple[Set[int], Set[int], int, int, int]:
    reproducible_units_by_run: Dict[int, int] = defaultdict(int)
    staging_units_by_run: Dict[int, int] = defaultdict(int)

    for unit in flagged:
        inputs = eligible_units.get((unit.runnumber, unit.segment), [])
        inputs_on_disk = bool(inputs) and all(
            bool(info.filename)
            for info in inputs
        )
        if inputs_on_disk:
            reproducible_units_by_run[unit.runnumber] += 1
        else:
            staging_units_by_run[unit.runnumber] += 1

    staging_runs = set(staging_units_by_run)
    reproducible_runs = set(reproducible_units_by_run) - staging_runs
    reproducible_units = sum(
        reproducible_units_by_run[runnumber]
        for runnumber in reproducible_runs
    )
    mixed_reproducible_units = sum(
        reproducible_units_by_run[runnumber]
        for runnumber in staging_runs
    )
    staging_units = sum(staging_units_by_run.values())
    return reproducible_runs, staging_runs, reproducible_units, staging_units, mixed_reproducible_units


def _log_flagged_input_availability(
    flagged: List[FlaggedWorkUnit],
    eligible_units: Dict[Tuple[int, int], List[DatasetInfo]],
) -> None:
    if not flagged:
        return

    reproducible_units_by_run: Dict[int, int] = defaultdict(int)
    staging_units_by_run: Dict[int, int] = defaultdict(int)

    for unit in flagged:
        inputs = eligible_units.get((unit.runnumber, unit.segment), [])
        inputs_on_disk = bool(inputs) and all(
            bool(info.filename)
            for info in inputs
        )
        if inputs_on_disk:
            reproducible_units_by_run[unit.runnumber] += 1
        else:
            staging_units_by_run[unit.runnumber] += 1

    staging_runs = set(staging_units_by_run)
    reproducible_runs = set(reproducible_units_by_run) - staging_runs
    reproducible_units = sum(
        reproducible_units_by_run[runnumber]
        for runnumber in reproducible_runs
    )
    mixed_reproducible_units = sum(
        reproducible_units_by_run[runnumber]
        for runnumber in staging_runs
    )
    staging_units = sum(staging_units_by_run.values())

    INFO("Summary: Flagged incomplete runs by required-input availability:")
    INFO(
        f"Summary: {len(reproducible_runs)} runs ({reproducible_units} work units) "
        f"have required input LFNs in FileCatalog and could be reproduced."
    )
    INFO(
        f"Summary: {len(staging_runs)} runs ({staging_units} work units) "
        f"are missing at least one required input LFN in FileCatalog and would likely need staging."
    )
    if mixed_reproducible_units:
        INFO(
            f"Summary: {mixed_reproducible_units} additional flagged work units in staging-needed runs "
            f"do have required input LFNs in FileCatalog; staging-needed wins at run granularity."
        )


def write_report(flagged: List[FlaggedWorkUnit], output: str = None) -> None:
    lines = [unit.report_line() for unit in flagged]
    text = "\n".join(lines)
    if text:
        text += "\n"

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(text)
        INFO(f"Flagged downstream work units written to {output}")


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


def _query_raw_daqhosts(match: Any, runnumbers: Iterable[int]) -> Dict[int, Set[str]]:
    from sphenixdbutils import cnxn_string_map, dbQuery

    run_condition = match._run_condition(list(runnumbers))
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
    from sphenixdbutils import cnxn_string_map, dbQuery

    run_condition = match._run_condition(list(runnumbers))
    if not run_condition:
        return []

    query = f"""
        SELECT d.dsttype, d.runnumber, d.segment, d.events, COALESCE(f.lfn, '') AS filename
        FROM {match.input_config.table} d
        LEFT JOIN files f ON f.lfn = d.filename
        WHERE dataset='{match.dataset}'
          AND tag='{match.input_config.intriplet}'
          AND dsttype IN {_sql_in(match.in_types)}
          AND {run_condition}
          {match.input_config.infile_query_constraints}
        ORDER BY d.runnumber, d.segment, d.dsttype
    """
    return dbQuery(cnxn_string_map[match.input_config.db], query).fetchall()


def _query_outputs(match: Any, runnumbers: Iterable[int]) -> List[Any]:
    from sphenixdbutils import cnxn_string_map, dbQuery

    run_condition = match._run_condition(list(runnumbers))
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

    from simpleLogger import slogger, set_log_timestamps_enabled
    import logging
    set_log_timestamps_enabled(False)
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
    raw_available_by_run: Dict[int, int] = {}
    catalog_available_by_run: Dict[int, int] = {}
    if required_hosts:
        raw_daqhosts_by_run = _query_raw_daqhosts(match, runnumbers)
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
        raw_available_by_run = daqhost_filter.raw_available_by_run
        catalog_available_by_run = daqhost_filter.catalog_available_by_run
        input_rows = [
            row for row in input_rows
            if int(_row_value(row, "runnumber", 1)) in allowed_runs
        ]
        INFO(f"{len(allowed_runs)} runs pass required daqhost availability checks.")
        INFO(f"{len(daqhost_failed_runs)} runs fail required daqhost availability checks.")
        INFO(f"{len(daqhost_failed_units)} run-segment combinations fail required daqhost availability checks.")

    output_rows = _query_outputs(match, runnumbers)
    INFO(f"{len(output_rows)} primary output FileCatalog rows found.")

    if len(match.in_types) == 1:
        log_single_input_file_summary(
            goodruns=goodruns,
            input_rows=input_rows,
            output_rows=output_rows,
            input_type=match.in_types[0],
            match=match,
            events_per_job=neventsper or 0,
            cut_segment=match.input_config.cut_segment,
            example_limit=args.example_limit,
        )
        return

    eligible_units = build_eligible_units(
        input_rows=input_rows,
        required_input_types=match.in_types,
        cut_segment=match.input_config.cut_segment,
    )
    INFO(f"{len(eligible_units)} available input combinations found.")

    # Run-level coverage check: compare summed input events to summed outputs
    _failing_runs = check_run_level_coverage(
        eligible_units=eligible_units,
        output_rows=output_rows,
        ratio_cut=args.ratio_cut,
        example_limit=args.example_limit,
    )

    # Also compare files DB outputs against eventsinrun from the DAQ DB.
    daq_events_by_run = {run: events for run, events in goodruns.items() if events}
    failing_daq: Set[int] = set()
    if daq_events_by_run:
        failing_daq = check_coverage_against_raw(
            output_rows=output_rows,
            raw_input_by_run=daq_events_by_run,
            ratio_cut=args.ratio_cut,
            example_limit=args.example_limit,
        )
        INFO(f"{len(failing_daq)} runs fail coverage against DAQ eventsinrun.")

    flagged = find_flagged_units(
        eligible_units=eligible_units,
        output_rows=output_rows,
        ratio_cut=args.ratio_cut,
        example_limit=args.example_limit,
    )

    if flagged:
        write_report(flagged, args.output)

    cleanup_flagged_work_units(flagged, args, match)

    log_event_summary(
        goodruns=goodruns,
        output_rows=output_rows,
        daqhost_failed_runs=daqhost_failed_runs,
        daqhost_failed_units=daqhost_failed_units,
        eligible_units=eligible_units,
        flagged=flagged,
        ratio_cut=args.ratio_cut,
        events_per_job=neventsper or 0,
        cut_segment=match.input_config.cut_segment,
        failing_daq_runs=failing_daq,
        example_limit=args.example_limit,
    )

    print_report(args.report, flagged, daqhost_failed_runs, daqhost_failed_units, raw_available_by_run, catalog_available_by_run, eligible_units)

    warn_input_mismatch_outputs(flagged, args)


def _input_mismatch_output_summary(flagged: List[FlaggedWorkUnit]) -> Tuple[int, int]:
    mismatch_output_units = [
        unit for unit in flagged
        if "input_mismatch" in unit.reasons
    ]
    mismatch_output_events = sum(
        unit.output_events for unit in mismatch_output_units
        if unit.output_events > 0
    )
    return len(mismatch_output_units), mismatch_output_events


def warn_input_mismatch_outputs(flagged: List[FlaggedWorkUnit], args: Any) -> None:
    mismatch_units, mismatch_events = _input_mismatch_output_summary(flagged)
    if not mismatch_units:
        return
    action = "will be included in --delete because --mismatch-delete is set" if args.mismatch_delete else "are protected from --delete unless --mismatch-delete is set"
    WARN(
        f"{mismatch_units} input_mismatch work units "
        f"({human_event_count(mismatch_events)} existing output events) have mismatched inputs/output; "
        f"they {action}."
    )


def _unit_tuple_condition(units: List[Tuple[int, int]]) -> str:
    values = ",".join(f"({runnumber},{segment})" for runnumber, segment in units)
    return f"(runnumber, segment) IN ({values})"


def _count_query_result(query: str, dbkey: str) -> int:
    from sphenixdbutils import cnxn_string_map, dbQuery

    rows = dbQuery(cnxn_string_map[dbkey], query).fetchall()
    return int(_row_value(rows[0], "count", 0)) if rows else 0


def cleanup_flagged_work_units(
    flagged: List[FlaggedWorkUnit],
    args: Any,
    match: Any,
    batch_size: int = 500,
) -> None:
    """Delete flagged downstream work units from production_jobs only."""
    if not flagged or not getattr(args, "delete", False):
        return

    from sphenixdbutils import cnxn_string_map, dbQuery

    if args.mismatch_delete:
        purge_flagged = flagged
    else:
        purge_flagged = [unit for unit in flagged if "input_mismatch" not in unit.reasons]
        protected_count, protected_events = _input_mismatch_output_summary(flagged)
        if protected_count:
            WARN(
                f"Skipping {protected_count} input_mismatch work units "
                f"({human_event_count(protected_events)} existing output events) during --delete; "
                f"use --mismatch-delete to include them."
            )
    if not purge_flagged:
        INFO("No flagged downstream work units selected for production_jobs cleanup.")
        return

    dryrun = args.dryrun or not args.andgo
    if dryrun:
        WARN("--delete given without --andgo or --dryrun set: dry run only, no deletions performed.")

    units = sorted({(unit.runnumber, unit.segment) for unit in purge_flagged})
    reasons_by_unit = {
        (unit.runnumber, unit.segment): ",".join(unit.reasons)
        for unit in purge_flagged
    }
    INFO(
        f"Preparing to purge {len(units)} flagged downstream work units "
        f"from production_jobs for tag={match.outtriplet}, dsttype={match.dsttype}."
    )

    quote = chr(39)
    for start in range(0, len(units), batch_size):
        batch = units[start:start + batch_size]
        unit_condition = _unit_tuple_condition(batch)
        catalog_unit_condition = unit_condition.replace("runnumber", "d.runnumber").replace("segment", "d.segment")

        datasets_count_query = f"""
            SELECT COUNT(*)
            FROM datasets
            WHERE dataset={quote}{match.dataset}{quote}
              AND tag={quote}{match.outtriplet}{quote}
              AND dsttype={quote}{match.dsttype}{quote}
              AND {unit_condition}
        """
        files_count_query = f"""
            SELECT COUNT(*)
            FROM files f
            JOIN datasets d ON f.lfn = d.filename
            WHERE d.dataset={quote}{match.dataset}{quote}
              AND d.tag={quote}{match.outtriplet}{quote}
              AND d.dsttype={quote}{match.dsttype}{quote}
              AND {catalog_unit_condition}
        """
        jobs_count_query = f"""
            SELECT COUNT(*)
            FROM production_jobs
            WHERE dataset={quote}{match.dataset}{quote}
              AND tag={quote}{match.outtriplet}{quote}
              AND dsttype={quote}{match.dsttype}{quote}
              AND {unit_condition}
        """

        datasets_count = _count_query_result(datasets_count_query, "fcr")
        files_count = _count_query_result(files_count_query, "fcr")
        jobs_count = _count_query_result(jobs_count_query, "statr")
        batch_index = start // batch_size + 1
        INFO(
            f"Delete batch {batch_index}: {len(batch)} work units, "
            f"production_jobs rows={jobs_count}, datasets rows={datasets_count}, files rows={files_count}."
        )

        if datasets_count or files_count:
            detail_query = f"""
                SELECT d.runnumber, d.segment, d.filename, d.events,
                       CASE WHEN f.lfn IS NULL THEN 0 ELSE 1 END AS has_files_row
                FROM datasets d
                LEFT JOIN files f ON f.lfn = d.filename
                WHERE d.dataset={quote}{match.dataset}{quote}
                  AND d.tag={quote}{match.outtriplet}{quote}
                  AND d.dsttype={quote}{match.dsttype}{quote}
                  AND {catalog_unit_condition}
                ORDER BY d.runnumber, d.segment, d.filename
                LIMIT {args.example_limit}
            """
            detail_rows = dbQuery(cnxn_string_map["fcr"], detail_query).fetchall()
            for row in detail_rows:
                runnumber = int(_row_value(row, "runnumber", 0))
                segment = int(_row_value(row, "segment", 1))
                filename = str(_row_value(row, "filename", 2))
                events = int(_row_value(row, "events", 3))
                has_files_row = bool(_row_value(row, "has_files_row", 4))
                reasons = reasons_by_unit.get((runnumber, segment), "unknown")
                WARN(
                    f"Existing FileCatalog row for purge candidate ({reasons}): "
                    f"run={runnumber} segment={segment} "
                    f"events={events} files_row={has_files_row} filename={filename}"
                )
            if datasets_count > len(detail_rows):
                WARN(
                    f"Additional FileCatalog detail rows suppressed after "
                    f"{len(detail_rows)} examples."
                )
            message = (
                "Matching FileCatalog rows exist for flagged downstream work units; "
                "not deleting production_jobs for this batch."
            )
            if dryrun:
                WARN(f"[dryrun] {message}")
                continue
            ERROR(message)
            sys.exit(3)

        delete_jobs = f"""
            DELETE FROM production_jobs
            WHERE dataset={quote}{match.dataset}{quote}
              AND tag={quote}{match.outtriplet}{quote}
              AND dsttype={quote}{match.dsttype}{quote}
              AND {unit_condition}
        """
        CHATTY(delete_jobs)
        curs = dbQuery(cnxn_string_map["statw"], delete_jobs, dryrun=dryrun)
        if curs:
            curs.commit()


def print_report(
    report: str,
    flagged: List[FlaggedWorkUnit],
    daqhost_failed_runs: Set[int],
    daqhost_failed_units: List[Tuple[int, int]],
    raw_available_by_run: Dict[int, int],
    catalog_available_by_run: Dict[int, int],
    eligible_units: Dict[Tuple[int, int], List[DatasetInfo]],
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
        for unit in sorted(flagged, key=lambda unit: (unit.runnumber, unit.segment)):
            if "missing_output" in unit.reasons:
                print(f"{unit.runnumber} {unit.segment}")
        return

    if report == "daqhost":
        for runnumber, segment in daqhost_failed_units:
            raw_available = raw_available_by_run.get(runnumber, 0)
            catalog_available = catalog_available_by_run.get(runnumber, 0)
            print(f"{runnumber} {segment} raw={raw_available} catalog={catalog_available}")
        return

    if report == "daqhost_runs":
        for runnumber in sorted(daqhost_failed_runs):
            raw_available = raw_available_by_run.get(runnumber, 0)
            catalog_available = catalog_available_by_run.get(runnumber, 0)
            print(f"{runnumber} raw={raw_available} catalog={catalog_available}")
        return

    if report in ("reproduce_runs", "stage_runs"):
        reproduce_runs, stage_runs, _, _, _ = _flagged_input_availability(flagged, eligible_units)
        selected_runs = reproduce_runs if report == "reproduce_runs" else stage_runs
        for runnumber in sorted(selected_runs):
            print(runnumber)
        return

    WARN(f"--report {report} is not supported by check_downstream.py; no report printed.")


if __name__ == "__main__":
    main()
