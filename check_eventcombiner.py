#!/usr/bin/env python3
"""
check_eventcombiner.py

For each run that already has output in the FileCatalog, compare
max(lastevent) from those output files against eventsinrun from the
DAQ run-quality DB. Runs whose ratio falls below --ratio-cut are
flagged and written to an output list.
"""

import sys
from pathlib import Path

from argparsing import submission_args
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixmatching import MatchConfig
from sphenixdbutils import cnxn_string_map, dbQuery, list_to_condition

# ============================================================================================
def main():
    args = submission_args()

    from simpleLogger import slogger
    import logging
    slogger.setLevel(getattr(logging, args.loglevel))

    param_overrides = {}
    param_overrides["runs"]     = args.runs
    param_overrides["runlist"]  = args.runlist
    param_overrides["nevents"]  = args.nevents
    param_overrides["prodmode"] = "production"
    if args.mangle_dirpath:
        param_overrides["prodmode"] = args.mangle_dirpath
    if args.physicsmode:
        param_overrides["physicsmode"] = args.physicsmode

    rule = RuleConfig.from_yaml_file(
        yaml_file       = args.config,
        rule_name       = args.rulename,
        param_overrides = param_overrides,
    )

    match = MatchConfig.from_rule_config(rule)

    if 'raw' not in match.input_config.db:
        ERROR(f"Rule '{args.rulename}' is not a combining rule (db={match.input_config.db}). Exiting.")
        sys.exit(2)

    # Get run list + eventsinrun from DAQ DB (same path as submission)
    eventsinrun_by_run = match.good_runlist()   # Dict[int, int]
    if not eventsinrun_by_run:
        INFO("No runs pass run quality cuts.")
        sys.exit(0)
    INFO(f"{len(eventsinrun_by_run)} runs pass run quality cuts.")

    run_condition = list_to_condition(list(eventsinrun_by_run))

    # Query FileCatalog for max(lastevent) per (runnumber, dsttype)
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
    rows = dbQuery(cnxn_string_map['fcr'], lastevent_query).fetchall()

    INFO(f"{len(rows)} (run, dsttype) combinations have existing output in the FileCatalog.")

    flagged = []   # list of (runnumber, dsttype)
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
            INFO(msg)
        else:
            CHATTY(msg)

    flagged = sorted(set(flagged))
    INFO(f"{len(flagged)} (run, dsttype) combinations flagged below ratio cut {args.ratio_cut}.")

    if flagged:
        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text('\n'.join(f"{r} {d}" for r, d in flagged) + '\n')
            INFO(f"Flagged combinations written to {args.output}")
        else:
            print('\n'.join(f"{r} {d}" for r, d in flagged))

# ============================================================================================
if __name__ == '__main__':
    main()
