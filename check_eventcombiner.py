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
import math

from argparsing import submission_args
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixmatching import MatchConfig
from sphenixdbutils import cnxn_string_map, dbQuery, list_to_condition

# ============================================================================================
def main():
    args = submission_args()

    from simpleLogger import slogger
    import logging
    slogger.setLevel(logging.getLevelName(args.loglevel))

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

    # Get run list + ideal (run, daqhost) combos from RAW DB
    daqhosts_dict, eventsinrun_by_run = match.daqhosts_for_combining()  # also prints run count
    if not eventsinrun_by_run:
        sys.exit(0)

    # Total expected downstream output files from events/neventsper (sum over runs)
    neventsper = getattr(rule.job_config, 'neventsper', None)
    try:
        neventsper = int(neventsper) if neventsper is not None else 0
    except Exception:
        neventsper = 0
    if neventsper:
        total_expected_outputs = sum(
            math.ceil(eventsinrun / neventsper) for eventsinrun in eventsinrun_by_run.values() if eventsinrun
        )
        INFO(f"{total_expected_outputs} expected downstream output files from events/neventsper={neventsper}.")

    seb_types = [h for h in match.in_types if h != 'gl1daq']

    n_ideal = sum(sum(1 for h in hosts if h != 'gl1daq') for hosts in daqhosts_dict.values())
    INFO(f"{n_ideal} (run, daqhost) combinations have all segments on lustre.")

    run_condition = list_to_condition(list(eventsinrun_by_run))

    total_query = f"""
        SELECT DISTINCT runnumber, daqhost FROM datasets
        WHERE {run_condition}
          AND daqhost IN {tuple(seb_types)}
        ORDER BY runnumber, daqhost
    """
    all_combos = dbQuery(cnxn_string_map['rawr'], total_query).fetchall()
    INFO(f"{len(all_combos)} (run, daqhost) combinations found in the raw DB.")
    not_on_lustre = [(int(r), h) for r, h in all_combos if h not in daqhosts_dict.get(int(r), set())]
    INFO(f"{len(not_on_lustre)} (run, daqhost) combinations are in the DB but not fully on lustre.")
    for run, daqhost in not_on_lustre[:5]:
        DEBUG(f"  Not fully on lustre: Run {run} {daqhost}")

    runs_without_gl1daq = [run for run, hosts in daqhosts_dict.items() if 'gl1daq' not in hosts]
    for run in sorted(runs_without_gl1daq):
        WARN(f"Run {run}: gl1daq not complete on lustre — run will not be submitted.")

    lustre_combos = [
        (run, daqhost)
        for run, hosts in daqhosts_dict.items()
        if 'gl1daq' in hosts
        for daqhost in hosts
        if daqhost != 'gl1daq'
    ]

    # --- FileCatalog ratio check ---
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

    fc_dsttypes_by_run = {}
    for r, d, _ in rows:
        fc_dsttypes_by_run.setdefault(int(r), []).append(d)

    lustre_no_fc = [
        (run, daqhost)
        for run, daqhost in lustre_combos
        if not any(daqhost in dsttype for dsttype in fc_dsttypes_by_run.get(run, []))
    ]
    INFO(f"{len(lustre_no_fc)} lustre combos have no FileCatalog entry.")
    if lustre_no_fc:
        for run, daqhost in sorted(lustre_no_fc)[:5]:
            DEBUG(f"  Run {run} {daqhost}")

    all_no_fc = [
        (int(r), h)
        for r, h in all_combos
        if not any(h in dsttype for dsttype in fc_dsttypes_by_run.get(int(r), []))
    ]
    if all_no_fc:
        WARN(f"{len(all_no_fc)} raw DB combos (lustre or not) have no FileCatalog entry. Check for corruption?")
        for run, daqhost in sorted(all_no_fc)[:5]:
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

    if flagged:
        report_and_cleanup(flagged, args, match)

    # Unique-run summaries
    unique_runs_no_fc = sorted({int(r) for r, h in all_no_fc})
    INFO(f"{len(unique_runs_no_fc)} unique runs have raw DB combos (lustre or not) with no FileCatalog entry.")
    # show up to 5 example (run,daqhost) combos for followup
    all_no_fc_by_run = {}
    for r, h in all_no_fc:
        rint = int(r)
        all_no_fc_by_run.setdefault(rint, []).append(h)
    examples_shown = 0
    for run in unique_runs_no_fc:
        if examples_shown >= 5:
            break
        hosts = all_no_fc_by_run.get(run, [])
        if hosts:
            DEBUG(f"  Example missing combo: Run {run} daqhost={hosts[0]}")
            examples_shown += 1

    unique_flagged_runs = sorted({r for r, d in flagged})
    INFO(f"{len(unique_flagged_runs)} unique runs have flagged (run,dsttype) combinations below ratio cut.")
    # show up to 5 example (run,dsttype) combos for followup
    unique_flagged_combos = sorted({(int(r), d) for r, d in flagged})
    for run, dst in unique_flagged_combos[:5]:
        DEBUG(f"  Example flagged combo: Run {run} dsttype={dst}")

    above_threshold = len(rows) - len(flagged)
    n_possible = len(all_combos)
    pct = 100.0 * above_threshold / n_possible if n_possible else 0.0
    INFO(f"Summary: {above_threshold}/{n_possible} possible combos have FileCatalog entries above threshold ({pct:.1f}%).")

# ============================================================================================
def report_and_cleanup(flagged, args, match):
    """
    Handle incomplete (runnumber, dsttype) combinations.
    Requires both --delete and --andgo to actually execute deletions.
    """
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text('\n'.join(f"{r} {d}" for r, d in flagged) + '\n')
        INFO(f"Flagged combinations written to {args.output}")

    if not getattr(args, 'delete', False):
        return

    dryrun = args.dryrun or not args.andgo
    if dryrun:
        WARN("--delete given without --andgo or --dryrun set: dry run only, no deletions performed.")

    tag = match.outtriplet

    for runnumber, dsttype in flagged:
        INFO(f"Deleting run {runnumber} {dsttype} from FileCatalog and production DB.")

        # 1. files (must come before datasets — uses JOIN to identify rows)
        delete_files = f"""
            DELETE FROM files USING datasets
            WHERE lfn=filename
              AND tag='{tag}'
              AND dsttype='{dsttype}'
              AND runnumber={runnumber}
        """
        # 2. datasets
        delete_datasets = f"""
            DELETE FROM datasets
            WHERE tag='{tag}'
              AND dsttype='{dsttype}'
              AND runnumber={runnumber}
        """
        # 3. production_jobs
        delete_prodjobs = f"""
            DELETE FROM production_jobs
            WHERE tag='{tag}'
              AND dsttype='{dsttype}'
              AND runnumber={runnumber}
        """

        CHATTY(delete_files)
        curs = dbQuery(cnxn_string_map['fcw'], delete_files, dryrun=dryrun)
        if curs: curs.commit()

        CHATTY(delete_datasets)
        curs = dbQuery(cnxn_string_map['fcw'], delete_datasets, dryrun=dryrun)
        if curs: curs.commit()

        CHATTY(delete_prodjobs)
        curs = dbQuery(cnxn_string_map['statw'], delete_prodjobs, dryrun=dryrun)
        if curs: curs.commit()

# ============================================================================================
if __name__ == '__main__':
    main()
