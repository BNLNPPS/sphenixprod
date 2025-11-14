#!/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import pstats
import sys
import shutil
import os

import pprint # noqa F401

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit, make_chunks
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixdbutils import dbQuery, cnxn_string_map, list_to_condition

def main():
    ### digest arguments
    args = submission_args()

    #################### Test mode?
    test_mode = args.test_mode

    # Set up submission logging before going any further
    sublogdir=setup_rot_handler(args)
    slogger.setLevel(args.loglevel)

    # Exit without fuss if we are already running
    if should_I_quit(args=args, myname=sys.argv[0]):
        DEBUG("Stop.")
        exit(0)
    INFO(f"Logging to {sublogdir}, level {args.loglevel}")

    if args.profile:
        DEBUG( "Profiling is ENABLED.")
        profiler = cProfile.Profile()
        profiler.enable()

    INFO(f"Starting {sys.argv[0]}.")
    INFO(sys.argv)

    if test_mode:
        INFO("Running in testbed mode.")
        args.mangle_dirpath = 'production-testbed'
    else:
        INFO("Running in production mode.")

    param_overrides = {}
    param_overrides["runs"]=args.runs
    param_overrides["runlist"]=args.runlist
    param_overrides["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor

    if args.physicsmode is not None:
        param_overrides["physicsmode"] = args.physicsmode # e.g. physics

    param_overrides["prodmode"] = "production"
    if args.mangle_dirpath:
        param_overrides["prodmode"] = args.mangle_dirpath

    CHATTY(f"Rule substitutions: {param_overrides}")
    INFO("Now loading and building rule configuration.")

    try:
        rule = RuleConfig.from_yaml_file( yaml_file=args.config, rule_name=args.rulename, param_overrides=param_overrides )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)

    CHATTY("Rule configuration:")
    CHATTY(yaml.dump(rule.dict))

    files_table = 'test_files' if test_mode else 'files'
    datasets_table = 'test_datasets' if test_mode else 'datasets'
    production_status_table = 'production_status'

    run_condition = list_to_condition(rule.runlist_int, name="d.runnumber")

    query = f"""
    SELECT f.lfn, f.time, d.runnumber, d.segment, d.dsttype
    FROM {files_table} f
    JOIN {datasets_table} d ON f.lfn = d.filename
    WHERE d.dsttype like '{rule.dsttype}%'
    AND d.tag = '{rule.outtriplet}'
    AND {run_condition};
    """
#    AND d.dataset = '{rule.dataset}'

    INFO("Querying file catalog...")
    DEBUG(f"Using query:\n{query}")
    files_cursor = dbQuery(cnxn_string_map['fcr'], query)
    if not files_cursor:
        ERROR("Failed to query file catalog.")
        exit(1)

    results = files_cursor.fetchall()
    INFO(f"Found {len(results)} files to update.")

    if not results:
        INFO("No files to update.")
        exit(0)

    chunk_size = 1000
    chunks = make_chunks(results, chunk_size)
    num_chunks = (len(results) + chunk_size - 1) // chunk_size

    for i, chunk in enumerate(chunks):
        INFO(f"Processing chunk {i+1}/{num_chunks}")
        
        lfns_in_chunk = [item[0] for item in chunk]
        lfn_list_for_sql = "','".join(lfns_in_chunk)
        check_query = f"SELECT dstfile FROM {production_status_table} WHERE dstfile IN ('{lfn_list_for_sql}')"
        
        existing_files_cursor = dbQuery(cnxn_string_map['statr'], check_query)
        if not existing_files_cursor:
            ERROR("Failed to query production_status for existing files.")
            continue

        existing_lfns = {row.dstfile for row in existing_files_cursor.fetchall()}
        
        all_statements = []
        for lfn, time, run, seg, dsttype in chunk:
            if lfn in existing_lfns:
                all_statements.append(f"UPDATE {production_status_table} SET ended = '{time}', status = 'finished' WHERE dstfile = '{lfn}';")
            else:
                dstname = lfn.split('-', 1)[0]
                all_statements.append(f"""
                INSERT INTO {production_status_table} (
                    dsttype, dstname, dstfile, run, segment, nsegments,
                    inputs, prod_id, cluster, process, status, ended
                ) VALUES (
                    '{dsttype}', '{dstname}', '{lfn}', {run}, {seg}, 0,
                    'dbquery', 0, 0, 0, 'finished', '{time}'
                );
                """)

        if all_statements:
            update_query = "\n".join(all_statements)
            
            if not args.dryrun:
                update_cursor = dbQuery(cnxn_string_map['statw'], update_query)
                if update_cursor:
                    update_cursor.commit()
                    INFO(f"Processed {len(chunk)} entries in production_status.")
                else:
                    ERROR("Failed to update/insert into production_status.")
            else:
                INFO("Dry run, not updating database.")
                CHATTY(update_query)
        

    if args.profile:
        profiler.disable()
        DEBUG("Profiling finished. Printing stats...")
        stats = pstats.Stats(profiler)
        stats.strip_dirs().sort_stats('time').print_stats(10)

    INFO(f"{Path(sys.argv[0]).name} DONE.")

if __name__ == '__main__':
    main()
    exit(0)
