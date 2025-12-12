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
from sphenixmisc import setup_rot_handler, should_I_quit
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixdbutils import dbQuery, cnxn_string_map, list_to_condition

def process_chunk(chunk, production_status_table, dryrun=False):
    """
    Processes a single chunk of results from the file catalog.
    Checks for existing files in production_status and generates
    aggregated INSERT and UPDATE statements.
    """
    DEBUG(f"Processing chunk of {len(chunk)} files...")
    
    lfns_in_chunk = [item[0] for item in chunk]
    lfn_list_for_sql = "','".join(lfns_in_chunk)
    check_query = f"SELECT dstfile FROM {production_status_table} WHERE dstfile IN ('{lfn_list_for_sql}')"
    
    existing_files_cursor = dbQuery(cnxn_string_map['statr'], check_query)
    if not existing_files_cursor:
        ERROR("Failed to query production_status for existing files.")
        return

    existing_lfns = {row.dstfile for row in existing_files_cursor.fetchall()}
    
    insert_values = []
    update_values = []
    for lfn, time, run, seg, dsttype in chunk:
        if lfn in existing_lfns:
            update_values.append(f"('{lfn}', '{time}'::timestamp)")
        else:
            dstname = lfn.split('-', 1)[0]
            insert_values.append(f"('{dsttype}', '{dstname}', '{lfn}', {run}, {seg}, 0, 'dbquery', 0, 0, 0, 'finished', '{time}')")

    all_statements = []
    if insert_values:
        values_str = ",\n".join(insert_values)
        insert_query = f"""
        INSERT INTO {production_status_table} (
            dsttype, dstname, dstfile, run, segment, nsegments,
            inputs, prod_id, cluster, process, status, ended
        ) VALUES
        {values_str};
        """
        all_statements.append(insert_query)

    if update_values:
        values_str = ",\n".join(update_values)
        update_query = f"""
        UPDATE {production_status_table} AS ps SET
            ended = v.ended,
            status = 'finished'
        FROM (VALUES {values_str}) AS v(dstfile, ended)
        WHERE ps.dstfile = v.dstfile;
        """
        all_statements.append(update_query)

    if all_statements:
        update_query = "\n".join(all_statements)
        CHATTY(update_query)

        if not dryrun:
            update_cursor = dbQuery(cnxn_string_map['statw'], update_query)
            if update_cursor:
                update_cursor.commit()
                INFO(f"Processed {len(chunk)} entries in production_status.")
            else:
                ERROR("Failed to update/insert into production_status.")
        else:
            INFO("Dry run, not updating database.")
            CHATTY(update_query)

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

    recency_interval = '1 DAYS' # args.recent if args.recent else '7 DAYS'
    base_query = f"""
    SELECT f.lfn, f.time, d.runnumber, d.segment, d.dsttype
    FROM {files_table} f
    JOIN {datasets_table} d ON f.lfn = d.filename
    WHERE d.dsttype like '{rule.dsttype}%'
    AND d.tag = '{rule.outtriplet}'
    AND {run_condition}
    AND f.time > (NOW() - INTERVAL '{recency_interval}')
    """
#    AND d.dataset = '{rule.dataset}'

    INFO("Querying file catalog in chunks...")
    
    last_lfn = ""
    chunk_size = 100000
    
    while True:
        
        paginated_query = base_query
        if last_lfn:
            paginated_query += f" AND f.lfn > '{last_lfn}'"

        query = f"""
        {paginated_query}
        ORDER BY f.lfn
        LIMIT {chunk_size}
        """

        DEBUG(f"Using query:\n{query}")
        files_cursor = dbQuery(cnxn_string_map['fcr'], query)
        if not files_cursor:
            ERROR("Failed to query file catalog.")
            break

        results = files_cursor.fetchall()
        if not results:
            INFO("No more files to update.")
            break
        
        INFO(f"Found {len(results)} files in this chunk.")
        process_chunk(results, production_status_table, args.dryrun)

        last_lfn = results[-1][0]
        

    if args.profile:
        profiler.disable()
        DEBUG("Profiling finished. Printing stats...")
        stats = pstats.Stats(profiler)
        stats.strip_dirs().sort_stats('time').print_stats(10)

    INFO(f"{Path(sys.argv[0]).name} DONE.")

if __name__ == '__main__':
    main()
    exit(0)
