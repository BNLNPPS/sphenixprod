#!/bin/env python

from pathlib import Path
from datetime import datetime
import cProfile
import pstats
import subprocess
import sys
import re

import pprint # noqa F401

import argparse
from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixdbutils import cnxn_string_map, dbQuery


# ============================================================================================
def locate_submitfiles(rule: RuleConfig, args: argparse.Namespace, allruns: bool=False):
    ### Outsourced because this function is independently useful
    submitdir = Path(f'{args.submitdir}').resolve()
    subbase = f'{rule.dsttype}_{rule.dataset}_{rule.outtriplet}'
    INFO(f'Submission files located in {submitdir}')
    INFO(f'Submission files based on {subbase}')

    sub_files = list(Path(submitdir).glob(f'{subbase}*.sub'))
    sub_files = list(map(str,sub_files))
    DEBUG(f"[locate_submitfiles] Submission files before run constraint:\n{pprint.pformat(sub_files)}")
    runlist=list(map(str,rule.runlist_int))

    # Only use those who match the run condition - the pythonic way
    if allruns:
        INFO("Ignoring run constraints, using all submission files.")
    else:
        #INFO(f"Selecting submission files for runs: {runlist}")
        INFO("Selecting submission files based on runlist")
        sub_files = {file for file in sub_files if any( f'_{runnumber}' in file for runnumber in runlist) }

    sub_files = sorted(sub_files,reverse=True) # latest runs first
    DEBUG(f"[locate_submitfiles] Submission files AFTER run constraint:\n{pprint.pformat(sub_files)}")
    if sub_files == []:
        INFO("No submission files found.")
    return sub_files


# ============================================================================================
def execute_submission(rule: RuleConfig, args: argparse.Namespace, allruns: bool=False):
    """ Look for job files and submit condor jobs if the current load is acceptable.
    Update production database to "submitted".
    Locking and deleting is used to avoid double-submission.
    """

    sub_files=locate_submitfiles(rule, args, allruns)
    if sub_files == []:
        INFO("No submission files found.")

    submitted_jobs=0
    for sub_file in sub_files:
        in_file=re.sub(r".sub$",".in",str(sub_file))
        ### Catch problems or skipped runs
        if not Path(in_file).is_file():
            WARN(f"Deleting {sub_file} as it doesn't have a corresponding .in file")
            Path(sub_file).unlink()

        ### Update production database
        # Extract dbids and filenames
        dbids=[]
        filenames=[]
        try:
            with open(in_file,'r') as f:
                for line in f:
                    parts = line.strip().rsplit(" ", 1)
                    dbids.append(str(parts[-1]))
                    # Filename is derived from the log file (first element in the CSV-like arguments)
                    if len(parts) > 0:
                        log_file = parts[0].split(",", 1)[0]
                        filenames.append(Path(log_file).stem + ".root")
        except Exception as e:
            ERROR(f"Error while parsing {in_file}:\n{e}")
            exit(1)
        dbids_str=", ".join(dbids)
        now_str=str(datetime.now().replace(microsecond=0))
        # Update production_jobs (primary) before condor_submit to mark as submitted
        update_prod_jobs = f"""
UPDATE production_jobs
   SET status='submitted', submitted='{now_str}'
WHERE id in
( {dbids_str} )
;
"""
        DEBUG(f"Updating db for {sub_file}")
        CHATTY(f"{update_prod_jobs}")
        prod_jobs_curs = dbQuery( cnxn_string_map['statw'], update_prod_jobs )
        prod_jobs_curs.commit()

        DEBUG(f"Submitting {sub_file}")
        cluster = 0
        if not args.dryrun:
            try:
                # Capture output to get cluster ID
                res = subprocess.run(f"condor_submit {sub_file}", shell=True, check=True, capture_output=True, text=True)
                # Parse Cluster ID
                for line in res.stdout.splitlines():
                    if "submitted to cluster" in line:
                        match = re.search(r"cluster (\d+)", line)
                        if match:
                            cluster = int(match.group(1))
                DEBUG(f"Submitted {sub_file} to cluster {cluster}")

                # Cleanup files
                Path(sub_file).unlink()
                Path(in_file).unlink()
                submitted_jobs+=len(dbids)
            except subprocess.CalledProcessError as e:
                ERROR(f"Submission failed for {sub_file}: {e.stderr}")
                continue

        # After successful submission, add ClusterId to production_jobs
        if not args.dryrun and cluster > 0:
            try:
                dbQuery(cnxn_string_map['statw'],
                        f"UPDATE production_jobs SET ClusterId={cluster} WHERE id IN ({dbids_str})").commit()
            except Exception as e:
                ERROR(f"Failed to update ClusterId in production_jobs: {e}")

    INFO(f"Received a total of {len(sub_files)} submission files.")
    INFO(f"Submitted a total of {submitted_jobs} jobs.")
    # Remove submission directory if empty
    submitdir = Path(f'{args.submitdir}').resolve()
    if not args.dryrun and submitdir.is_dir() and not any(submitdir.iterdir()):
        submitdir.rmdir()


# ============================================================================================
def main():
    ### digest arguments
    args = submission_args()

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

    INFO("Running in production mode.")

    #################### Rule has steering parameters and two subclasses for input and job specifics
    # Rule is instantiated via the yaml reader.

    ### Parse command line arguments into a substitution dictionary
    # This dictionary is passed to the ctor to override/customize yaml file parameters
    # Note: The following could all be hidden away in the RuleConfig ctor
    # but this way, CLI arguments are used by the function that received them and
    # constraint constructions are visibly handled away from the RuleConfig class
    param_overrides = {}
    param_overrides["runs"]=args.runs
    param_overrides["runlist"]=args.runlist
    param_overrides["prodmode"] = None  # Not relevant, but needed for the RuleConfig ctor
    param_overrides["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor

    CHATTY(f"Rule substitutions: {param_overrides}")
    INFO("Now loading and building rule configuration.")

    #################### Load specific rule from the given yaml file.
    try:
        rule =  RuleConfig.from_yaml_file( yaml_file=args.config, rule_name=args.rulename, param_overrides=param_overrides )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)

    # CHATTY("Rule configuration:")
    # CHATTY(yaml.dump(rule.dict))

    filesystem = rule.job_config.filesystem
    CHATTY(f"Filesystem: {filesystem}")

    ### And go
    execute_submission(rule, args)

    if args.profile:
        profiler.disable()
        DEBUG("Profiling finished. Printing stats...")
        stats = pstats.Stats(profiler)
        stats.strip_dirs().sort_stats('time').print_stats(10)

    INFO(f"{Path(sys.argv[0]).name} DONE.")

# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)
