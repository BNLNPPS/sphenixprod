#!/usr/bin/env python
import argparse
from pathlib import Path
import pprint # noqa F401

from argparsing import monitor_args
from sphenixdbutils import test_mode as dbutils_test_mode
from simpleLogger import slogger, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixmatching import MatchConfig
from sphenixmisc import setup_rot_handler, should_I_quit
import htcondor2 as htcondor  # type: ignore
import classad2 as classad # type: ignore

def monitor_condor_jobs(batch_name: str, dryrun: bool=True):
    """
    Check on the status of held jobs and process them using the htcondor2 bindings.
    """
    INFO("Polling for all condor jobs using htcondor2 python bindings...")
    
    try:
        schedd = htcondor.Schedd()

        batch_pattern = f'.*\\.{batch_name}$'
        # Query all jobs for the batch, we will filter by status locally
        constraint = f'regexp("{batch_pattern}", JobBatchName)'
        INFO(f"Querying condor with constraint: {constraint}")

        attrs = [
            'ClusterId', 'ProcId', 'JobStatus', 'Owner', 'JobBatchName', 'QDate', 'CompletionDate',
            'ExitCode', 'HoldReason', 'RemoveReason', 'Cmd', 'Args', 'Iwd', 'RemoteHost', 'NumJobStarts',
            'ResidentSetSize', 'MemoryProvisioned', 'LastHoldReasonCode'
        ]

        jobs = schedd.query(constraint=constraint, projection=attrs)

        if not jobs:
            INFO("No jobs found for the specified batch name.")
            return

        INFO(f"Found {len(jobs)} jobs for batch_name {batch_name}.")
    except Exception as e:
        ERROR(f"An unexpected error occurred during condor query: {e}")
        exit(1)

    ad_by_dbid = {}
    for ad in jobs:
        dbid=ad.get('Args', '').split()[-1]
        if not dbid.isdigit():
            ERROR(f"Job with Args {ad.get('Args', '')} has non-integer dbid {dbid}. Stop.")
            exit(1)
        dbid = int(dbid)
        if dbid in ad_by_dbid:
            ERROR(f"Duplicate dbid {dbid} found in jobs, overwriting previous entry. Stop.")
            exit(1)
        ad_by_dbid[dbid] = ad
    INFO(f"Mapped {len(ad_by_dbid)} jobs by dbid.")
    return ad_by_dbid


def base_batchname_from_args(args: argparse.Namespace) -> str:
    if args.base_batchname is not None:
        return args.base_batchname

    # Prepare param_overrides for RuleConfig
    param_overrides = {}
    param_overrides["runs"] = args.runs
    param_overrides["runlist"] = args.runlist
    param_overrides["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor

    if args.physicsmode is not None:
        param_overrides["physicsmode"] = args.physicsmode

    # filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    param_overrides["prodmode"] = "production"
    if args.mangle_dirpath:
        param_overrides["prodmode"] = args.mangle_dirpath

    # Load specific rule from the given yaml file.
    try:
        rule = RuleConfig.from_yaml_file(
            yaml_file=args.config,
            rule_name=args.rulename,
            param_overrides=param_overrides
        )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error loading rule configuration: {e}")
        exit(1)

    # Create a match configuration from the rule
    INFO("Match configuration created.")

    # Call the main monitoring function
    batch_name=rule.job_config.batch_name # usually starts with "main." or so. Remove that
    batch_name=batch_name.split(".", 1)[-1]
    return batch_name

def main():
    args = monitor_args()
    #################### Test mode?
    test_mode = (
            dbutils_test_mode
            or args.test_mode
            # or ( hasattr(rule, 'test_mode') and rule.test_mode ) ## allow in the yaml file?
        )

    # Set up submission logging before going any further
    sublogdir=setup_rot_handler(args)
    slogger.setLevel(args.loglevel)
    INFO(f"Logging to {sublogdir}, level {args.loglevel}")

    if test_mode:
        INFO("Running in testbed mode.")
        args.mangle_dirpath = 'production-testbed'
    else:
        INFO("Running in production mode.")

    monitor_condor_jobs(batch_name=base_batchname_from_args(args), dryrun=args.dryrun)
    INFO(f"{Path(__file__).name} DONE.")

if __name__ == '__main__':
    main()
    exit(0)
