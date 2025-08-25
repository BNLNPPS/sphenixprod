#!/usr/bin/env python

from pathlib import Path
import sys

import pprint # noqa F401

from argparsing import submission_args
from sphenixdbutils import test_mode as dbutils_test_mode
from simpleLogger import slogger, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixmatching import MatchConfig
from sphenixmisc import setup_rot_handler, should_I_quit

def monitor_condor_jobs(match_config: MatchConfig, dryrun: bool=True):
    """
    Check on the status of running, held, finished jobs using condor_q.
    """
    INFO("Payload for monitor_condor_jobs is a placeholder.")
    # TODO: Implement the actual condor_q logic here.
    # For example:
    # cmd = "condor_q -all"
    # status, output = shell_command(cmd)
    # INFO(f"condor_q output:\n{output}")
    pass

def main():
    args = submission_args()

    #################### Test mode?
    test_mode = (
            dbutils_test_mode
            or args.test_mode
            # or ( hasattr(rule, 'test_mode') and rule.test_mode ) ## allow in the yaml file?
        )

    # Set up submission logging before going any further
    sublogdir=setup_rot_handler(args)
    slogger.setLevel(args.loglevel)

    # Exit without fuss if we are already running
    if should_I_quit(args=args, myname=sys.argv[0]):
        DEBUG("Stop.")
        exit(0)
    INFO(f"Logging to {sublogdir}, level {args.loglevel}")

    if test_mode:
        INFO("Running in testbed mode.")
        args.mangle_dirpath = 'production-testbed'
    else:
        INFO("Running in production mode.")

    # Prepare param_overrides for RuleConfig
    param_overrides = {}
    param_overrides["runs"] = args.runs
    param_overrides["runlist"] = args.runlist

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
    match_config = MatchConfig.from_rule_config(rule)
    INFO("Match configuration created.")

    # Call the main monitoring function
    monitor_condor_jobs(match_config, dryrun=args.dryrun)

    INFO(f"{Path(__file__).name} DONE.")

if __name__ == '__main__':
    main()
    exit(0)
