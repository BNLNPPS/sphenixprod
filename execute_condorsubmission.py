#!/bin/env python

from pathlib import Path
from datetime import datetime
import cProfile
import pstats
import subprocess
import sys
import re
from typing import List

import pprint # noqa F401

import argparse
from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit, shell_command
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixdbutils import test_mode as dbutils_test_mode
from sphenixdbutils import cnxn_string_map, dbQuery


# ============================================================================================
def locate_submitfiles(rule: RuleConfig, args: argparse.Namespace):
    ### Outsourced because this function is independently useful
    submitdir = Path(f'{args.submitdir}').resolve()
    subbase = f'{rule.dsttype}_{rule.dataset}_{rule.outtriplet}'
    INFO(f'Submission files located in {submitdir}')
    INFO(f'Submission files based on {subbase}')

    sub_files = list(Path(submitdir).glob(f'{subbase}*.sub'))
    sub_files = list(map(str,sub_files))
    DEBUG(f"Submission files before run constraint:\n{pprint.pformat(sub_files)}")
    runlist=list(map(str,rule.runlist_int))

    # Only use those who match the run condition - the pythonic way
    sub_files = {file for file in sub_files if any( f'_{runnumber}' in file for runnumber in runlist) }
    sub_files = sorted(sub_files,reverse=True) # latest runs first
    DEBUG(f"Submission files after run constraint:\n{pprint.pformat(sub_files)}")
    if sub_files == []:
        INFO("No submission files found.")
    return sub_files
        

# ============================================================================================

def execute_submission(rule: RuleConfig, args: argparse.Namespace):
    """ Look for job files and submit condor jobs if the current load is acceptable.
    Update production database to "submitted".
    Locking and deleting is used to avoid double-submission.
    """

    sub_files=locate_submitfiles(rule, args)
    if sub_files == []:
        INFO("No submission files found.")
        
    submitted_jobs=0
    # Determine what's already in "idle"
    # Note: For this, we cannot use runnumber cuts, too difficult (and expensive) to get from condor.
    # Bit of a clunky method. But it works and doesn't get called all that often.
    cq_query  =  'condor_q'
    cq_query += f" -constraint \'JobBatchName==\"{rule.job_config.batch_name}\"' "  # Select our batch
    cq_query +=  ' -format "%d." ClusterId -format "%d\\n" ProcId'                  # any kind of one-line-per-job output. e.g. 6398.10
    idle_procs = shell_command(cq_query + ' -idle' ) # Select what to count (idle, held must be asked separately)
    held_procs = shell_command(cq_query + ' -held' ) 
    if len(idle_procs) > 0:
        INFO(f"We already have {len(idle_procs)} jobs in the queue waiting for execution.")
    if len(held_procs) > 0:
        WARN(f"There are {len(held_procs)} held jobs what should be removed and resubmitted.")
    
    max_submitted=10000 
    for sub_file in sub_files:
        if submitted_jobs>max_submitted:
            break

        in_file=re.sub(r".sub$",".in",str(sub_file))
        ### Update production database
        # Extract dbids
        dbids=[]
        try: 
            with open(in_file,'r') as f:
                for line in f:
                    dbids.append(str(line.strip().split(" ")[-1]))
        except Exception as e:
            ERROR(f"Error while parsing {in_file}:\n{e}")
            exit(1)
        submitted_jobs+=len(dbids)
        dbids_str=", ".join(dbids)
        now_str=timestamp=str(datetime.now().replace(microsecond=0))
        update_prod_state = f"""
UPDATE production_status
   SET status='submitted',submitted='{now_str}'
WHERE id in
( {dbids_str} )
;
""" 
        INFO(f"Updating db for {sub_file}")
        CHATTY(f"{update_prod_state}")
        prod_curs = dbQuery( cnxn_string_map['statw'], update_prod_state )
        prod_curs.commit()

        INFO(f"Submitting {sub_file}\n\t\t && Removing {in_file}")
        if not args.dryrun:
            subprocess.run(f"condor_submit {sub_file} && rm {sub_file} {in_file}",shell=True)
            # subprocess.run(f"echo condor_submit {sub_file} && echo rm {sub_file} {in_file}",shell=True)
    
    
# ============================================================================================
def main():
    ### digest arguments
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

    if args.profile:
        DEBUG(f"Profiling is ENABLED.")
        profiler = cProfile.Profile()
        profiler.enable()    
    
    if test_mode:
        INFO("Running in testbed mode.")
        args.mangle_dirpath = 'production-testbed'
    else:
        INFO("Running in production mode.")

    #################### Rule has steering parameters and two subclasses for input and job specifics
    # Rule is instantiated via the yaml reader.

    ### Parse command line arguments into a substitution dictionary
    # This dictionary is passed to the ctor to override/customize yaml file parameters
    # Note: The following could all be hidden away in the RuleConfig ctor
    # but this way, CLI arguments are used by the function that received them and
    # constraint constructions are visibly handled away from the RuleConfig class
    rule_substitutions = {}
    rule_substitutions["runs"]=args.runs
    rule_substitutions["runlist"]=args.runlist
    rule_substitutions["prodmode"] = None  # Not relevant, but needed for the RuleConfig ctor
    rule_substitutions["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor
    
    CHATTY(f"Rule substitutions: {rule_substitutions}")
    INFO("Now loading and building rule configuration.")

    #################### Load specific rule from the given yaml file.
    try:
        rule =  RuleConfig.from_yaml_file( yaml_file=args.config, rule_name=args.rulename, rule_substitutions=rule_substitutions )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)

    # CHATTY("Rule configuration:")
    # CHATTY(yaml.dump(rule.dict))
    
    filesystem = rule.job_config.filesystem
    DEBUG(f"Filesystem: {filesystem}")    

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
