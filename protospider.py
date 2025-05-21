#!/bin/env python

from pathlib import Path
import datetime
import yaml
import cProfile
import subprocess
import sys
import shutil

# from dataclasses import fields
from logging.handlers import RotatingFileHandler
import pprint # noqa F401

from argparsing import submission_args
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig,list_to_condition, extract_numbers_to_commastring
from sphenixdbutils import test_mode as dbutils_test_mode

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
    # No matter how we determined test_mode, make sure it is now propagated to job directories.
    # Note that further down we'll turn on transfer of the .testbed file to the worker
    if test_mode:
        Path('.testbed').touch()

    #################### Set up submission logging before going any further
    if args.sublogdir:
        sublogdir=args.sublogdir
    else:
        if test_mode:
            sublogdir='/tmp/testbed/sphenixprod/'
        else:
            sublogdir='/tmp/sphenixprod/sphenixprod/'
    sublogdir += f"{args.rulename}".replace('.yaml','')

    Path(sublogdir).mkdir( parents=True, exist_ok=True )
    RotFileHandler = RotatingFileHandler(
        filename=f"{sublogdir}/{str(datetime.datetime.today().date())}.log",
        mode='a',
        maxBytes=25*1024*1024, #   maxBytes=5*1024,
        backupCount=10,
        encoding=None,
        delay=0
    )
    RotFileHandler.setFormatter(CustomFormatter())
    slogger.addHandler(RotFileHandler)
    slogger.setLevel(args.loglevel)
    
    # Exit without fuss if we are already running 
    p = subprocess.Popen(["ps","axuww"], stdout=subprocess.PIPE)
    stdout_bytes, stderr_bytes = p.communicate() # communicate() returns bytes
    stdout_str = stdout_bytes.decode(errors='ignore') # Decode to string
    # debug
    #stdout_str = 'python tester.py --config run3auau/NewDST_STREAMING_run3auau_new_2024p012.yaml --rule DST_STREAMING_EVENT_run3auau_streams --runs 50229 50400'
    
    # Construct a search key with script name, config file, and rulename
    # to check for other running instances with the same parameters.
    count_already_running = 0
    
    for psline in stdout_str.splitlines():
        if sys.argv[0] in psline and args.config in psline and args.rulename in psline:
            count_already_running += 1

    CHATTY ( f"Found {count_already_running} instance(s) of {sys.argv[0]} with config {args.config} and rulename {args.rulename} in the process list.")
    if count_already_running == 0:
        ERROR("No running instance found, including myself. That can't be right.")
        exit(1)

    if count_already_running > 1:
        DEBUG("Looks like there's already a running instance of me. Stop.")
        exit(0)

    # stdout is already added to slogger by default
    INFO(f"Logging to {sublogdir}, level {args.loglevel}")

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
    rule_substitions = {}
    rule_substitions["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor

    ### Which runs to process?
    run_condition = None
    if args.runlist:
        INFO(f"Processing runs from file: {args.runlist}")
        run_condition = f"and runnumber in ( {extract_numbers_to_commastring(args.runlist)} )"
    elif args.runs:
        INFO(f"Processing run (range): {args.runs}")
        run_condition = list_to_condition(args.runs, "runnumber")
    else:
        ERROR("No runs specified.")
        exit(1)

    # Limit the number of results from the query?
    limit_condition = ""
    if args.limit:
        limit_condition = f"limit {args.limit}"
        WARN(f"Limiting input query to {args.limit} entries.")

    DEBUG( f"Run condition is \"{run_condition}\"" )
    if limit_condition != "":
        DEBUG( f"Limit condition is \"{limit_condition}\"" )

    if run_condition != "":
        run_condition = f"\t{run_condition}\n"
    if limit_condition != "":
        WARN( f"For testing, limiting input query to {args.limit} entries. Probably not what you want." )
        limit_condition = f"\t{limit_condition}\n"
    
    rule_substitions["file_query_constraints"] = f"""{run_condition}{limit_condition}"""
    rule_substitions["status_query_constraints"] = f"""{run_condition.replace('runnumber','run')}{limit_condition}"""
    DEBUG(f"Input query constraints: {rule_substitions['file_query_constraints']}")
    DEBUG(f"Status query constraints: {rule_substitions['status_query_constraints']}")

    # Rest of the input substitutions
    if args.physicsmode is not None:
        rule_substitions["physicsmode"] = args.physicsmode # e.g. physics

    if args.mangle_dstname:
        DEBUG("Mangling DST name")
        rule_substitions['DST']=args.mangle_dstname

    # filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    rule_substitions["prodmode"] = "production"
    if args.mangle_dirpath:
        rule_substitions["prodmode"] = args.mangle_dirpath

    CHATTY(f"Rule substitutions: {rule_substitions}")
    INFO("Now loading and building rule configuration.")

    #################### Load specific rule from the given yaml file.
    try:
        rule =  RuleConfig.from_yaml_file( yaml_file=args.config, rule_name=args.rulename, rule_substitions=rule_substitions )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)

    CHATTY("Rule configuration:")
    CHATTY(yaml.dump(rule.dict))


    # Which find command to use?
    filesystem = rule.job_config.filesystem
    find = shutil.which('rbh-find')
    if find is not None:
        find = f"{find} -f /etc/robinhood.d/myfs.sphnxpro.conf"
        for k,v in filesystem.items():
            filesystem[k] = v.replace('/sphenix/lustre01', '/mnt')
    else:
        WARN("rbh-find (robinhood) not found.")
        find = shutil.which('lfs')
    
    if find is None:
        WARN("'lfs find' not found either.")
        find = shutil.which('find')
    INFO(f"Using {find}.")

    # Original output directory, the final destination, and the file name trunk
    inlocation=filesystem['outdir']
    outlocation=filesystem['finaldir']
    DEBUG(f"Filesystem: {filesystem}")
    INFO(f"Original output directory: {inlocation}")
    INFO(f"Final destination: {outlocation}")

    # List of files to process
    findcommand = f"{find} {inlocation} -type f -name {rule.rulestem}\* -print"
    INFO(f"Find command: {findcommand}")
    foundfiles = subprocess.run(findcommand, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()

    # Extract information encoded in the file name
    for file in foundfiles:
        try:
            fullpath,_,nevents,_,md5,_,dbid = file.split(':')
        except Exception as e:
            ERROR(f"Error: {e}")
            continue
        print(f"File: {Path(fullpath).name}")
        print(f"  nevents: {nevents}")
        print(f"  md5: {md5}")
        print(f"  dbid: {dbid}")


    

    exit(0)

# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)

    cProfile.run('main()', '/tmp/sphenixprod.prof')
    import pstats
    p = pstats.Stats('/tmp/sphenixprod.prof')
    p.strip_dirs().sort_stats('time').print_stats(10)

    # Sort the output by the following options:
    # calls: Sort by the number of calls made.
    # cumulative: Sort by the cumulative time spent in the function and its callees.
    # filename: Sort by file name.
    # nfl: Sort by name/file/line.
    # pcalls: Sort by the number of primitive calls.
    # stdname: Sort by standard name (default).
    # time: Sort by the total time spent in the function itself.    #
