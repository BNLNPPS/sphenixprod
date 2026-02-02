#!/usr/bin/env python

import argparse
import subprocess
import yaml
import platform
import cProfile
import pstats
import pprint # noqa F401
import sys
from typing import Dict, Any, Tuple

from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
from sphenixprodrules import check_params

from collections import namedtuple
SubmitDstHist = namedtuple('SubmitDstHist',['submit','dstspider','histspider','finishmon'])

# ============================================================================================
def main():
    """
    Specifically intended for cron jobs. Steered by a control yaml and based on the hostname,
    dispatch submission, spider, and any other processes for production jobs.
    """

    ### Minutiae
    args = steering_args()
    setup_my_rot_handler(args)
    slogger.setLevel(args.loglevel)

    hostname=args.hostname.split('.')[0] if isinstance(args.hostname,str) else None
    if not hostname:
        hostname=platform.node().split('.')[0]
        INFO(f"{sys.argv[0]} invoked on {hostname}")
    else:
        WARN(f"{sys.argv[0]} invoked for {hostname} but from {platform.node().split('.')[0]}")

    if args.profile:
        DEBUG( "Profiling is ENABLED.")
        profiler = cProfile.Profile()
        profiler.enable()

    ### Parse yaml
    try:
        INFO(f"Reading rules from {args.steerfile}")
        with open(args.steerfile, "r") as yamlstream:
            yaml_data = yaml.safe_load(yamlstream)
    except yaml.YAMLError as yerr:
        raise ValueError(f"Error parsing YAML file: {yerr}")
    except FileNotFoundError:
        ERROR(f"YAML file not found: {args.steerfile}")
        exit(1)

    try:
        host_data = yaml_data[hostname]
    except KeyError:
        WARN(f"Host '{hostname}' not found in {args.steerfile}")
        exit(1)

    ### defaultlocations is special
    # pop removes it so the remainder are rules
    # Note: Could be made optional for full path files, but too much fuss to be worth it.
    defaultlocations=host_data.pop("defaultlocations",None)
    if not defaultlocations:
        ERROR(f'Could not find field "defaultlocations" in the yaml for {hostname}')
        exit(1)
    prettyfs = pprint.pformat(defaultlocations)
    DEBUG(f"Default file locations:\n{prettyfs}")
    INFO(f"Successfully loaded {len(host_data)} rules for {hostname}")
    CHATTY(f"YAML dict for {hostname} is:\n{pprint.pformat(host_data)}")

    ### Walk through the rules.
    for rule in host_data:
        INFO(f"Working on {rule}")
        thisprod,ruleargs,sdh_tuple=collect_yaml_data(host_data=host_data,rule=rule,defaultlocations=defaultlocations,dryrun=args.dryrun)

        ### environment; pass-through arguments
        envline=f"source {thisprod}"
        ruleargs+=f" --loglevel {args.loglevel}"
        if args.dryrun:
            ruleargs+=" --dryrun"

        ### Loop through process types
        ### Go from fast to slow processes.
        ### They'll overlap anyway.
        INFO(f"sdh_tuple is {pprint.pformat(sdh_tuple)}")
        if sdh_tuple.histspider:
            procline=f"histspider.py {ruleargs}"
            execline=f"{envline}  &>/dev/null && {procline} &>/dev/null"
            DEBUG(f"Executing\n{execline}")
            if not args.dryrun:
                subprocess.Popen(f"{execline}",shell=True)

        if sdh_tuple.dstspider:
            procline=f"dstspider.py {ruleargs}"
            execline=f"{envline}  &>/dev/null && {procline} &>/dev/null"
            DEBUG(f"Executing\n{execline}")
            if not args.dryrun:
                subprocess.Popen(f"{execline}",shell=True)

        if sdh_tuple.submit:
            procline=f"create_submission.py {ruleargs}"
            ### Submission is special. Ideally (one day) we'd split up into creation that registers as "submitting"
            ### and submission which registers as "submitted". So far, do it in one go.
            procline+=" --andgo"
            execline=f"{envline}  &>/dev/null && {procline} &>/dev/null"
            DEBUG(f"Executing\n{execline}")
            if not args.dryrun:
                subprocess.Popen(f"{execline}",shell=True)

        if sdh_tuple.finishmon:
            procline=f"monitor_finish.py {ruleargs}"
            execline=f"{envline}  &>/dev/null && {procline} &>/dev/null"
            DEBUG(f"Executing\n{execline}")
            if not args.dryrun:
                subprocess.Popen(f"{execline}",shell=True)

    if args.profile:
        profiler.disable()
        DEBUG("Profiling finished. Printing stats...")
        stats = pstats.Stats(profiler)
        stats.strip_dirs().sort_stats('time').print_stats(8)

    INFO("All done.")
    exit(0)


# ============================================================================================
def collect_yaml_data( host_data: Dict[str, Any], rule: str, defaultlocations: str, dryrun: bool ) -> Tuple[str,str,SubmitDstHist]:
    rule_data=host_data[rule]
    check_params(  rule_data
                 , required=["config"]
                 , optional=["runs", "runlist",
                             "submit", "dstspider","histspider",
                             "finishmon",
                             "prodbase", "configbase",
                             "nevents",
                             "jobmem", "jobprio",
                             "force", "force_delete",
                             "cut_segment",
                             ])
    ### Local file location changes?
    prodbase=rule_data.get("prodbase",defaultlocations["prodbase"])
    configbase=rule_data.get("configbase",defaultlocations["configbase"])
    submitdir=rule_data.get("submitdir",defaultlocations["submitdir"])
    submitdir=submitdir.format(rule=rule)

    ### location of the this_sphenixprod script
    thisprod=f"{prodbase}/this_sphenixprod.sh"
    if not Path(thisprod).is_file():
        ERROR(f'Init script {thisprod} does not exist.')
        exit(1)

    ### construct arguments
    ruleargs=f"--rule {rule}"
    config=rule_data.get("config", None)
    if not config.startswith("/"):
        config=f"{configbase}/{config}"
    if not Path(config).is_file():
        ERROR(f"Cannot find config file {config}")
        exit(1)
    ruleargs += f" --config {config}"

    if not dryrun:
        Path(submitdir).mkdir( parents=True, exist_ok=True )
    ruleargs += f" --submitdir {submitdir}"

    runs=rule_data.get("runs", None)
    if runs:
        runs = map(str,runs)
        runs = '" "'.join(runs)
        ruleargs += f" --runs {runs}"
    runlist=rule_data.get("runlist", None)
    if runlist:
        ruleargs += f" --runlist {runlist}"
    if runs and runlist:
        ERROR( 'You cannot specify both "runs" and "runlist"')
        exit(1)

    ### More rare extra arguments
    nevents=rule_data.get("nevents", None)
    if nevents:
        ruleargs += f" --nevents {nevents}"
    jobmem=rule_data.get("jobmem", None)
    if jobmem:
        ruleargs += f" --mem {jobmem}"
    jobprio=rule_data.get("jobprio", None)
    if jobprio:
        ruleargs += f" --priority {jobprio}"
    cut_segment=rule_data.get("cut_segment", None)
    if cut_segment:
        ruleargs += f" --cut-segment {cut_segment}"

    ### Force options
    force=rule_data.get("force", False)
    force_delete=rule_data.get("force_delete", False)
    if force:
        WARN('"force" is not a good idea. Uncomment if you\'re sure')
        # ruleargs += " --force"
    if force_delete:
        WARN('"force_delete" is not a good idea. Uncomment if you\'re sure')
        #ruleargs += " --force-delete"

    ### Booleans for what to run
    sdh_tuple=SubmitDstHist(submit=rule_data.get("submit", False),
                            dstspider=rule_data.get("dstspider", True),
                            histspider=rule_data.get("histspider", rule_data.get("dstspider", True)),
                            finishmon=rule_data.get("finishmon", False)
                            )
    ## sanity
    for k,v in sdh_tuple._asdict().items():
        if not isinstance(v,bool):
            ERROR(f'Value of "{k}" must be (yaml-)boolean, got "{v}"')
            exit(1)

    ## cleanup
    while ruleargs.startswith(" "):
        ruleargs=ruleargs[1:-1]

    return thisprod,ruleargs,sdh_tuple

# ============================================================================================
def setup_my_rot_handler(args):
    sublogdir='/tmp/sphenixprod/sphenixprod/'

    sublogdir += f"{Path(args.steerfile).name}".replace('.yaml','')
    Path(sublogdir).mkdir( parents=True, exist_ok=True )
    RotFileHandler = RotatingFileHandler(
        filename=f"{sublogdir}/{str(datetime.today().date())}.log",
        mode='a',
        maxBytes=25*1024*1024, #   maxBytes=5*1024,
        backupCount=10,
        encoding=None,
        delay=0
    )
    RotFileHandler.setFormatter(CustomFormatter())
    slogger.addHandler(RotFileHandler)

    return sublogdir

# ============================================================================================
def steering_args():
    """Handle command line tedium for steering jobs."""
    arg_parser = argparse.ArgumentParser( prog='production_control.py',
                                          description='"Production manager to dispatch jobs depending on submit node."',
                                         )
    arg_parser.add_argument( '--steerfile', '-f', dest='steerfile', required=True,
                             help='Location of steering instructions per host' )

    arg_parser.add_argument( '--dryrun', '-n',
                             help="flag is passed through to the scripts", dest="dryrun", action="store_true")

    arg_parser.add_argument( '--hostname', dest='hostname', default=None,
                             help='Act as if running on [hostname]' )

    arg_parser.add_argument( '--profile',help="Enable profiling", action="store_true")

    vgroup = arg_parser.add_argument_group('Logging level')
    exclusive_vgroup = vgroup.add_mutually_exclusive_group()
    exclusive_vgroup.add_argument( '-v', '--verbose', help="Prints more information per repetition", action='count', default=0)
    exclusive_vgroup.add_argument( '-d', '--debug', help="Prints even more information", action="store_true")
    exclusive_vgroup.add_argument( '-c', '--chatty', help="Prints the most information", action="store_true")
    exclusive_vgroup.add_argument( '--loglevel', dest='loglevel', default='INFO',
                                   help="Specific logging level (CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL)" )

    args = arg_parser.parse_args()
    if args.verbose==1 :
        args.loglevel = 'INFO'
    if args.debug or args.verbose==2 :
        args.loglevel = 'DEBUG'
    if args.chatty or args.verbose==3 :
        args.loglevel = 'CHATTY'

    return args


# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)
