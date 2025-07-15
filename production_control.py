#!/usr/bin/env python

import argparse
import subprocess
import yaml
import platform
import pprint # noqa F401
import sys
from typing import Dict, Any, Tuple

from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
from sphenixprodrules import check_params

from collections import namedtuple
SubmitDstHist = namedtuple('SubmitDstHist',['submit','dstspider','histspider'])

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
    
    hostname=args.hostname
    if not hostname:
        hostname=platform.node()
    hostname=hostname.split('.')[0]
    INFO(f"{sys.argv[0]} invoked on {hostname}")

    ### Parse yaml
    try:
        INFO("Reading rules from {args.steerfile}")
        with open(args.steerfile, "r") as yamlstream:
            yaml_data = yaml.safe_load(yamlstream)
    except yaml.YAMLError as exc:
        raise ValueError(f"Error parsing YAML file: {exc}")
    except FileNotFoundError:
        raise FileNotFoundError(f"YAML file not found: {args.steerfile}")
    # prettydict = pprint.pformat(yaml_data)
    # CHATTY(f"YAML dict is:\n{prettydict}")
    try:
        host_data = yaml_data[hostname]
    except KeyError:
        WARN(f"Host '{hostname}' not found in YAML data.")
        exit(0)
    
    ### defaultlocations is special
    # pop removes it so the remainder are rules
    # Note: Could be made optional for full path files, but too much fuss to be worth it.
    defaultlocations=host_data.pop("defaultlocations",None)
    if not defaultlocations:
        ERROR(f'Could not find field "defaultlocations" in the yaml for {hostname}')
        exit(1)
    
    prettyfs = pprint.pformat(defaultlocations)
    DEBUG(f"Default file locations:\n{prettyfs}")
    INFO(f"Successfully loaded {len(host_data)-1} rules for {hostname}")
    prettyyaml = pprint.pformat(host_data)
    CHATTY(f"YAML dict for {hostname} is:\n{prettyyaml}")

    ### Walk through the rules.
    for rule in host_data:        
        INFO(f"Working on {rule}")
        thisprod,ruleargs,sdh_tuple=collect_yaml_data(host_data[rule],defaultlocations)
        if args.dryrun:
            ruleargs+=" --dryrun"
        print(thisprod)
        print(ruleargs)
        print(sdh_tuple)

    # subprocess.Popen(f"./popenscript.sh > popenresult",shell=True)


# ============================================================================================
def collect_yaml_data( rule_data: [str, Any], defaultlocations: str ) -> Tuple[str,str,SubmitDstHist]:
    check_params(  rule_data
                 , required=["config"]
                 , optional=["runs", "runlist",
                             "submit", "dstspider","histspider",
                             "prodbase", "configbase",
                             "jobmem", "jobprio",
                             ])
    ### Local file location changes?
    prodbase=rule_data.get("prodbase",defaultlocations["prodbase"])
    configbase=rule_data.get("configbase",defaultlocations["configbase"])

    ### location of the this_sphenixprod script    
    thisprod=f"{prodbase}/this_sphenixprod.sh"
    if not Path(thisprod).is_file():
        ERROR(f'Init script {thisprod} does not exist.')
        exit(1)
    
    ### construct arguments    
    ruleargs=""
    config=rule_data.get("config", None)
    if not config.startswith("/"):
        config=f"{configbase}/{config}"
    if not Path(config).is_file():
        ERROR(f"Cannot find config file {config}")
        exit(1)
    ruleargs += f" --config {config}"

    runs=rule_data.get("runs", None)
    if runs:
        runs = map(str,runs)
        runs = '" "'.join(runs)
        ruleargs += f" --runs {runs}"
    runlist=rule_data.get("runlist", None)
    if runlist:
        ruleargs += f" --runlist {runlist}"
    if runs and runlist:
        ERROR(f'You cannot specify both "runs" and "runlist"')
        exit(1)

    ### More rare extra arguments
    jobmem=rule_data.get("jobmem", None)
    if jobmem:
        ruleargs += f" --mem {jobmem}"
    jobprio=rule_data.get("jobprio", None)
    if jobprio:
        ruleargs += f" --priority {jobprio}"
        
    ### Booleans for what to run
    sdh_tuple=SubmitDstHist(submit=rule_data.get("submit", True),
                            dstspider=rule_data.get("dstspider", True),
                            histspider=rule_data.get("histspider", True),
                            )
    ## sanity
    for k,v in sdh_tuple._asdict().items():
        if not isinstance(v,bool):
            ERROR(f'Value of "{k}" must be (yaml-)boolean, got "{v}"')
            exit(1)
        
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

# ============================================================================
def steering_args():
    """Handle command line tedium for steering jobs."""
    arg_parser = argparse.ArgumentParser( prog='production_control.py',
                                          description='"Production manager to dispatch jobs depending on submit node."',
                                         )
    arg_parser.add_argument( '--steerfile', '-f', dest='steerfile',
                             default='/sphenix/u/sphnxpro/devkolja/ProdFlow/short/run3auau/run3auau_production.yaml',
                             help='Location of steering instructions per host' )

    arg_parser.add_argument( '--dryrun', '-n',
                             help="Take no action. Just print things", dest="dryrun", action="store_true")

    arg_parser.add_argument( '--hostname', dest='hostname', default=None,
                             help='Act as if running on [hostname]' )

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
