#!/usr/bin/env python

import argparse
import subprocess
import yaml
import platform
import pprint # noqa F401

from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401

# ============================================================================================
def main():
    """
    Specifically intended for cron jobs. Steered by a control yaml and based on the hostname,
    dispatch submission, spider, and any other processes for production jobs.
    """

    ### Minutiae
    args = steering_args()
    #setup_rot_handler(args)    
    slogger.setLevel(args.loglevel)
    
    hostname=args.hostname
    if not hostname:
        hostname=platform.node()
    hostname=hostname.split('.')[0]
    INFO(f"Invoked on {hostname}")

    ### Parse yaml
    try:
        with open(args.steerfile, "r") as yamlstream:
            yaml_data = yaml.safe_load(yamlstream)
    except yaml.YAMLError as exc:
        raise ValueError(f"Error parsing YAML file: {exc}")
    except FileNotFoundError:
        raise FileNotFoundError(f"YAML file not found: {args.steerfile}")
    CHATTY(f"YAML dict is:\n{yaml_data}")
    try:
        host_data = yaml_data[hostname]
    except KeyError:
        WARN(f"Host '{hostname}' not found in YAML data.")
        exit(0)
    INFO(f"Successfully loaded dict for {hostname}")
    INFO(f"Found {len(host_data)} rules.")
    CHATTY(f"YAML dict for {hostname} is:\n{host_data}")
        

    
    # subprocess.Popen(f"./popenscript.sh > popenresult",shell=True)


# ============================================================================
# ============================================================================================
def setup_rot_handler(args):
    # if not args.sublogdir:
    #     sublogdir='/tmp/sphenixprod/sphenixprod/'

    # sublogdir += f"{args.rulename}".replace('.yaml','')
    # Path(sublogdir).mkdir( parents=True, exist_ok=True )
    # RotFileHandler = RotatingFileHandler(
    #     filename=f"{sublogdir}/{str(datetime.today().date())}.log",
    #     mode='a',
    #     maxBytes=25*1024*1024, #   maxBytes=5*1024,
    #     backupCount=10,
    #     encoding=None,
    #     delay=0
    # )
    # RotFileHandler.setFormatter(CustomFormatter())
    # slogger.addHandler(RotFileHandler)

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

    
#def from_yaml(cls, yaml_file: str, yaml_data: Dict[str, Any])
# ============================================================================================

if __name__ == '__main__':
    

    main()
    exit(0)
