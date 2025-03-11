import pathlib
import datetime
import yaml
import cProfile

from logging.handlers import RotatingFileHandler

from simpleLogger import slogger, CustomFormatter, ERROR, INFO, DEBUG
from ruleclasses import RuleConfig
from utils import extract_numbers_to_commastring
from utils import list_to_condition

from argparsing import submission_args
# Keep arguments and the parser global so we can access them everywhere and 
# so submodules can add to them a la
# https://mike.depalatis.net/blog/simplifying-argparse
args     = None
userargs = None

# ============================================================================================

def main():

    # digest arguments
    args = submission_args()

    # better safe than sorry, we have multiple ways to signalize test mode
    test_mode = ( 
            False
            or args.test_mode
            or 'testbed' in str(pathlib.Path(".").absolute()).lower()
            or pathlib.Path(".slurp/testbed").is_file()
            # or ( hasattr(rule, 'test_mode') and rule.test_mode )
        )

    # Set up logging before going any further
    if args.logdir:
        mylogdir=args.logdir
    elif test_mode:
        mylogdir=f"/tmp/testbed/sphenixprod/{args.rule}"
    else:
        mylogdir=f"/tmp/sphenixprod/sphenixprod/{args.rule}"
        
    pathlib.Path(mylogdir).mkdir( parents=True, exist_ok=True )
    RotFileHandler = RotatingFileHandler(
        filename=f"{mylogdir}/{str(datetime.datetime.today().date())}.log",
        mode='a',
        maxBytes=25*1024*1024, #   maxBytes=5*1024,
        backupCount=10,
        encoding=None,
        delay=0
    )
    RotFileHandler.setFormatter(CustomFormatter())
    slogger.addHandler(RotFileHandler)
    slogger.setLevel(args.loglevel)

    if test_mode:
        INFO("Running in testbed mode.")
        args.mangle_dirpath = 'production-testbed'    
    else:
        INFO("Running in production mode.")

    DEBUG(f"Logging to {mylogdir}")
    DEBUG(f"Log level: {args.loglevel}")

    # Load specific rule from the given yaml file.
    try:
        all_rules = RuleConfig.from_yaml_file(args.config)
        rule = all_rules[args.rule]
        INFO(f"Successfully loaded rule configuration: {args.rule}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)
    except KeyError as e:
        ERROR(f"Error loading rule configuration: {e}")
        exit(1)

    DEBUG("Rule configuration:")
    DEBUG(yaml.dump(rule.dict))
    
    # Which runs to process?
    if args.runlist:
        INFO(f"Processing runs from file: {args.runlist}")
        extract_numbers_to_commastring(args.runlist)
        run_condition = f"and runnumber in ( {extract_numbers_to_commastring(args.runlist)} )"
    else:
        run_condition = list_to_condition(args.runs, "runnumber")

    if not run_condition:
        ERROR("No runs specified.")
        exit(1)

    INFO(f"Run condition is  \"{run_condition}\"")

    # Which segments to process?
    seg_condition = list_to_condition(args.segments, "segment")
    if seg_condition:
        INFO(f"Segment condition is \"{seg_condition}\"")
    else:
        INFO("No segment condition specified.")
        seg_condition = ""
    
    
    INFO( "KTHXBYE!" )

# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)

    cProfile.run('main()')
