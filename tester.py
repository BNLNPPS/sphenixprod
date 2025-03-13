import pathlib
import datetime
import yaml
import cProfile

from logging.handlers import RotatingFileHandler
from dataclasses import fields

from simpleLogger import slogger, CustomFormatter, ERROR, WARN, INFO, DEBUG
from ruleclasses import RuleConfig
from utils import extract_numbers_to_commastring
from utils import list_to_condition

from argparsing import submission_args

# ============================================================================================
# Format strings for run and segment numbers.  n.b. that the "rungroup" which defines the logfile and output file directory structure
# hardcodes "08d" as the run format...  
RUNFMT = "%08i"
SEGFMT = "%05i"

# ============================================================================================

def main():
    ### digest arguments
    args = submission_args()

    #################### Test mode? Multiple ways to turn it on
    ### TODO: ".slurp" is outdated as a name, just using it for backward compatibility
    test_mode = ( 
            False
            or args.test_mode
            or 'testbed' in str(pathlib.Path(".").absolute()).lower()
            or pathlib.Path(".slurp/testbed").is_file() # deprecated
            or pathlib.Path(".testbed").is_file()
            # or ( hasattr(rule, 'test_mode') and rule.test_mode )
        )

    #################### Set up logging before going any further
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
    # stdout is already added by default
    # If one cares, logging to stdout and file cans be at different levels and formats

    if test_mode:
        INFO("Running in testbed mode.")
        args.mangle_dirpath = 'production-testbed'    
    else:
        INFO("Running in production mode.")

    DEBUG(f"Logging to {mylogdir}")
    DEBUG(f"Log level: {args.loglevel}")

    #################### Load specific rule from the given yaml file.
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

    #################### Rule has steering parameters and two subclasses for input and job specifics
    ### rule_input has a big SQL query template that needs to be filled in
    ### from command line arguments and parameters
    rule_input   = rule.input 

    ### Which runs to process?
    if args.runlist:
        INFO(f"Processing runs from file: {args.runlist}")
        extract_numbers_to_commastring(args.runlist)
        run_condition = f"and runnumber in ( {extract_numbers_to_commastring(args.runlist)} )"
    else:
        run_condition = list_to_condition(args.runs, "runnumber")

    if not run_condition:
        ERROR("No runs specified.")
        exit(1)
    DEBUG(f"Run condition is  \"{run_condition}\"")

    ### Which segments to process?
    seg_condition = ""
    if seg_condition:
        seg_condition = list_to_condition(args.segments, "segment")
        if "seg_condition" not in rule_input.query:
            WARN("A segment condition specified but will be ignored by the rule.")
        DEBUG(f"Segment condition is \"{seg_condition}\"")
    else:
        DEBUG("No segment condition specified.")

    ### Limit the number of results from the query?
    limit_condition = ""
    if args.limit:
        limit_condition = f"limit {args.limit}"
        DEBUG(f"Limiting input query to {args.limit} entries.")

    INFO( f"Basic constraints are \" where ... {run_condition} {seg_condition} {limit_condition}\"" )

    # TODO: this is where Jason added the cursor pickup

    ### Populate the input query with the above constraints
    # rule_input is a psql template, "**" unpacks a dictionary into keyword arguments
    # We could also hand over **locals() but that limits local variable names
    # Safer and less opaque to do it explicitly
    input_query  = rule_input.query.format(
            **rule.dict()
            , run_condition=run_condition
            , seg_condition=seg_condition
            , limit_condition=limit_condition
            )
    
    if args.printquery:
        slogger.log(100, input_query) # 100 is the highest log level, it should always print
        return 0

    ### Rest of the rule.input parameters, i.e. database name and direct path
    DEBUG (f"Using database {rule_input.db}")
    if rule_input.direct_path is not None :
        # A specified direct path may still need substitution for the physics mode
        rule_input.direct_path = rule_input.direct_path.format( **vars( args ))
    DEBUG (f"Using direct path {rule_input.direct_path}")

    ### Rest of the rule parameters
    # KK, DELME: params = config.get('params') is no longer a thing. Instead, RuleConfig has all the parameters
    # Note that slurp did a lot of essentially copy/pasting from the config file into the rule object
    # This is now done in the RuleConfig class yaml reader and only necessary updates and substitutions are done here

    # KK, DELME: lfn2pfn is (used to be) an option
    # lfn2pfn provides a mapping between physical files on disk and the corresponding lfn
    # (i.e. the pfn with the directory path stripped off).
    # Not using it until we have to (no currently existing rules have it) 

    # KK, DELME: runlist_query is deprecated anyway

    # KK, DELME: kaedama does rule.name = rule.name.format( **locals() ) here, but no names with {} are in the yaml files

    if args.mangle_dstname:
        rule.name = rule.name.replace('DST',args.mangle_dstname)            
        WARN(f"DST name is mangled to {rule.name}")

    # KK:  args.mangle_dirpath handled later

    # Just format strings, RUNFMT and SEGFMT from the top
    for base in [rule.outbase, rule.logbase]:
        base=base.format( **globals( ) )

    # Locations. KK, Delme: 
    rule_job=rule.job

    for field in fields(rule_job):
        print(f'{field.name}: {getattr(rule_job, field.name)}')

    ### Make sure ".slurp" and any custom files/directories are copied to the worker
    rsync = rule.rsync + ",.slurp/"
    rsync = rule.rsync + ",.testbed"
    if args.append2rsync:
        rsync = rsync + "," + args.append2rsync
        DEBUG(f"Appending to rsync: {args.append2rsync}")


    
    INFO( "KTHXBYE!" )

# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)

    cProfile.run('main()')
