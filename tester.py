import pathlib
import datetime
import yaml
import cProfile

# from dataclasses import fields
from logging.handlers import RotatingFileHandler
import pprint # noqa F401

from argparsing import submission_args
from ruleclasses import RuleConfig, MatchConfig
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401

from sphenixprodutils import extract_numbers_to_commastring
from sphenixprodutils import list_to_condition
from sphenixdbutils import test_mode as dbutils_test_mode

# ============================================================================================

def main():
    ### digest arguments
    args = submission_args()

    #################### Test mode? 
    test_mode = ( 
            dbutils_test_mode
            or args.test_mode
            # or ( hasattr(rule, 'test_mode') and rule.test_mode )
        )
    # No matter how we determined test_mode, make sure it is now propagated to job directories.
    # Note that further down we'll turn on transfer of the .testbed file to the worker
    if test_mode:
        pathlib.Path('.testbed').touch()

    #################### Set up submission logging before going any further
    if args.sublogdir:
        sublogdir=args.sublogdir
    else:
        if test_mode:
            sublogdir='/tmp/testbed/sphenixprod/'
        else:
            sublogdir='/tmp/sphenixprod/sphenixprod/'
    sublogdir += f"{args.rulename}".replace('.yaml','')
        
    pathlib.Path(sublogdir).mkdir( parents=True, exist_ok=True )
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

    #  Force copy our own files to the worker:
    # .testbed, .slurp (deprecated) indicate test mode
    append2rsync=""
    if args.append2rsync is not None:
        append2rsync = ","+args.append2rsync+ ",.testbed"+ ",.slurp/"
    else:
        append2rsync = ",.testbed"+ ",.slurp/"
    DEBUG(f"Addtional resources to be copied to the worker: {append2rsync}")
    rule_substitions["append2rsync"] = append2rsync

    ### Which runs to process?
    # TODO: Finish cleaner construction of run constraints
    run_condition = None
    runlist=None
    if args.runlist:
        INFO(f"Processing runs from file: {args.runlist}")
        run_condition = f"and runnumber in ( {extract_numbers_to_commastring(args.runlist)} )"
        runlist=args.runlist
    elif args.runs:
        INFO(f"Processing run (range): {args.runs}")
        run_condition,runlist = list_to_condition(args.runs, "runnumber")
    else:
        ERROR("No runs specified.")
        exit(1)
    DEBUG(f'Run condition is "{run_condition}"')

    # Limit the number of results from the query?
    limit_condition = ""
    if args.limit:
        limit_condition = f"limit {args.limit}"
        DEBUG(f"Limiting input query to {args.limit} entries.")

    # TODO: this is where the run cursor pickup logic should go, if kept

    DEBUG( f"Run condition is \"{run_condition}\"" )
    DEBUG( f"Limit condition is \"{limit_condition}\"" )

    if run_condition != "":
        run_condition = f"\t{run_condition}\n"
    if limit_condition != "":
        limit_condition = f"\t{limit_condition}\n"
    rule_substitions["input_query_constraints"] = f"""{run_condition}{limit_condition}"""
    rule_substitions["runlist"] = runlist

    # Rest of the input substitutions
    if args.mode is not None:
        rule_substitions["mode"] = args.mode # e.g. physics 

    if args.mangle_dstname:
        DEBUG("Mangling DST name")
        rule_substitions['DST']=args.mangle_dstname

    # filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    rule_substitions["prodmode"] = "production"
    if args.mangle_dirpath:
        rule_substitions["prodmode"] = args.mangle_dirpath

    WARN("Don't forget other override_args")
    ## TODO dbinput, mem, docstring, unblock, batch_name

    CHATTY(f"Rule substitutions: {rule_substitions}")
    DEBUG("Now loading and building rule configuration.")

    #################### Load specific rule from the given yaml file.
    try:
        rule =  RuleConfig.from_yaml_file( yaml_file=args.config, rule_name=args.rulename, rule_substitions=rule_substitions )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)

    CHATTY("Rule configuration:")
    CHATTY(yaml.dump(rule.dict))

    # if args.printquery:
    #     # prettyquery = pprint.pformat(rule.inputConfig.query)
    #     # 100 is the highest log level, it should always print
    #     slogger.log(100, "[Print constructed query]")
    #     slogger.log(100, rule.inputConfig.query)
    #     slogger.log(100, "[End of query]")
    #     exit(0)
 
    # TODO: add to sanity checks:
    # if rev==0 and build != 'new':
    #     logging.error( f'production version must be nonzero for fixed builds' )
    #     result = False

    # if rev!=0 and build == 'new':
    #     logging.error( 'production version must be zero for new build' )
    #     result = False


    #################### Rule and its subfields for input and job details now have all the information needed for submitting jobs
    INFO("Rule construction complete. Now constructing corresponding match configuration.")

    # Create a match configuration from the rule
    match_config = MatchConfig.from_rule_config(rule)  
    CHATTY("Match configuration:")
    CHATTY(yaml.dump(match_config.dict))

    match_config.doanewthing(args, runlist)
    exit(0)
    
    outputs = match_config.doyourthing(args)
    print(outputs)


    INFO( "KTHXBYE!" )

# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)

    cProfile.run('main()')
