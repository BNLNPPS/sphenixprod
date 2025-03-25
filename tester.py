import pathlib
import datetime
import yaml
import cProfile

from logging.handlers import RotatingFileHandler
from dataclasses import fields
import pprint

from simpleLogger import slogger, CustomFormatter, ERROR, WARN, INFO, DEBUG
from ruleclasses import RuleConfig, MatchConfig
from sphenixprodutils import extract_numbers_to_commastring
from sphenixprodutils import list_to_condition
from sphenixdbutils import cnxn_string_map, dbQuery, test_mode as dbutils_test_mode

from argparsing import submission_args

# =============================================================================================
from ruleclasses import DSTFMTv, RUNFMT, SEGFMT

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
    # No matter how we determined test_mode, make sure it is now propagated to job directories
    # note that further down we'll turn on transfer with rsync = rule.rsync + ",.testbed"
    if test_mode:
        pathlib.Path('.testbed').touch()

    #################### Set up logging before going any further
    if args.sublogdir:
        sublogdir=args.sublogdir
    elif test_mode:
        sublogdir=f"/tmp/testbed/sphenixprod/{args.rulename}"
    else:
        sublogdir=f"/tmp/sphenixprod/sphenixprod/{args.rulename}"
        
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
    # stdout is already added by default
    # If one cares, logging to stdout and file cans be at different levels and formats

    if test_mode:
        INFO("Running in testbed mode.")
        args.mangle_dirpath = 'production-testbed'    
    else:
        INFO("Running in production mode.")

    DEBUG(f"Logging to {sublogdir}")
    DEBUG(f"Log level: {args.loglevel}")

    #################### Load specific rule from the given yaml file.
    try:
        all_rules = RuleConfig.from_yaml_file(args.config)
        rule = all_rules[args.rulename]
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)
    except KeyError as e:
        ERROR(f"Error loading rule configuration: {e}")
        exit(1)

    DEBUG("Rule configuration:")
    DEBUG(yaml.dump(rule.dict))

    #################### Rule has steering parameters and two subclasses for input and job specifics
    # KK: params = config.get('params') is no longer a thing. Instead, RuleConfig has all the parameters
    # Note that slurp does a lot of essentially copy/pasting from the config file into the rule object
    # This is now done in the RuleConfig class yaml reader and only necessary updates and substitutions are done here
    ###  First, update the params that are 1-1 overwritten by the command line   
    ## TODO dbinput, mem, docstring, unblock, batch_name

    # .testbed, .slurp (deprecated), and any custom files/directories are copied to the worker 
    rsync = rule.rsync + ",.slurp/"
    rsync = rule.rsync + ",.testbed"
    if args.append2rsync:
        rsync = rsync + "," + args.append2rsync
        DEBUG(f"Appending to rsync: {args.append2rsync}")

    # Limit the number of results from the query?
    limit_condition = ""
    if args.limit:
        limit_condition = f"limit {args.limit}"
        DEBUG(f"Limiting input query to {args.limit} entries.")

    # Resubmitting jobs? Shouldn't be true in the yaml file, but can be set from the command line
    if args.resubmit :
        DEBUG("Sertting resubmit to true")
        rule.resubmit = True

    #################### InputConfig
    ### rule_input has a big SQL query template that needs to be filled in
    ### from command line arguments and parameters
    rule_input = rule.input 

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

    INFO( f"Basic constraints are \" where ... {run_condition} {seg_condition} {limit_condition}\"" )

    # TODO: this is where Jason added the cursor pickup

    ### Populate the input query with the above constraints
    # rule_input is a psql template, "**" unpacks a dictionary into keyword arguments
    # We could also hand over **locals() but that limits local variable names
    # Safer and less opaque to do it explicitly
    rule_input.query = rule_input.query.format(
            **rule.dict()
            , run_condition=run_condition
            , seg_condition=seg_condition
            , limit_condition=limit_condition
            )
    
    if args.printquery:
         # 100 is the highest log level, it should always print
        slogger.log(100, "[Print constructed query]")
        # prettyquery = pprint.pformat(rule.input.query)
        # slogger.log(100, prettyquery)
        slogger.log(100, rule.input.query)
        slogger.log(100, "[End of query]")
        exit(0)

    # Rest of the rule.input parameters, i.e. database name and direct path
    DEBUG (f"Using database {rule_input.db}")
    # Specified direct path may still need substitution
    if rule_input.direct_path is not None and args.mode is not None:
        rule_input.direct_path = rule_input.direct_path.format( mode=args.mode )
    DEBUG (f"Using direct path {rule_input.direct_path}")

    # KK, DELME: lfn2pfn is (used to be) an option
    # lfn2pfn provides a mapping between physical files on disk and the corresponding lfn
    # (i.e. the pfn with the directory path stripped off).
    # --> Not using it until we have to (no currently existing rules have it) 
    # KK, DELME: runlist_query is deprecated anyway
    # KK, DELME: kaedama does rule.name = rule.name.format( **locals() ), but no names with {} are in the yaml files
    #            Note that $(streamname) does exist, but that's populated differently and later

    if args.mangle_dstname:
        rule.name = rule.name.replace('DST',args.mangle_dstname)
        WARN(f"DST name is mangled to {rule.name}")

    # Just format strings
    rule.logbase=rule.logbase.format( RUNFMT=RUNFMT, SEGFMT=SEGFMT )
    rule.outbase=rule.outbase.format( RUNFMT=RUNFMT, SEGFMT=SEGFMT )

    # Note: rule.filesystem can be changed via yaml file, but existing files that do that 
    #       are not compatible with the current code because a lot of params would have to be made optional.
    #       Seems to be historical only and not worth the effort atm.

    #################### JobConfig
    rule_job=rule.job

    # filesystem is the base for all output, allow mangling here
    # "production" (in the default filesystem) is replaced
    if args.mangle_dirpath:
        print(args.mangle_dirpath)
        for key,val in rule.filesystem.items():
            rule.filesystem[key]=rule.filesystem[key].replace("production",args.mangle_dirpath)
            DEBUG(f"Filesystem: {key} is mangled to {rule.filesystem[key]}")

    # Fill remaining substitutables. Most importanly, fill in base paths
    for field in fields(rule_job):
        subsval = getattr(rule_job, field.name)
        if isinstance(subsval, str): # don't try changing None or dictionaries
            subsval = subsval.format( 
                  **rule.dict()
                , PWD=pathlib.Path(".").absolute()
                , **rule.filesystem
        )
        setattr(rule_job, field.name, subsval)
        DEBUG (f'Job after substitution: {field.name}: {getattr(rule_job, field.name)}')

    
    # TODO: add to sanity checks:
    # if rev==0 and build != 'new':
    #     logging.error( f'production version must be nonzero for fixed builds' )
    #     result = False

    # if rev!=0 and build == 'new':
    #     logging.error( 'production version must be zero for new build' )
    #     result = False



    #################### Rule and its subfields for input and job details now have all the information needed for submitting jobs
    INFO("Rule construction complete.")

    ## KK: Breaking open slurp's
    #dispatched = slurp.submit (rule, args.maxjobs, nevents=args.nevents, **submitkw, **filesystem )     
    ## KK: First step is  
    ## matching, setup, runlist = matches( rule, kwargs ) 
    # where kwargs = submitkw = params["mem","disk","dump", "neventsper"]
    # PLUS potentially "resubmit"
    ## So let's dig into that. 
    # Jason handover does seemingly nothing but add resubmit
    #    or revert to "mem","disk","dump", "neventsper" from the yaml even if they were specified on the command line
    # This seems to be leftover crud, if anything, allow args to override the yaml which isn't done this way
    # In any case, we should have already fixed all rule overrides from the command line at this time

    ###name      = kwargs.get('name',      rule.name)
    # ex DST_TRKR_CLUSTER_run3auau or DST_STREAMING_EVENT_$(streamname)_run3auau or  MANGLED_STREAMING_EVENT_$(streamname)_run3auau, 
    # overwritten only if --mangle_dstname is set, not explicitly. 
    ## TODO: add a check that it is consistent with the actual rule name, i.e args.rulename ~= rule.name

    ### build     = kwargs.get('build',     rule.build)      # TODO... correct handling from submit.  build=ana.xyz --> build=anaxyz buildarg=ana.xyz
    # ex. build='new', build='ana.472'
    # never overwritten by command line
    ### buildarg  = kwargs.get('buildarg',  rule.buildarg) should _always_ be either "new" or anaxyz for build=ana.xyz, note the .!
    # ex. buildarg='ana472' <-- add to sanity check; see above TODO

    ### tag       = kwargs.get('tag',       rule.tag)
    # ex. tag='2024p012'
    # never overwritten by command line
    # poorly named, filled via tag = params['dbtag'],  
    # Retain in some way for legacy because $tag is used in the job script for naming
        
    ### script    = kwargs.get('script',    rule.script)
    # ex. script='run.sh'
    # never overwritten by command line

    ### payload   = kwargs.get('payload',   rule.payload)
    # ex. payload=./ProdFlow/run3auau/streaming/
    # never overwritten by command line
    # should definitely be part of the rsync list but the yaml does that explicitly instead, e.g.
    #  payload :   ./ProdFlow/run3auau/streaming/
    #  rsync   : "./ProdFlow/run3auau/streaming/*"
    # TODO: add to sanity check

    ### update    = kwargs.get('update',    True ) # update the DB
    # huh. NEVER overwritten, NEVER in the yaml --> always True

    ### version  = rule.version
    # ex. version='1'
    # never overwritten by command line

    # Create a match configuration from the rule
    match_config = MatchConfig.from_rule_config(rule)

    # Formatted version number, needed to identify repeated new_nocdb productions, 0 otherwise
    DEBUG(f"Version string: {match_config.version_string}")

    # print()
    # print(f"MatchConfig from RuleConfig {rule.name}:")
    # pprint.pprint(match_config.dict())

    DEBUG("[Print cnxn_string]")
    DEBUG(pprint.pformat(cnxn_string_map[ rule.input.db ]))
    DEBUG("[End of cnxn_string]")
    DEBUG("[Print constructed query]")
    prettyquery = pprint.pformat(rule.input.query)
    DEBUG(prettyquery)
    DEBUG("[End of query]")

    dbresult = dbQuery( cnxn_string_map[ rule.input.db ], rule.input.query )

    for line in dbresult:
        # run     = line.runnumber
        # segment = line.segment
        run     = line[1]
        segment = line[2]
        runsegkey = f"{run}-{segment}"

        #streamname = getattr( line, 'streamname', None )
        #streamname = line[4]
        #print(runsegkey, streamname)
        streamname = None

        if streamname:
            rule.name = rule.name.replace( '$(streamname)',streamname ) # hack in condor replacement
            runsegkey = f"{run}-{segment}-{streamname}"

        # Build output name
        # e.g. DST_TRKR_CLUSTER_run3auau_ana.472_2024p012-00057655-00003.root
        
        ## A version string is now mandatory, the following is deprecated
        # if match_config.version_string is None:
        #     output_ = DSTFMT % (  match_config.name, match_config.buildarg, match_config.dbtag
        #                         , int(run), int(segment)) 
        # else:
        #     output_ = DSTFMTv % (  match_config.name, match_config.buildarg, match_config.dbtag, match_config.version
        #                         , int(run), int(segment))              

        output_ = DSTFMTv % (  match_config.name, match_config.buildarg
                            , match_config.dbtag, match_config.version_string
                            , int(run), int(segment))              
        print(output_)

        print(match_config.base_string + RUNFMT + "-" + SEGFMT + ".root")
        print(match_config.buildarg)



    # # Do not submit if we fail sanity check on definition file
    # if not sanity_checks( params, input_ ):
    #     ERROR( "Sanity check failed. Exiting." )
    #     exit(1)
        


    exit(0)

    INFO( "KTHXBYE!" )

# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)

    cProfile.run('main()')
