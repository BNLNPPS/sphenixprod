#!/usr/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import subprocess
import os
import sys

import pprint # noqa F401
if os.uname().sysname!='Darwin' :
    import htcondor # type: ignore

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit, make_chunks
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig, MatchConfig,list_to_condition, extract_numbers_to_commastring
from sphenixprodrules import pRUNFMT,pSEGFMT
from sphenixcondorjobs import CondorJob
from sphenixdbutils import test_mode as dbutils_test_mode
import importlib.util # to resolve the path of sphenixdbutils without importing it as a whole
from sphenixdbutils import cnxn_string_map, dbQuery

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
    # Set up submission logging before going any further
    sublogdir=setup_rot_handler(args)
    slogger.setLevel(args.loglevel)
    
    # Exit without fuss if we are already running 
    if should_I_quit(args=args, myname=sys.argv[0]):
        DEBUG("Stop.")
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
    rule_substitions["nevents"] = args.nevents
    
    payload_list=[]
    ### Copy our own files to the worker:
    # For database access - from _production script_ directory
    script_path = Path(__file__).parent.resolve()
    payload_list += [ importlib.util.find_spec('sphenixdbutils').origin ]
    payload_list += [ importlib.util.find_spec('simpleLogger').origin ]
    payload_list += [ f"{script_path}/stageout.sh" ]
    payload_list += [ f"{script_path}/GetNumbers.C" ]
    
    # .testbed, .slurp (deprecated): indicate test mode -- Search in the _submission_ directory
    if Path(".testbed").exists():
        payload_list += [str(Path('.testbed').resolve())]
    if Path(".slurp").exists():
        WARN('Using a ".slurp" file or directory is deprecated')
        payload_list += [str(Path('.slurp').resolve())]

    # from command line - the order means these can overwrite the default files from above
    if args.append2rsync:
        payload_list.insert(args.append2rsync)
        
    DEBUG(f"Addtional resources to be copied to the worker: {payload_list}")
    rule_substitions["payload_list"] = payload_list

    ### Which runs to process?
    run_condition = None
    if args.runlist:
        INFO(f"Processing runs from file: {args.runlist}")
        run_cond_text, _ = extract_numbers_to_commastring(args.runlist)
        run_condition = f"and runnumber in ({run_cond_text} )"
    elif args.runs:
        INFO(f"Processing run (range): {args.runs}")
        run_condition, _ = list_to_condition(args.runs, "runnumber")        
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

    if args.mem:
        DEBUG(f"Setting memory to {args.mem}")
        rule_substitions['mem']=args.mem

    # rule.filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    rule_substitions["prodmode"] = "production"
    if args.mangle_dirpath:
        rule_substitions["prodmode"] = args.mangle_dirpath

    #WARN("Don't forget other override_args")
    ## TODO? dbinput, mem, docstring, unblock, batch_name

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

    # Assign shared class variables for CondorJob
    # Note: If these need to differ per instance, they shouldn't be ClassVar
    # CondorJob.script                = rule.job_config.script
    # CondorJob.neventsper            = rule.job_config.neventsper
    # 04/25/2025: accounting_group and accounting_group_user should no longer be set,
    # submit host will do this automatically.
    # CondorJob.accounting_group      = rule.job_config.accounting_group
    # CondorJob.accounting_group_user = rule.job_config.accounting_group_user

    # if args.printquery:
    #     # prettyquery = pprint.pformat(rule.inputConfig.query)
    #     # 100 is the highest log level, it should always print
    #     slogger.log(100, "[Print constructed query]")
    #     slogger.log(100, rule.inputConfig.query)
    #     slogger.log(100, "[End of query]")
    #     exit(0)

    #################### Rule and its subfields for input and job details now have all the information needed for submitting jobs
    INFO("Rule construction complete. Now constructing corresponding match configuration.")

    # Create a match configuration from the rule
    match_config = MatchConfig.from_rule_config(rule)
    CHATTY("Match configuration:")
    CHATTY(yaml.dump(match_config.dict))

    # Note: matches() is keyed by output file names, but the run scripts use the output base name and separately the run number    
    rule_matches=match_config.matches()
    INFO(f"Matching complete. {len(rule_matches)} jobs to be submitted.")
        
    # for out_file,(in_files, outbase, logbase, run, seg, daqhost, leaf) in rule_matches.items():
    #     CHATTY(f"Run:     {run}, Seg:  {seg}")
    #     CHATTY(f"daqhost:    {daqhost}, Leaf:    {leaf}")
    #     CHATTY(f"Outbase: {outbase}  Output: {out_file}")
    #     CHATTY(f"Logbase: {logbase}")
    #     CHATTY(f"nInput:  {len(in_files)}\n")

    if os.uname().sysname=='Darwin' :
        WARN("Running on native Mac, cannot use condor.")
        WARN("Exiting early.")
        exit(0)
    
    submission_dir = Path('./tosubmit').resolve()
    if not args.dryrun:
        Path( submission_dir).mkdir( parents=True, exist_ok=True )
    subbase = f'{rule.rulestem}_{rule.outstub}_{rule.dataset}'
    INFO(f'Submission files based on {subbase}')

    # For a fairly collision-safe identifier that could be used to not trample on existing files:
    # import nanoid
    # short_id = nanoid.generate(size=6)
    # print(f"Short ID: {short_id}")

    # Check for and remove existing submission files for this subbase
    existing_sub_files =  list(Path(submission_dir).glob(f'{subbase}*.in'))
    existing_sub_files += list(Path(submission_dir).glob(f'{subbase}*.sub'))
    if existing_sub_files:
        WARN(f"Removing {int(len(existing_sub_files)/2)} existing submission file pairs for base: {subbase}")
        for f_to_delete in existing_sub_files: 
            CHATTY(f"Deleting: {f_to_delete}")
            if not args.dryrun:
                Path(f_to_delete).unlink() # could unlink the entire directory instead

    # Header for all submission files
    CondorJob.job_config = rule.job_config
    base_job = htcondor.Submit(CondorJob.job_config.condor_dict())

    # Individual submission file pairs are created to handle chunks of jobs
    chunk_size = 500
    chunked_jobs = make_chunks(list(rule_matches.items()), chunk_size)
    for i, chunk in enumerate(chunked_jobs):
        DEBUG(f"Creating submission files for chunk {i+1} of {len(rule_matches)//chunk_size + 1}")

        # len(chunked_jobs) doesn't work, it's a generator
        if not args.dryrun:
            with open(f'{submission_dir}/{subbase}_{i}.sub', "w") as condor_subfile:
                condor_subfile.write(str(base_job))
                condor_subfile.write(
f"""
log = $(log)
output = $(output)
error = $(error)
arguments = $(arguments)
queue log,output,error,arguments from {submission_dir}/{subbase}_{i}.in
""")
        prod_state_rows=[]
        condor_rows=[]
        for out_file,(in_files, outbase, logbase, run, seg, daqhost, leaf) in chunk:
            # Create .in file row
            condor_job = CondorJob.make_job( output_file=out_file, 
                                             inputs=in_files,
                                             outbase=outbase,
                                             logbase=logbase,
                                             leafdir=leaf,
                                             run=run,
                                             seg=seg,
                                             daqhost=daqhost,
                                            )
            condor_rows.append(condor_job.condor_row())

            # Make sure directories exist
            if not args.dryrun:
                Path(condor_job.outdir).mkdir( parents=True, exist_ok=True ) # dstlake on lustre
                Path(condor_job.histdir).mkdir( parents=True, exist_ok=True ) # dstlake on lustre
                
                # stdout, stderr, and condorlog locations, usually on sphenix02:
                for file_in_dir in condor_job.output, condor_job.error, condor_job.log :
                    Path(file_in_dir).parent.mkdir( parents=True, exist_ok=True )
                    
            # Add to production database
            # FIXME: inputs, prod_id
            dsttype=logbase.split(f'_{rule.dataset}')[0]
            dstfile=f'{outbase}-{run:{pRUNFMT}}-{0:{pSEGFMT}}' # Does NOT have ".root" extension
            # FIXME: SEGMENT?
            # Following is fragile, don't add spaces
            prod_state_rows.append ("('{dsttype}','{dstname}','{dstfile}',{run},{segment},{nsegments},'{inputs}',{prod_id},{cluster},{process},'{status}','{timestamp}','{host}')".format(
                dsttype=dsttype,
                dstname=outbase,
                dstfile=dstfile,
                run=run, segment=seg,
                nsegments=0, # CHECKME
                inputs='dbquery',
                prod_id=0, # CHECKME
                cluster=0, process=0,
                status="submitting",
                timestamp=str(datetime.now().replace(microsecond=0)),
                host=os.uname().nodename.split('.')[0]
            ))
            # # end of chunk loop

        comma_prod_state_rows=',\n'.join(prod_state_rows)
        insert_prod_state = f"""
insert into production_status
( dsttype, dstname, dstfile, run, segment, nsegments, inputs, prod_id, cluster, process, status, submitting, submission_host )
values 
{comma_prod_state_rows}
returning id
"""
        CHATTY(insert_prod_state+";")
        
        # important note: dstfile is not UNIQUE, so we can't detect conflict here and need to rely
        # on catching already submitted files earlier
        # could doublecheck with a query here
        # Also important: "id" can be (zipped with and) handed to the arguments. Good if workers update the db, otherwise questionable 
        # print(insert_prod_state)
        # exit()

        if not args.dryrun:
            # Register in the db, hand the ids the condor job (for faster db access; usually passed through to head node daemons)
            prod_curs = dbQuery( cnxn_string_map['statw'], insert_prod_state )
            prod_curs.commit()
            ids=[str(id) for (id,) in prod_curs.fetchall()]
            CHATTY(f"Inserted {len(ids)} rows into production_status, IDs: {ids}")
            # condor_rows=list(zip(condor_rows,ids)) # zip the ids to the condor rows, so that we can use them in the arguments
            # condor_rows=','.join( pair for pair in zip(condor_rows, ids) )
            # print(a)
            condor_rows=[ f"{x} {y}" for x,y in list(zip(condor_rows, ids))]
 
        if not args.dryrun:
            with open(f'{submission_dir}/{subbase}_{i}.in', "w") as condor_infile:
                condor_infile.writelines(row+'\n' for row in condor_rows)
                
    if len(rule_matches) ==0 :
        INFO("No jobs to submit.")
    else:
        INFO(f"Created {i+1} submission file pairs in {submission_dir} for {len(rule_matches)} jobs.")

    prettyfs = pprint.pformat(rule.job_config.filesystem)
    INFO(f"Other location templates:\n{prettyfs}")

    if args.andgo and not args.dryrun:
        sub_files = list(Path(submission_dir).glob(f'{subbase}*.sub'))
        for sub_file in sub_files:
            INFO(f"Submitting {sub_file}")
            subprocess.run(f"condor_submit {sub_file}",shell=True)
    
    INFO( "KTHXBYE!" )

    # for m in matching:
    #     run     = int(m['run'])
    #     segment = int(m['seg'])
    #     name    = m['name']
    #     streamname = m.get( 'streamname', None )
    #     name_ = name
    #     if streamname:
    #         name_ = name.replace("$(streamname)",streamname)

    #     dstfileinput = m['lfn'].split('.')[0]

    #     if m['inputs']:
    #         dstfileinput=m['inputs']

    #     # TODO: version ???
    #     # TODO: is dstfile and key redundant ???
    #     version = m.get('version',None)
    #     dsttype = name_
    #     dstname = dsttype +'_'+setup.build.replace(".","")+'_'+setup.dbtag
    #     if version:
    #         dstname = dstname + "_" + version  
    #     dstfile = ( dstname + '-' + RUNFMT + '-' + SEGFMT ) % (run,segment)        

    #     # TODO: version???
    #     key = sphenix_base_filename( setup.name, setup.build, setup.dbtag, run, segment, version )
        
    #     prod_id = setup.id

    #     # Cluster and process are unset during at this pint
    #     cluster = 0
    #     process = 0

    #     status  = state        

    #     timestamp=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )

    #     # TODO: Handle conflict
    #     node=platform.node().split('.')[0]

    #     value = f"('{dsttype}','{dstname}','{dstfile}',{run},{segment},0,'{dstfileinput}',{prod_id},{cluster},{process},'{status}', '{timestamp}', 0, '{node}' )" 

    #     if streamname:
    #         value = value.replace( '$(streamname)', streamname )

    #     values.append( value )
       
    # insvals = ','.join(values)    
    # insert = f"""
    # insert into production_status
    #        (dsttype, dstname, dstfile, run, segment, nsegments, inputs, prod_id, cluster, process, status, submitting, nevents, submission_host )
    # values 
    #        {insvals}
    
    # returning id
    # """
    # on conflict
    # on constraint {files_table}_pkey
    # do update set 
    # time=EXCLUDED.time,
    # size=EXCLUDED.size,
    # md5=EXCLUDED.md5
    # ;
    # """

    # TODO: add to sanity checks:
    # if rev==0 and build != 'new':
    #     ERROR( f'production version must be nonzero for fixed builds' )
    #     result = False

    # if rev!=0 and build == 'new':
    #     ERROR.error( 'production version must be zero for new build' )
    #     result = False

    # TODO: Find the right class to store update, updateDb, etc.
    # update    = kwargs.get('update',    True ) # update the DB
    # updateDb= not args.submit


    # # Do not submit if we fail sanity check on definition file
    # if not sanity_checks( params, input_ ):
    #     ERROR( "Sanity check failed. Exiting." )
    #     exit(1)

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
