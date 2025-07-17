#!/usr/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import pstats
import subprocess
import os
import sys

import pprint # noqa F401
if os.uname().sysname!='Darwin' :
    import htcondor # type: ignore

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit, make_chunks
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig, MatchConfig
from sphenixprodrules import pRUNFMT,pSEGFMT
from sphenixjobdicts import inputs_from_output
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

    #################### Set up submission logging before going any further
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
    rule_substitutions["nevents"] = args.nevents
    
    payload_list=[]
    ### Copy our own files to the worker:
    # For database access - from _production script_ directory
    script_path = Path(__file__).parent.resolve()

    # Safely add module origins
    sphenixdbutils_spec = importlib.util.find_spec('sphenixdbutils')
    if sphenixdbutils_spec and sphenixdbutils_spec.origin:
        payload_list += [sphenixdbutils_spec.origin]
    else:
        ERROR("sphenixdbutils module not found.")
        exit(1)

    simplelogger_spec = importlib.util.find_spec('simpleLogger')
    if simplelogger_spec and simplelogger_spec.origin:
        payload_list += [simplelogger_spec.origin]
    else:
        ERROR("simpleLogger module not found.")
        exit(1)

    payload_list += [ f"{script_path}/stageout.sh" ]
    payload_list += [ f"{script_path}/GetNumbers.C" ]
    payload_list += [ f"{script_path}/create_filelist_run_daqhost.py" ]
    payload_list += [ f"{script_path}/create_filelist_run_seg.py" ]
    payload_list += [ f"{script_path}/create_full_filelist_run_seg.py" ]
    
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
    rule_substitutions["payload_list"] = payload_list

    # Rest of the input substitutions
    if args.physicsmode is not None:
        rule_substitutions["physicsmode"] = args.physicsmode # e.g. physics

    if args.mangle_dstname:
        DEBUG("Mangling DST name")
        rule_substitutions['DST']=args.mangle_dstname

    if args.mem:
        DEBUG(f"Setting memory to {args.mem}")
        rule_substitutions['mem']=args.mem

    if args.mem:
        DEBUG(f"Setting priority to {args.priority}")
        rule_substitutions['priority']=args.priority

    # rule.filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    rule_substitutions["prodmode"] = "production"
    if args.mangle_dirpath:
        rule_substitutions["prodmode"] = args.mangle_dirpath

    #WARN("Don't forget other override_args")
    ## TODO? dbinput, mem, docstring, unblock, batch_name

    CHATTY(f"Rule substitutions: {rule_substitutions}")
    INFO("Now loading and building rule configuration.")

    #################### Load specific rule from the given yaml file.
    try:
        rule =  RuleConfig.from_yaml_file( yaml_file=args.config, rule_name=args.rulename, rule_substitutions=rule_substitutions )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)

    CHATTY("Rule configuration:")
    CHATTY(yaml.dump(rule.dict))
    

    #################### Rule and its subfields for input and job details now have all the information needed for submitting jobs
    INFO("Rule construction complete. Now constructing corresponding match configuration.")

    # Create a match configuration from the rule
    match_config = MatchConfig.from_rule_config(rule)
    CHATTY("Match configuration:")
    CHATTY(yaml.dump(match_config.dict))

    # Note: matches() is keyed by output file names, but the run scripts use the output base name and separately the run number    
    rule_matches=match_config.matches()
    INFO(f"Matching complete. {len(rule_matches)} jobs to be submitted.")
        
    if os.uname().sysname=='Darwin' :
        WARN("Running on native Mac, cannot use condor.")
        WARN("Exiting early.")
        exit(0)
    
    submission_dir = Path('./tosubmit').resolve()
    if not args.dryrun:
        Path( submission_dir).mkdir( parents=True, exist_ok=True )
    subbase = f'{rule.dsttype}_{rule.dataset}_{rule.outtriplet}'
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
        print(base_job)
        exit()
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
            dsttype=logbase.split(f'_{rule.dataset}')[0]
            if 'TRIGGERED_EVENT' in dsttype or 'STREAMING_EVENT' in dsttype: # TODO: FIXME for those as well
                dstfile=f'{outbase}-{run:{pRUNFMT}}-{0:{pSEGFMT}}' # Does NOT have ".root" extension
            else:
                dstfile=out_file # this is much more robust and correct
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
        #print(insert_prod_state)
        # important note: dstfile is not UNIQUE, so we can't detect conflict here and need to rely
        # on catching already submitted files earlier. could doublecheck with a query here
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
        INFO(f"Created {i+1} submission chunk(s) in {submission_dir} for {len(rule_matches)} jobs.")
    
    prettyfs=pprint.pformat(rule.job_config.filesystem)
    input_stem=inputs_from_output[rule.dsttype]
    if isinstance(input_stem, list):
        prettyfs=prettyfs.replace('{leafdir}',rule.dsttype)
    INFO(f"Other location templates:\n{prettyfs}")

    if args.andgo and not args.dryrun:
        sub_files = list(Path(submission_dir).glob(f'{subbase}*.sub'))
        for sub_file in sub_files:
            INFO(f"Submitting {sub_file}")
            subprocess.run(f"condor_submit {sub_file}",shell=True)
    
    if args.profile:
        profiler.disable()
        DEBUG("Profiling finished. Printing stats...")
        stats = pstats.Stats(profiler)
        stats.strip_dirs().sort_stats('time').print_stats(10)

    INFO( "KTHXBYE!" )

# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)
