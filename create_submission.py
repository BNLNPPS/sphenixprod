#!/usr/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import pstats
import subprocess
import re
import os
import sys
import itertools

import pprint # noqa F401
if os.uname().sysname!='Darwin' :
    import htcondor # type: ignore

import argparse
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
from execute_condorsubmission import locate_submitfiles,execute_submission

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

    if args.force:
        ERROR('Got "--force": That doesn\'t work yet. Sorry.')
        exit(1)
        #### For --force, we could do the file and database deletion in RuleConfig.
        # Would be kinda nice because only then we'll know what's _really_ affected, and we could use the logic there.
        # Instead, ensure that the rule logic needs no special cases, set everything up here.
        WARN('Got "--force": Override existing output in files, datasets, and production_status DBs. Delete those files.')
        WARN('               Note that it\'s YOUR job to ensure there\'s no job in the queue or file in the DST lake which will overwrite this later!')
        answer = input("Do you want to continue? (yes/no): ")
        if answer.lower() != "yes":
            print("Exiting. Smart.")
            exit(0)
        WARN("Here we go then.")

    # Exit without fuss if we are already running 
    if should_I_quit(args=args, myname=sys.argv[0]) and not args.force:
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

    # Files to copy to the worker - can be added to later by yaml and args
    payload_list=[]

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

    script_path = Path(__file__).parent.resolve()
    payload_list += [ f"{script_path}/stageout.sh" ]
    payload_list += [ f"{script_path}/GetNumbers.C" ]
    payload_list += [ f"{script_path}/common_runscript_prep.sh" ]
    payload_list += [ f"{script_path}/create_filelist_run_daqhost.py" ]
    payload_list += [ f"{script_path}/create_filelist_run_seg.py" ]
    payload_list += [ f"{script_path}/create_full_filelist_run_seg.py" ]
    
    # .testbed: indicate test mode -- Search in the _submission_ directory
    if Path(".testbed").exists():
        payload_list += [str(Path('.testbed').resolve())]

    # from command line - the order means these can overwrite the default files from above
    if args.append2rsync:
        payload_list.insert(args.append2rsync)
    DEBUG(f"Addtional resources to be copied to the worker: {payload_list}")

    ### Parse command line arguments into a substitution dictionary
    # This dictionary is passed to the ctor to override/customize yaml file parameters
    param_overrides = {}
    param_overrides["script_path"]       = script_path
    param_overrides["payload_list"]      = payload_list
    param_overrides["runs"]              = args.runs
    param_overrides["runlist"]           = args.runlist
    param_overrides["nevents"]           = args.nevents
    param_overrides["combine_seg0_only"] = args.onlyseg0  # "None" if not explicitly given, to allow precedence of the yaml in that case    
    param_overrides["prodmode"]          = "production"
    # For testing, "production" (close to the root of all paths) in the default filesystem) can be replaced
    if args.mangle_dirpath:
        param_overrides["prodmode"] = args.mangle_dirpath

    # Rest of the input substitutions
    if args.physicsmode is not None:
        param_overrides["physicsmode"] = args.physicsmode # e.g. physics

    if args.mem:
        DEBUG(f"Setting memory to {args.mem}")
        param_overrides['request_memory']=args.mem

    if args.priority:
        DEBUG(f"Setting priority to {args.priority}")
        param_overrides['priority']=args.priority

    CHATTY(f"Rule substitutions: {param_overrides}")
    INFO("Now loading and building rule configuration.")

    #################### Load specific rule from the given yaml file.
    try:
        rule =  RuleConfig.from_yaml_file( yaml_file=args.config,
                                           rule_name=args.rulename,
                                           param_overrides=param_overrides )
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
    
    submitdir = Path(f'{args.submitdir}').resolve()
    if not args.dryrun:
        Path( submitdir).mkdir( parents=True, exist_ok=True )
    subbase = f'{rule.dsttype}_{rule.dataset}_{rule.outtriplet}'
    INFO(f'Submission files based on {subbase}')

    # For a fairly collision-safe identifier that could be used to not trample on existing files:
    # import nanoid
    # short_id = nanoid.generate(size=6)
    # print(f"Short ID: {short_id}")

    # Header for all submission files
    CondorJob.job_config = rule.job_config
    base_job = htcondor.Submit(CondorJob.job_config.condor_dict())

    ## Instead of same-size chunks, group submission files by runnumber
    matchlist=list(rule_matches.items())
    ## Brittle! Assumes value[key][3] == runnumber
    keyfunc = lambda item: item[1][3]  # x[0] is outfilename, x[1] is tuple, 4th field is runnumber
    matchlist=sorted(matchlist, key=keyfunc)
    matches_by_run = {k : list(g) for k, g in itertools.groupby(matchlist,key=keyfunc)}
    submittable_runs=list(matches_by_run.keys())
    # Newest first
    submittable_runs=sorted(submittable_runs, reverse=True)

    INFO(f"Creating submission for {len(submittable_runs)} runs")
    ### Limit number of job files lying around
    max_jobs=20000 
    # Count up what we already have
    existing_jobs=0
    sub_files=locate_submitfiles(rule,args)
    for sub_file in sub_files:
        in_file=re.sub(r".sub$",".in",str(sub_file))
        if not Path(in_file).is_file():
            continue
        with open(in_file,'r') as f:
            existing_jobs += len(f.readlines())
            
    if existing_jobs>0:
        INFO(f"We already have {existing_jobs} jobs waiting for submission.")

    # Update existing or produce more submission files up to the given limit
    for submit_run in submittable_runs:
        if existing_jobs>max_jobs:
            break
        matches=matches_by_run[submit_run]
        INFO(f"Creating {len(matches)} submission files for run {submit_run}.")
        existing_jobs+=len(matches)
        INFO(f"Total jobs waiting for submission: {existing_jobs}")

        condor_subfile=f'{submitdir}/{subbase}_{submit_run}.sub'
        condor_infile =f'{submitdir}/{subbase}_{submit_run}.in'
        if not args.dryrun:
            # (Re-) create the "header" - common job parameters
            Path(condor_subfile).unlink(missing_ok=True) 
            with open(condor_subfile, "w") as f:
                f.write(str(base_job))
                f.write(
f"""
log = $(log)
output = $(output)
error = $(error)
arguments = $(arguments)
queue log,output,error,arguments from {condor_infile}
""")

        # individual lines per job
        prod_state_rows=[]
        condor_rows=[]
        for out_file,(in_files, outbase, logbase, run, seg, daqhost, dsttype) in matches:            
            # Create .in file row
            condor_job = CondorJob.make_job( output_file=out_file, 
                                             inputs=in_files,
                                             outbase=outbase,
                                             logbase=logbase,
                                             leafdir=dsttype,
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
            # if 'TRIGGERED_EVENT' in dsttype or 'STREAMING_EVENT' in dsttype: # TODO: FIXME for those as well
            #     dstfile=f'{outbase}-{run:{pRUNFMT}}-{0:{pSEGFMT}}'
            # else:
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
            # end of collecting job lines for this run

        comma_prod_state_rows=',\n'.join(prod_state_rows)
        insert_prod_state = f"""
insert into production_status
( dsttype, dstname, dstfile, run, segment, nsegments, inputs, prod_id, cluster, process, status, submitting, submission_host )
values 
{comma_prod_state_rows}
returning id
""" 
        # Commit "submitting" to db
        if not args.dryrun:
            # Register in the db, hand the ids the condor job (for faster db access; usually passed through to head node daemons)
            prod_curs = dbQuery( cnxn_string_map['statw'], insert_prod_state )
            prod_curs.commit()
            ids=[str(id) for (id,) in prod_curs.fetchall()]
            CHATTY(f"Inserted {len(ids)} rows into production_status, IDs: {ids}")
            condor_rows=[ f"{x} {y}" for x,y in list(zip(condor_rows, ids))]
 
        # Write or update job line file
        if not args.dryrun:
            with open(condor_infile, "a") as f:
                f.writelines(row+'\n' for row in condor_rows)

    ### And submit, if so desired
    if args.andgo: 
        execute_submission(rule, args)
        
    if args.profile:
        profiler.disable()
        DEBUG("Profiling finished. Printing stats...")
        stats = pstats.Stats(profiler)
        stats.strip_dirs().sort_stats('time').print_stats(10)

    prettyfs=pprint.pformat(rule.job_config.filesystem)
    input_stem=inputs_from_output[rule.dsttype]
    if isinstance(input_stem, list):
        prettyfs=prettyfs.replace('{leafdir}',rule.dsttype)

    INFO(f"Submission directory is {submitdir}")    
    INFO(f"Other location templates:\n{prettyfs}")
    INFO( "KTHXBYE!" )

# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)
