#!/usr/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import subprocess
import sys
import shutil
import math
# from contextlib import nullcontext

# from dataclasses import fields
import pprint # noqa F401

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig,list_to_condition, extract_numbers_to_commastring, inputs_from_output
from sphenixdbutils import test_mode as dbutils_test_mode
from sphenixdbutils import cnxn_string_map, dbQuery
from sphenixdbutils import insert_files_tmpl, insert_datasets_tmpl
from sphenixmisc import remove_empty_directories, binary_contains_bisect, make_chunks

# ============================================================================================

def main():
    """Instantiate a given rule and remove all logfiles, histograms, dsts, database entries etc.
    TODO: The first hundred or so lines are shared with protospider and create_submission.
    Refactor when there's downtime.
""" 

    ### digest arguments
    args = submission_args()

    # This is a dangerous operation. Make sure the user means it.
    if not args.dryrun:
        answer = input("This is not a drill. Do you want to continue? (yes/no): ")
        if answer.lower() != "yes":
            print("Exiting...")
            exit(0)
        else:
            print("Here we go deleting then.")

    #################### Test mode?
    test_mode = (
            dbutils_test_mode
            or args.test_mode
            # or ( hasattr(rule, 'test_mode') and rule.test_mode ) ## allow in the yaml file?
        )
    # if test_mode:
    #     Path('.testbed').touch()

    # Set up submission logging before going any further
    sublogdir=setup_rot_handler(args)
    slogger.setLevel(args.loglevel)
    
    # Exit without fuss if we are already running 
    if should_I_quit(args=args, myname=sys.argv[0]):
        DEBUG("Stop.")
        exit(0)
    
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
    rule_substitions["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor

    # Rest of the input substitutions
    if args.physicsmode is not None:
        rule_substitions["physicsmode"] = args.physicsmode # e.g. physics

    if args.mangle_dstname:
        DEBUG("Mangling DST name")
        rule_substitions['DST']=args.mangle_dstname

    # filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    rule_substitions["prodmode"] = "production"
    if args.mangle_dirpath:
        rule_substitions["prodmode"] = args.mangle_dirpath

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

    # Which find command to use?
    # Lustre's robin hood, rbh-find, doesn't offer advantages for our usecase, and it is more cumbersome to use.
    # But "lfs find" is preferrable to the regular kind.
    lfind = shutil.which('lfs')
    if lfind is None:
        WARN("'lfs find' not found.")
        lfind = shutil.which('find')
    else:
        lfind = f'{lfind} find'
    INFO(f'Using "{lfind}.')
    
    ######## Now clean up
    ### Condor jobs:
    # condor_q -const 'JobBatchName=="kolja.DST_STREAMING_EVENT_run3physics_new_nocdbtag_v000"' -format "%d\n"  ClusterId |wc -l
    condor_batchname=rule.job_config.batch_name
    # This is not necessary, just information
    condor_running_command=f"condor_q -const 'JobBatchName==\"{condor_batchname}\"' -format '%d\\n'  ClusterId |wc -l"
    condor_running = subprocess.run(condor_running_command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').split()[0]
    WARN(f"About to kill {condor_running} condor jobs for JobBatchName==\"{condor_batchname}\"" )
    if not args.dryrun:
        condor_rm_command=f"condor_rm -long -const 'JobBatchName==\"{condor_batchname}\"' | grep -c ^job_"
        condor_rm = subprocess.run(condor_rm_command, shell=True, check=True, capture_output=True).stdout.decode('utf-8')
        WARN(f"Killed {condor_rm_command} jobs using {condor_rm_command}" )

    filesystem = rule.job_config.filesystem
    DEBUG(f"Filesystem: {filesystem}")

    ### Which runs to delete?
    runlist=[-1]
    if args.runlist:
        INFO(f"Processing runs from file: {args.runlist}")
        _, runlist = extract_numbers_to_commastring(args.runlist)
    elif args.runs:
        if args.runs==['-1'] : 
            INFO(f"Processing all runs.")
        else:
            INFO(f"Processing run (range): {args.runs}")
        _, runlist = list_to_condition(args.runs, "runnumber")
    else:
        ERROR("Something's wrong. No runs provided, but this should have been caught by the \"runs\" default value in argparsing.")
        exit(-1)        

    ### Submission directory. Hacky.
    submission_dir = Path('./tosubmit').resolve() 
    subbase = f'{rule.rulestem}_{rule.outstub}_{rule.dataset}'
    INFO(f'Submission files based on {subbase}')
    existing_sub_files =  list(Path(submission_dir).glob(f'{subbase}*.in'))
    existing_sub_files += list(Path(submission_dir).glob(f'{subbase}*.sub'))
    if existing_sub_files:
        WARN(f"Removing {int(len(existing_sub_files)/2)} existing submission file pairs for base: {subbase}")
        for f_to_delete in existing_sub_files: 
            CHATTY(f"Deleting: {f_to_delete}")
            if not args.dryrun:
                Path(f_to_delete).unlink()
    if Path(submission_dir).is_dir() and not any(Path(submission_dir).iterdir()):
        WARN(f"Submission directory is empty. Removing {submission_dir}")
        if not args.dryrun:
            Path(submission_dir).rmdir()

    ### DSTs still in the lake
    lakelocation=filesystem['outdir']
    INFO(f"Original output directory: {lakelocation}")
    findcommand = f"{lfind} {lakelocation} -type f -name {rule.rulestem}\*"
    # INFO(f"Find command: {findcommand}")
    lakefiles = subprocess.run(findcommand, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
    del_lakefiles=[]
    for f_to_delete in lakefiles:
        lfn=Path(f_to_delete).name
        run = int(lfn.split(rule.dataset)[1].split('-')[1])
        if runlist==[-1] or binary_contains_bisect(runlist,run):
            del_lakefiles.append(f_to_delete)        
    WARN(f"Removing {len(del_lakefiles)} DSTs of the form {rule.rulestem} in the lake at {lakelocation}")
    
    for f_to_delete in lakefiles: 
        CHATTY(f"Deleting: {f_to_delete}")
        if not args.dryrun:
            Path(f_to_delete).unlink() # could unlink the entire directory instead?
    if not any(Path(lakelocation).iterdir()):
        WARN(f"DST lake is empty. Removing {lakelocation}")
        if not args.dryrun:
            Path(lakelocation).rmdir()            

    ### DSTS, final destination
    finaldir_tmpl=filesystem['finaldir']
    INFO(f"Final destination template: {finaldir_tmpl}")

    ## Complicated way: Extract hostname == leaf, construct directory and dst names
    # # Extract information encoded in the file name
    # input_stem = inputs_from_output[rule.rulestem]
    # INFO(f"Input stem: {input_stem}")
    # outstub = rule.outstub
    # INFO(f"Output stub: {outstub}")
    # dst_type_template = f'{rule.rulestem}'
    # if 'raw' in rule.input_config.db:
    #     dst_type_template += '_{host}'
    # dst_types = { f'{dst_type_template}'.format(host=host) for host in input_stem.keys() }    
    # INFO(f"Destination type template: {dst_type_template}")
    # INFO(f"Destination types: {dst_types}")
    # ...
    
    ## Simple way: Replace all placeholders in the final destination template with '*'
    ## Search by filename
    try:
        finaldir_glob = finaldir_tmpl.format(leafdir='*',rungroup='*')
    except Exception as e:
        ERROR(f"Trying to globify {finaldir_tmpl} failed. Error:\n{e}")
        exit()
    final_dsts_command=f"{lfind} {finaldir_glob} -type f -name {rule.rulestem}\*"
    all_final_dsts = subprocess.run(final_dsts_command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
    del_final_dsts = []
    for dst in all_final_dsts:
        # Extract runnumber from the file name
        # Logic: first split is at new_nocdbtag_v000, second split isolates the run number, which is between two dashes
        lfn=Path(dst).name
        # dsttype=lfn.split(f'_{rule.dataset}')[0]
        run = int(lfn.split(rule.dataset)[1].split('-')[1])
        # rungroup=rule.job_config.rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
        
        if runlist==[-1] or binary_contains_bisect(runlist,run):
            del_final_dsts.append(dst)
    WARN(f"Removing {len(del_final_dsts)} of the {len(all_final_dsts)} DSTs found by:\n{final_dsts_command}")
    for f_to_delete in del_final_dsts:
        CHATTY(f"Deleting: {f_to_delete}")
        if not args.dryrun:
            Path(f_to_delete).unlink()

    ### Update databases accordingly
    ## Note: We are only using the actually deleted filenames.    
    ## It would be more thorough to do it by a more general rule, but that's complicated b/c you have to dissect lfn
    chunk_size = 500
    chunked_dsts = list(make_chunks(del_final_dsts, chunk_size))
    dbstring = 'testw' if test_mode else 'fcw'
    files_table='test_files' if test_mode else 'files'
    datasets_table='test_datasets' if test_mode else 'datasets'
    WARN(f"Deleting {len(del_final_dsts)} DST rows from table {files_table} and from table {datasets_table}")

    ## files
    del_files_tmpl="""
    select count(lfn) from {files_table} 
    where lfn in ({lfns})
    """
    ## datasets
    del_datasets_tmpl="""
    select count(filename) from {datasets_table} 
    where filename in ({lfns})
    """
    if not args.dryrun:
        del_files_tmpl=del_files_tmpl.replace("select count(lfn) from", "delete from")
        del_datasets_tmpl=del_datasets_tmpl.replace("select count(filename) from", "delete from") 

    for i, chunk in enumerate(chunked_dsts):
        lfns=[]
        for dst in chunk:
            lfns.append(f"'{Path(dst).name}'")
        del_files_db=del_files_tmpl.format(
            files_table=files_table,
            lfns=",".join(lfns)
        )
        del_datasets_db=del_datasets_tmpl.format(
            datasets_table=datasets_table,
            lfns=",".join(lfns)
        )
        CHATTY(del_files_db )
        CHATTY(del_datasets_db )
        if not args.dryrun:
            files_curs = dbQuery( cnxn_string_map[ dbstring ], del_files_db )
            response = [ c for c in files_curs ]
            DEBUG(f"Delete chunk {i} from files db, response: {response[0]}")
            datasets_curs = dbQuery( cnxn_string_map[ dbstring ], del_datasets_db )
            response = [ c for c in datasets_curs ]
            DEBUG(f"Delete chunk {i} from datasets db, response: {response[0]}")
            
    ### Clean up empty directories on lustre
    finaldir_trunk=finaldir_glob.replace('/*',"")
    finaldir_trunk=f"{finaldir_trunk}/{rule.rulestem}*"
    # With lfs find on lustre, "-empty" doesn't work. Rely on the cleaner to check that
    final_dirs_command=f"{lfind} {finaldir_trunk} -type d"
    all_final_dirs = subprocess.run(final_dirs_command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
    INFO(f"{final_dirs_command} found {len(all_final_dirs)} directories. Removing the empty ones.")
    if not args.dryrun:
        remove_empty_directories( set(all_final_dirs) )

    # More surgical, less flexible
    # for del_dir in all_final_dirs: # Only one level deep!
    #     for daughter in Path(del_dir).iterdir():            
    #         if Path(daughter).is_dir() and not any(Path(daughter).iterdir()):
    #             DEBUG(f"Deleting {daughter}")
    #             Path(daughter).rmdir()
    #     if not any(Path(del_dir).iterdir()):
    #         DEBUG(f"Deleting {del_dir}")
    #         Path(del_dir).rmdir()

    ######### Take care of out, err, log, hist
    ## Simple way: Replace all placeholders in the destination template with '*'
    ## Lazy assumption: they all live in the same place. Check that.
    ## Note: The fact that Path(...).parent works for a path with placeholders is a bit weird but it does.
    datadir_tmpl=Path(filesystem['histdir']).parent
    if datadir_tmpl!=Path(filesystem['logdir']).parent or datadir_tmpl!=Path(filesystem['condor']).parent:
        ERROR("Assumption that the root of histdir, logdir, condor is the same failed.")
        print(f"histdir: {filesystem['histdir']}")
        print(f"logdir:  {filesystem['logdir']}")
        print(f"condor:  {filesystem['condor']}")

    try:
        datadir_glob = str(datadir_tmpl).format(leafdir='*',rungroup='*')
    except Exception as e:
        ERROR(f"Trying to globify {datadir_glob} failed. Error:\n{e}")
        exit()

    final_data_command=f"find {datadir_glob} -type f -name {rule.rulestem}\*.out -o -name {rule.rulestem}\*.err -o -name {rule.rulestem}\*.condor -o -name HIST_{rule.rulestem}\*.root"
    INFO(final_data_command)
    all_final_data = subprocess.run(final_data_command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
    WARN(f"Found {len(all_final_data)} histogram and log files.")
    del_final_data = []
    for data in all_final_data:
        # Extract runnumber from the file name
        # Logic: first split is at new_nocdbtag_v000, second split isolates the run number, which is between two dashes
        lfn=Path(data).name
        # Sigh. Two patterns: "foo_v000-run-segment.root, and "bar_v000_run.[out|err|condor]. 
        try: 
            if lfn.endswith(".root"):
                run = int(lfn.split(rule.dataset)[1].split('-')[1])
            elif lfn.endswith(".out") or lfn.endswith(".err") or lfn.endswith(".condor") :
                tmp = lfn.split(".")[0]
                run=int(tmp.split("_")[-1])
            else:
                ERROR(f"Unrecognized data file {lfn}.")
                exit(-1)                
        except Exception as e:
            print(lfn)
            print(run)
            print(e)
            exit(-1)
        if runlist==[-1] or binary_contains_bisect(runlist,run):
            del_final_data.append(data)
            
    WARN(f"Removing {len(del_final_data)} of the {len(all_final_data)} log and histo files found by:\n{final_data_command}")
    for f_to_delete in del_final_data:
        CHATTY(f"Deleting: {f_to_delete}")
        if not args.dryrun:
            Path(f_to_delete).unlink()
    
    # And remove them from databases
    histnumber= sum('HIST_' in s for s in del_final_data)
    WARN(f"Deleting {histnumber} histogram files from rows from table {files_table} and from table {datasets_table}. Also deleting the other data files if they somehow made it in.")
    chunked_data = list(make_chunks(del_final_data, chunk_size))
    for i, chunk in enumerate(chunked_data):
        lfns=[]
        for data in chunk:
            lfns.append(f"'{Path(data).name}'")
        del_files_db=del_files_tmpl.format(
            files_table=files_table,
            lfns=",".join(lfns)
        )
        del_datasets_db=del_datasets_tmpl.format(
            datasets_table=datasets_table,
            lfns=",".join(lfns)
        )
        CHATTY(del_files_db )
        CHATTY(del_datasets_db )
        if not args.dryrun:
            files_curs = dbQuery( cnxn_string_map[ dbstring ], del_files_db )
            response = [ c for c in files_curs ]
            DEBUG(f"Delete chunk {i} from files db, response: {response[0]}")
            datasets_curs = dbQuery( cnxn_string_map[ dbstring ], del_datasets_db )
            response = [ c for c in datasets_curs ]
            DEBUG(f"Delete chunk {i} from datasets db, response: {response[0]}")

    ### Clean up empty directories on /sphenix/data/data02
    datatrunk=datadir_glob.replace("/*","")
    datatrunk=f"{datatrunk}/{rule.rulestem}*"
    data_dirs_find=f"find {datatrunk} -type d -empty"
    empty_data_dirs = subprocess.run(data_dirs_find, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
    INFO(f"{len(empty_data_dirs)} empty leaf directories found with {data_dirs_find}. Removing.")
    if not args.dryrun:
        remove_empty_directories( set(empty_data_dirs) )
        
# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)

    cProfile.run('main()', '/tmp/sphenixprod.prof')
    import pstats
    p = pstats.Stats('/tmp/sphenixprod.prof')
    p.strip_dirs().sort_stats('time').print_stats(10)

