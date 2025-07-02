#!/usr/bin/env python

import pyodbc
from pathlib import Path
from datetime import datetime  # noqa: F401
import yaml
import cProfile
import subprocess
import sys
import shutil

# from dataclasses import fields
import pprint # noqa F401

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig,list_to_condition
from sphenixprodrules import parse_lfn
from sphenixdbutils import test_mode as dbutils_test_mode
from sphenixdbutils import cnxn_string_map
from sphenixmisc import remove_empty_directories, binary_contains_bisect, make_chunks

# ============================================================================================
def delQuery( cnxn_string, query ):
    if 'delete' not in query:
        WARN(f'delQuery called without "delete". Query: {query}')

    DEBUG(f'[cnxn_string] {cnxn_string}')
    DEBUG(f'[query      ]\n{query}')
    conn = pyodbc.connect( cnxn_string )
    curs = conn.cursor()
    curs.execute( query )
    curs.commit()
    return(curs.rowcount)

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
            print("Exiting. Smart.")
            exit(0)
        else:
            print("Here we go deleting then.")

    #################### Test mode?
    test_mode = (
            dbutils_test_mode
            or args.test_mode
            # or ( hasattr(rule, 'test_mode') and rule.test_mode ) ## allow in the yaml file?
        )

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
    rule_substitutions = {}
    rule_substitutions["runs"]=args.runs
    rule_substitutions["runlist"]=args.runlist
    rule_substitutions["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor
        
    # Rest of the input substitutions
    if args.physicsmode is not None:
        rule_substitutions["physicsmode"] = args.physicsmode # e.g. physics

    if args.mangle_dstname:
        DEBUG("Mangling DST name")
        rule_substitutions['DST']=args.mangle_dstname

    # filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    rule_substitutions["prodmode"] = "production"
    if args.mangle_dirpath:
        rule_substitutions["prodmode"] = args.mangle_dirpath

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
    
    ### Which find command to use for lustre?
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
    condor_batchname=rule.job_config.batch_name
    # This is not necessary, just information
    condor_running_command=f"condor_q -const 'JobBatchName==\"{condor_batchname}\"' -format '%d\\n'  ClusterId |wc -l"
    condor_running=0
    try:
        condor_running = subprocess.run(condor_running_command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').split()[0]
        DEBUG("Command successful!")
    except subprocess.CalledProcessError as e:
        print("Command failed with exit code:", e.returncode)
    finally:
        pass

    WARN(f"About to kill {condor_running} condor jobs for JobBatchName==\"{condor_batchname}\"" )
    condor_rm_command=f"condor_rm -long -const 'JobBatchName==\"{condor_batchname}\"' | grep -c ^job_"
    WARN(f"{condor_rm_command}")
    if not args.dryrun:
        condor_rm='0'
        try:
            condor_rm = subprocess.run(condor_rm_command, shell=True, check=True, capture_output=True).stdout.decode('utf-8')
            DEBUG("Command successful!")
        except subprocess.CalledProcessError as e:
            print("Command failed with exit code:", e.returncode)
        finally:
            pass
        WARN(f"Killed {condor_rm} jobs using {condor_rm_command}" )

    ### Submission directory. Hacky.
    submission_dir = Path('./tosubmit').resolve() 
    subbase = f'{rule.rulestem}_{rule.outstub}_{rule.outdataset}'
    INFO(f'Submission files based on {subbase}')
    existing_sub_files =  list(Path(submission_dir).glob(f'{subbase}*.in'))
    existing_sub_files += list(Path(submission_dir).glob(f'{subbase}*.sub'))
    if existing_sub_files:
        WARN(f"Removing {int(len(existing_sub_files)/2)} existing submission file pairs for base: {subbase}")
        for f_to_delete in existing_sub_files: 
            CHATTY(f"Deleting: {f_to_delete}")
            if not args.dryrun:
                Path(f_to_delete).unlink(missing_ok=True)
    if Path(submission_dir).is_dir() and not any(Path(submission_dir).iterdir()):
        WARN(f"Submission directory is empty. Removing {submission_dir}")
        if not args.dryrun:
            Path(submission_dir).rmdir()

    ############# DSTs still in the lake
    filesystem = rule.job_config.filesystem
    DEBUG(f"Filesystem: {filesystem}")
    dstbase = f'{rule.rulestem}\*{rule.outstub}_{rule.outdataset}\*'
    INFO(f'DST files filtered as {dstbase}')

    lakelocation=filesystem['outdir']
    INFO(f"Original output directory: {lakelocation}")
    findcommand = f"{lfind} {lakelocation} -type f -name {dstbase}\*.root\*"
    findcommand=findcommand.replace('\*\*','\*') # cleanup eventual double asterisks
    INFO(f"Find command: {findcommand}")
    lakefiles=[]
    if Path(lakelocation).is_dir():
        lakefiles = subprocess.run(findcommand, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
    print(f"Found {len(lakefiles)} matching dsts sans runnumber cut in the lake.")
    del_lakefiles=[]
    for f_to_delete in lakefiles:
        lfn=Path(f_to_delete).name
        _,run,_,_=parse_lfn(lfn,rule)
        if binary_contains_bisect(rule.runlist_int,run):
            del_lakefiles.append(f_to_delete)
    WARN(f"Removing {len(del_lakefiles)} .root {lakelocation}")
    for f_to_delete in del_lakefiles: 
        CHATTY(f"Deleting: {f_to_delete}")
        if not args.dryrun:
            Path(f_to_delete).unlink(missing_ok=True) # could unlink the entire directory instead?

    findcommand = f"{lfind} {lakelocation} -type f -name {dstbase}\*.finished\*"
    findcommand=findcommand.replace('\*\*','\*') # cleanup eventual double asterisks
    INFO(f"Find command: {findcommand}")
    ## DEBUG
    finishedlakefiles = []
    if Path(lakelocation).is_dir():
        finishedlakefiles = subprocess.run(findcommand, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
    print(f"Found {len(finishedlakefiles)} matching .finished files in the lake.")
            
    del_lakefiles=[]
    for f_to_delete in finishedlakefiles:
        lfn=Path(f_to_delete).name
        _,run,_,_=parse_lfn(lfn,rule)
        if binary_contains_bisect(rule.runlist_int,run):
            del_lakefiles.append(f_to_delete)
    WARN(f"Removing {len(del_lakefiles)} .finished files in the lake at {lakelocation}")
    for f_to_delete in del_lakefiles: 
        CHATTY(f"Deleting: {f_to_delete}")
        if not args.dryrun:
            Path(f_to_delete).unlink(missing_ok=True) # could unlink the entire directory instead?

    # Clean up directories
    if Path(lakelocation).is_dir() and not any(Path(lakelocation).iterdir()):
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
        exit(-1)
    final_dsts_command=f"{lfind} {finaldir_glob} -type f -name {dstbase}\*"
    INFO(f"Find command for moved DSTs: {final_dsts_command}")
    all_final_dsts =[]
    try:
        all_final_dsts = subprocess.run(final_dsts_command, shell=True, check=True, capture_output=True)
        all_final_dsts = all_final_dsts.stdout.decode('utf-8').splitlines()
        DEBUG("Command successful! len(all_final_dsts)={len(all_final_dsts)}")
    except subprocess.CalledProcessError as e:
        print("Command failed with exit code:", e.returncode)
    finally:
        pass
    
    del_final_dsts = []
    for dst in all_final_dsts:
        lfn=Path(dst).name
        _,run,_,_=parse_lfn(lfn,rule)
        if binary_contains_bisect(rule.runlist_int,run):
            del_final_dsts.append(dst)
    WARN(f"Removing {len(del_final_dsts)} of the {len(all_final_dsts)} DSTs found by:\n{final_dsts_command}")
    for f_to_delete in del_final_dsts:
        CHATTY(f"Deleting: {f_to_delete}")
        if not args.dryrun:
            Path(f_to_delete).unlink(missing_ok=True)

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
            response = delQuery( cnxn_string_map[ dbstring ], del_files_db )
            DEBUG(f"Delete chunk {i} from files db, response: {response}")
            response = delQuery( cnxn_string_map[ dbstring ], del_datasets_db )
            DEBUG(f"Delete chunk {i} from datasets db, response: {response}")
            
    ### Clean up empty directories on lustre
    # With lfs find on lustre, "-empty" doesn't work. Rely on the cleaner to check that
    # Very generous find, but we're only cleaning up empties after all
    final_dirs_command=f"{lfind} {finaldir_glob} -type d"
    INFO(f"Find command: {final_dirs_command}")
    
    all_final_dirs =[]
    try:
        all_final_dirs = subprocess.run(final_dirs_command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
        DEBUG("Command successful!")
    except subprocess.CalledProcessError as e:
        # If the spider never ran, the directories may not exist
        print("Command failed with exit code:", e.returncode)
    finally:
        pass

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
        exit(-1)

    final_data_command=f"find {datadir_glob} -type f -name {dstbase}\*.out -o -name {dstbase}\*.err -o -name {dstbase}\*.condor -o -name HIST_{dstbase}\*.root"
    INFO(final_data_command)
    all_final_data=[]
    try:
        all_final_data = subprocess.run(final_data_command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()    
        DEBUG("Command successful!")
    except subprocess.CalledProcessError as e:
        print("Command failed with exit code:", e.returncode)
    finally:
        pass
    WARN(f"Found {len(all_final_data)} histogram and log files.")
    del_final_data = []
    for data in all_final_data:
        lfn=Path(data).name
        _,run,seg,end=parse_lfn(lfn,rule)
        if binary_contains_bisect(rule.runlist_int,run):
            del_final_data.append(data)
            
    WARN(f"Removing {len(del_final_data)} of the {len(all_final_data)} log and histo files found by:\n{final_data_command}")
    for f_to_delete in del_final_data:
        CHATTY(f"Deleting: {f_to_delete}")
        if not args.dryrun:
            Path(f_to_delete).unlink(missing_ok=True)
    
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
            response = delQuery( cnxn_string_map[ dbstring ], del_files_db )
            DEBUG(f"Delete chunk {i} from files db, response: {response}")
            response = delQuery( cnxn_string_map[ dbstring ], del_datasets_db )
            DEBUG(f"Delete chunk {i} from datasets db, response: {response}")

    ### Clean up empty directories on /sphenix/data/data02
    datatrunk=datadir_glob.replace("/*","")
    datatrunk=f"{datatrunk}/{rule.rulestem}*"
    data_dirs_find=f"find {datatrunk} -type d -empty"
    empty_data_dirs=[]
    try:
        empty_data_dirs = subprocess.run(data_dirs_find, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
        DEBUG("Command successful!")
    except subprocess.CalledProcessError as e:
        print("Command failed with exit code:", e.returncode)
    finally:
        pass
    INFO(f"{len(empty_data_dirs)} empty leaf directories found with {data_dirs_find}. Removing.")
    if not args.dryrun:
        remove_empty_directories( set(empty_data_dirs) )

    sqldstbase=dstbase.replace("\*","%")
    prodrun_condition=list_to_condition(rule.runlist_int,"run")
    ### Finally, clean the production database
    del_prod_state = f"""
delete from production_status 
where 
dstname like '{sqldstbase}'
and {prodrun_condition}
returning *
"""
    WARN(del_prod_state+";")
    if not args.dryrun:
        response = delQuery( cnxn_string_map[ "statw" ], del_prod_state )
        DEBUG(f"Delete states from prod db, response: {response}")

    INFO("Done.")
    if not args.dryrun:
        WARN("You should run this again in a minute or two, to catch files still trickling in from killed jobs.")
    exit(0)
        
# ============================================================================================

if __name__ == '__main__':
    # main()
    # exit(0)

    cProfile.run('main()', '/tmp/sphenixprod.prof')
    import pstats
    p = pstats.Stats('/tmp/sphenixprod.prof')
    p.strip_dirs().sort_stats('time').print_stats(10)

