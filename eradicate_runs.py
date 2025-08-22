#!/usr/bin/env python

from pathlib import Path
import sys

import pprint # noqa F401

from argparsing import submission_args
from create_submission import dbutils_test_mode
from simpleLogger import slogger, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixmatching import MatchConfig
from sphenixmisc import setup_rot_handler, should_I_quit, make_chunks
from sphenixmisc import read_batches,lock_file, unlock_file
from sphenixmisc import shell_command
from sphenixdbutils import cnxn_string_map, dbQuery

def eradicate_runs(match_config: MatchConfig, dryrun: bool=True):
    """ Run this script to remove files and db entries
    so that the run can be resubmitted.
    (Ex.: With different resource specs)
    """
    # Note: Deletion of physical files is unconnected from the data base
    #       because we cannot be sure the two are consistent.
    
    # Unique identiers for what to process
    dataset=match_config.dataset
    outtriplet=match_config.outtriplet
    dsttype=match_config.dsttype
    runlist=match_config.runlist_int
    DEBUG(dataset)
    DEBUG(dsttype)
    DEBUG(outtriplet)
    filesystem=match_config.filesystem
    
    # existing_lfns=match_config.get_files_in_db(runlist)
    # exit()
    # # print(existing_lfns)

    ### 0. TODO: Identify and kill condor jobs. Basically impossible without also at least running on the submission node.

    ### 1. Delete output ON DISK
    dstlistname=filesystem['logdir']
    dstlistname=dstlistname.split("{")[0]
    while dstlistname.endswith("/"):
        dstlistname=dstlistname[0:-1]
    dstlistname=f"{dstlistname}/{dsttype}_deletelist"
    #dstlistname="/tmp/delmelist"
    #dstlistname=None
    if not lock_file(file_path=dstlistname, dryrun=dryrun, max_lock_age=30*60):
        ERROR( "Not safe to proceed without intervention.")
        exit(1)
    
    rootfiles=match_config.get_output_files(filemask="\*root\*",dstlistname=dstlistname,dryrun=dryrun)
    nfiles=0
    if not dstlistname:
        nfiles=len(rootfiles)
    else:
        if Path(dstlistname).exists():
            wccommand=f"wc -l {dstlistname}"
            ret = shell_command(wccommand)
            nfiles = int(ret[0])

    INFO(f"Found {nfiles} files to delete.")

    filebatches=[]
    if nfiles>0:
        filebatches=read_batches(dstlistname,100000) if dstlistname else (rootfiles,)
    for i,batch in enumerate(filebatches):
        INFO(f"Processing batch {i}, length is {len(batch)} lines.")
        if not dryrun:
            for rootfile in batch:
                # Path(rootfile).unlink(missing_ok=True)
                pass

    if not dryrun:
        unlock_file(file_path=dstlistname,dryrun=dryrun)
        #Path(dstlistname).unlink(missing_ok=True)

    ### 2. Select from production DB
    # We could do this together with the next step, for individual lfns.
    existing_status=match_config.get_prod_status(runlist)
    existing_status=list(existing_status.keys())
    INFO(f"Found {len(existing_status)} output files in the production db")

    ### 2a. Delete from production db.
    dbstring = 'statw'
    status_query="SELECT id" if dryrun else "DELETE"
    status_query+="""
    FROM production_status
        WHERE
    dstfile in
    """
    chunksize=5000
    statusmax=len(existing_status)
    for i,statuschunk in enumerate(make_chunks(existing_status,chunksize)):
        statuschunk_str="','".join(statuschunk)
        
        DEBUG( f'Removing file #{i*chunksize}/{statusmax} from database production_status')
        prod_curs = dbQuery( cnxn_string_map[ dbstring ], status_query+f"( '{statuschunk_str}' )" )
        if prod_curs:
            prod_curs.commit()
        else:
            ERROR("Failed to delete file(s) from production database")
            exit(1)
    
        
    ### 3. Select lfns in DB
    # This is necessary because the files db has no fields to select by runnumber etc.
    existing_lfns=match_config.get_files_in_db(runlist)
    INFO(f"Found {len(existing_lfns)} entries in the FileCatalog")

    ### 4. Delete from datasets and files
    dbstring = 'testw' if dbutils_test_mode else 'fcw'
    datasets_table='test_datasets' if dbutils_test_mode else 'datasets'
    datasets_query="SELECT" if dryrun else "DELETE"
    datasets_query+=f"""    
    FROM {datasets_table}
        WHERE
    filename in
    """

    files_table='test_files' if dbutils_test_mode else 'files'
    files_query="SELECT" if dryrun else "DELETE"
    files_query+=f"""    
    FROM {files_table}
        WHERE
    lfn in
    """
    
    chunksize=5000
    lfnmax=len(existing_lfns)
    for i,lfnchunk in enumerate(make_chunks(existing_lfns,chunksize)):
        DEBUG( f'File #{i*chunksize}/{lfnmax}' )
        lfnchunk_str="','".join(lfnchunk)
        
        DEBUG( f'Removing from database {files_table}')
        files_curs = dbQuery( cnxn_string_map[ dbstring ], files_query+f"( '{lfnchunk_str}' )" )
        if files_curs:
            files_curs.commit()
        else:
            ERROR("Failed to delete file(s) from files database")
            exit(1)
            
        DEBUG( f'Removing from database {datasets_table}' )
        datasets_curs = dbQuery( cnxn_string_map[ dbstring ], datasets_query+f"( '{lfnchunk_str}' )" )
        if datasets_curs:
            datasets_curs.commit()
        else:
            ERROR("Failed to delete file(s) from datasets database")
            exit(1)

    return

def main():
    args = submission_args()

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

    # Prepare param_overrides for RuleConfig
    param_overrides = {}
    param_overrides["runs"] = args.runs
    param_overrides["runlist"] = args.runlist
    param_overrides["nevents"] = 0 # Not relevant for eradication, but RuleConfig expects it.

    if args.physicsmode is not None:
        param_overrides["physicsmode"] = args.physicsmode

    # Load specific rule from the given yaml file.
    try:
        rule = RuleConfig.from_yaml_file(
            yaml_file=args.config,
            rule_name=args.rulename,
            param_overrides=param_overrides
        )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error loading rule configuration: {e}")
        exit(1)

    # Create a match configuration from the rule
    match_config = MatchConfig.from_rule_config(rule)
    INFO("Match configuration created.")

    # Call the main eradication function
    eradicate_runs(match_config, dryrun=args.dryrun)

    INFO(f"{Path(__file__).name} DONE.")

if __name__ == '__main__':
    main()
    exit(0)
