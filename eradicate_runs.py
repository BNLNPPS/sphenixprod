#!/usr/bin/env python

from pathlib import Path

import pprint # noqa F401

from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixmatching import MatchConfig
from sphenixmisc import read_batches,lock_file,unlock_file,make_chunks
from sphenixmisc import shell_command
from sphenixdbutils import test_mode as dbutils_test_mode
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
    status_query+=f"""
    FROM production_status
        WHERE
    dstfile in
    """
    chunksize=5000 
    statusmax=len(existing_status)
    for i,statuschunk in enumerate(make_chunks(existing_status,chunksize)):
        DEBUG( f'' )
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
