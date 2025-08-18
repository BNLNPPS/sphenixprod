#!/usr/bin/env python

from pathlib import Path

import pprint # noqa F401

from sphenixmisc import shell_command
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixmatching import MatchConfig
from sphenixmisc import read_batches,lock_file,unlock_file

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

    ### 1. Delete output
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
        print(f"Processing batch {i}, length is {len(batch)} lines.")
        if not dryrun:
            for rootfile in batch:
                # Path(rootfile).unlink(missing_ok=True)
                pass

    if not dryrun:
        unlock_file(file_path=dstlistname,dryrun=dryrun)
        #Path(dstlistname).unlink(missing_ok=True)
    
    exit()

    ### 2. Select lfns.
    existing_lfns=match_config.get_files_in_db(runlist)
    print(existing_lfns)


    
    exit()
    

