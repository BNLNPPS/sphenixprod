#!/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import pstats
import sys
import shutil
import os
import math

# from dataclasses import fields
import pprint # noqa F401

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit, make_chunks
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixmatching import MatchConfig, parse_lfn, parse_spiderstuff
from sphenixdbutils import test_mode as dbutils_test_mode
from sphenixdbutils import long_filedb_info, filedb_info, full_db_info, upsert_filecatalog, update_proddb  # noqa: F401
from sphenixmisc import binary_contains_bisect,shell_command,lock_file,unlock_file


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

    # Set up submission logging before going any further
    sublogdir=setup_rot_handler(args)
    slogger.setLevel(args.loglevel)

    # Exit without fuss if we are already running
    if should_I_quit(args=args, myname=sys.argv[0]):
        DEBUG("Stop.")
        exit(0)
    INFO(f"Logging to {sublogdir}, level {args.loglevel}")

    if args.profile:
        DEBUG( "Profiling is ENABLED.")
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
    param_overrides = {}
    param_overrides["runs"]=args.runs
    param_overrides["runlist"]=args.runlist
    param_overrides["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor

    # Rest of the input substitutions
    if args.physicsmode is not None:
        param_overrides["physicsmode"] = args.physicsmode # e.g. physics

    # filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    param_overrides["prodmode"] = "production"
    if args.mangle_dirpath:
        param_overrides["prodmode"] = args.mangle_dirpath

    CHATTY(f"Rule substitutions: {param_overrides}")
    INFO("Now loading and building rule configuration.")

    #################### Load specific rule from the given yaml file.
    try:
        rule = RuleConfig.from_yaml_file( yaml_file=args.config, rule_name=args.rulename, param_overrides=param_overrides )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)

    CHATTY("Rule configuration:")
    CHATTY(yaml.dump(rule.dict))
    filesystem = rule.job_config.filesystem

    # Create a match configuration from the rule
    match_config = MatchConfig.from_rule_config(rule)
    CHATTY("Match configuration:")
    CHATTY(yaml.dump(match_config.dict))

    ### Use or create a list file containing all the existing files to work on.
    ### This reduces memory footprint and repeated slow `find` commands for large amounts of files
    dstlistname=filesystem['logdir']
    dstlistname=dstlistname.split("{")[0]
    while dstlistname.endswith("/"):
        dstlistname=dstlistname[0:-1]
    dstlistname=f"{dstlistname}/{rule.dsttype}_dstlist"
    
    # First, lock. This way multiple spiders can work on a file without stepping on each others' (8) toes
    if not lock_file(dstlistname,args.dryrun):
        exit(0)

    INFO(f"Looking for existing filelist {dstlistname}")
    if Path(dstlistname).exists():
        INFO(f" ... found.")
    else:
        INFO(" ... not found. Creating a new one.")
        Path(dstlistname).parent.mkdir( parents=True, exist_ok=True )
        match_config.get_output_files("\*root:\*",dstlistname,args.dryrun)

    if not Path(dstlistname).is_file():
        INFO("List file not found.")
        exit(0)

    wccommand=f"wc -l {dstlistname}"
    ret = shell_command(wccommand)
    INFO("List contains {ret[0]} files.")

    ### Grab the first N files and work on those.
    nfiles_to_process=500000
    exhausted=False
    dstfiles=[]
    tmpname=f"{dstlistname}.tmp"
    with open(dstlistname,"r") as infile, open(f"{dstlistname}.tmp", "w") as smallerdstlist:
        for _ in range(nfiles_to_process):
            line=infile.readline()
            if line:
                dstfiles.append(line.strip())
            else:
                exhausted=True
                break
        for line in infile:
            smallerdstlist.write(line)
    if not args.dryrun:
        shutil.move(tmpname,dstlistname)
        if exhausted: # Used up the existing list.
            INFO("Used up all previously found dst files. Next call will create a new list")
            Path(dstlistname).unlink(missing_ok=True)
    else:
        Path(tmpname).unlink(missing_ok=True)
 
    # Done with selecting or creating our chunk, release the lock
    unlock_file(dstlistname,args.dryrun)

    ### Collect root files that satisfy run and dbid requirements
    mvfiles_info=[]
    for file in dstfiles:
        lfn=Path(file).name
        dsttype,run,seg,_=parse_lfn(lfn,rule)
        if binary_contains_bisect(rule.runlist_int,run):  # Safety net to move only specified runs
            fullpath,nevents,first,last,md5,size,ctime,dbid = parse_spiderstuff(file)
            if dbid <= 0:
                ERROR("dbid is {dbid}. Can happen for legacy files, but it shouldn't currently.")
                exit(0)
            info=filedb_info(dsttype,run,seg,fullpath,nevents,first,last,md5,size,ctime)
            mvfiles_info.append( (file,info) )

    INFO(f"{len(mvfiles_info)} total root files to be processed.")

    ####################################### Start moving and registering DSTs
    tstart = datetime.now()
    tlast = tstart
    chunksize=2000
    fmax=len(mvfiles_info)

    chunked_mvfiles = make_chunks(mvfiles_info, chunksize)
    for i, chunk in enumerate(chunked_mvfiles):
        now = datetime.now()
        print( f'DST #{i*chunksize}/{fmax}, time since previous output:\t {(now - tlast).total_seconds():.2f} seconds ({chunksize/(now - tlast).total_seconds():.2f} Hz). ' )
        print( f'                   time since the start:       \t {(now - tstart).total_seconds():.2f} seconds (cum. {i*chunksize/(now - tstart).total_seconds():.2f} Hz). ' )
        tlast = now

        fullinfo_chunk=[]
        seen_lfns=set()
        for file_and_info in chunk:
            file,info=file_and_info
            dsttype,run,seg,lfn,nevents,first,last,md5,size,time=info
            ## lfn duplication can happen for reproductions where only the db was updated without deleting existing output.
            ## The "best" one isn't always clear, so assume the latest one is better than what's old.
            if lfn in seen_lfns:
                existing = str(Path(file).parent)+'/'+lfn
                INFO(f"We already have a file with lfn {lfn}. Deleting {existing}.")
                if not args.dryrun:
                    Path(existing).unlink(missing_ok=True)
                continue
            seen_lfns.add(lfn)

            fileparent=Path(file).parent
            full_file_path = f'{fileparent}/{lfn}'
            fullinfo_chunk.append(full_db_info(
                origfile=file,
                info=info,
                lfn=lfn,
                full_file_path=full_file_path,
                dataset=rule.dataset,
                tag=rule.outtriplet,
                ))
            # end of chunk creation loop

        ###### Here be dragons
        ### Register first, then move.
        try:
            upsert_filecatalog(fullinfos=fullinfo_chunk,
                           dryrun=args.dryrun # only prints the query if True
                           )
        except Exception as e:
            WARN( f"dstspider is ignoring the database exception and moving on: {e}")
            ### database errors can happen when there are multiples of a file in the prod db.
            ### Why _that_ happens should be investigated, but here, we can just move on to the next chunk.
            continue
            exit(1)

        if not args.dryrun:
            for fullinfo in fullinfo_chunk:
                try:
                    os.rename( fullinfo.origfile, fullinfo.full_file_path )
                    # shutil.move( fullinfo.origfile, fullinfo.full_file_path )
                except Exception as e:
                    WARN(e)
                    # exit(-1)
                # end of chunk move loop
            # dryrun?
        pass # End of DST loop

    if args.profile:
        profiler.disable()
        DEBUG("Profiling finished. Printing stats...")
        stats = pstats.Stats(profiler)
        stats.strip_dirs().sort_stats('time').print_stats(10)

    INFO(f"{Path(sys.argv[0]).name} DONE.")

# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)
