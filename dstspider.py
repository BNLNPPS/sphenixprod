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
from sphenixprodrules import RuleConfig,inputs_from_output
from sphenixprodrules import parse_lfn,parse_spiderstuff
from sphenixdbutils import test_mode as dbutils_test_mode
from sphenixdbutils import long_filedb_info, filedb_info, full_db_info, upsert_filecatalog, update_proddb  # noqa: F401
from sphenixmisc import binary_contains_bisect,shell_command


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
    
    filesystem = rule.job_config.filesystem
    DEBUG(f"Filesystem: {filesystem}")

    ### Which find command to use for lustre?
    # Lustre's robin hood, rbh-find, doesn't offer advantages for our usecase, and it is more cumbersome to use.
    # But "lfs find" is preferrable to the regular kind.
    find=shutil.which('find')
    lfind = shutil.which('lfs')    
    if lfind is None:
        WARN("'lfs find' not found")
        lfind = shutil.which('find')
    else:
        lfind = f'{lfind} find'
    INFO(f'Using find={find} and lfind="{lfind}.')

    ##################### DSTs, from lustre to lustre
    # Original output directory, the final destination, and the file name trunk
    dstbase = f'{rule.dsttype}\*{rule.dataset}_{rule.outtriplet}\*'
    # dstbase = f'{rule.dsttype}\*{rule.outtriplet}_{rule.dataset}\*' ## WRONG
    INFO(f'DST files filtered as {dstbase}')

    outlocation=filesystem['outdir']
    INFO(f"Directory tree: {outlocation}")
    # Further down, we will simplify by assuming finaldir == outdir, otherwise this script shouldn't be used.
    if filesystem['finaldir'] != outlocation:
         ERROR("Found finaldir != outdir. Use/adapt dstlakespider instead." )
         print(f"finaldir = {finaldir}")
         print(f"outdir = {outlocation}")
         exit(1)

    ### Use or create a list file containing all the existing files to work on.
    ### This reduces memory footprint and repeated slow `find` commands for large amounts of files
    dstlistname=filesystem['logdir']
    dstlistname=dstlistname.split("{")[0]
    while dstlistname.endswith("/"):
        dstlistname=dstlistname[0:-1]
    dstlistname=f"{dstlistname}/{rule.dsttype}_dstlist"
    dstlistlock=dstlistname+".lock"
    # First, lock. This way multiple spiders can work on a file without stepping on each others' (8) toes
    if Path(dstlistlock).exists():
        WARN(f"Lock file {dstlistlock} already exists, indicating another spider is running over the same rule.")
        # Safety valve. If it's old, we assume some job didn't end gracefully and proceed anyway.
        mod_timestamp = Path(dstlistlock).stat().st_mtime 
        mod_datetime = datetime.fromtimestamp(mod_timestamp) 
        time_difference = datetime.now() - mod_datetime
        threshold = 8 * 60 * 60
        if time_difference.total_seconds() > threshold:
            WARN(f"lock file is already {time_difference.total_seconds()} seconds old. Overriding.")
        else:
            exit(0)
    if not args.dryrun:
        Path(dstlistlock).parent.mkdir(parents=True,exist_ok=True)
        Path(dstlistlock).touch()
    INFO(f"Looking for existing filelist {dstlistname}")
    if Path(dstlistname).exists():
        wccommand=f"wc -l {dstlistname}"
        ret = shell_command(wccommand)
        INFO(f" ... found. List contains {ret[0]} files.")
    else:
        INFO(" ... not found. Creating a new one.")
        Path(dstlistname).parent.mkdir( parents=True, exist_ok=True )
        Path(dstlistname).unlink(missing_ok=True) ### should never be necessary
        
        # All leafs:
        tstart = datetime.now()
        leafparent=outlocation.split('/{leafdir}')[0]
        leafdirs = shell_command(f"{find} {leafparent} -type d -name {rule.dsttype}\* -mindepth 1 -a -maxdepth 1")
        CHATTY(f"Leaf directories: \n{pprint.pformat(leafdirs)}")

        # Run groups that we're interested in
        desirable_rungroups = { rule.job_config.rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100)) for run in rule.runlist_int }
    
        ### Walk through leafs - assume rungroups may change between run groups
        for leafdir in leafdirs :
            available_rungroups = shell_command(f"{find} {leafdir} -name run_\* -type d -mindepth 1 -a -maxdepth 1")
            # Very pythonic list comprehension here...
            # Want to have the subset of available rungroups where a desirable rungroup is a substring (cause the former have the full path)
            rungroups = {rg for rg in available_rungroups if any( drg in rg for drg in desirable_rungroups) }
            CHATTY(f"For {leafdir}, we have {len(rungroups)} run groups to work on")
            
            for rungroup in rungroups:
                shell_command(f"{lfind} {rungroup} -type f -name \*root:\* >> {dstlistname}")

        wccommand=f"wc -l {dstlistname}"
        ret = shell_command(wccommand)
        INFO(f"Found {ret[0]} DSTs to process.")
        INFO(f"List creation took {(datetime.now() - tstart).total_seconds():.2f} seconds.")

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
    if not args.dryrun:
        Path(dstlistlock).unlink()
    
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
            ## lfn duplication can happen for unclean productions. Detect here.
            ## We could try and id the "best" one but that's pricey for a rare occasion. Just delete the file and move on.
            #### It happens when productions get interrupted. Delete the existing one.
            if lfn in seen_lfns:
                existing = str(Path(file).parent)+'/'+lfn
                WARN(f"We already have a file with lfn {lfn}. Deleting {existing}.")
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
            WARN(f"dstspider is ignoring the database exception and moving on.")
            ### database errors can happen when there are multiples of a file in the dst.
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
