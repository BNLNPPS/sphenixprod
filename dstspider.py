#!/usr/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import subprocess
import sys
import shutil
import os
import math
from typing import List

# from dataclasses import fields
import pprint # noqa F401

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit, make_chunks
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig,inputs_from_output
from sphenixprodrules import parse_lfn,parse_spiderstuff
from sphenixdbutils import test_mode as dbutils_test_mode
from sphenixdbutils import long_filedb_info, filedb_info, full_db_info, upsert_filecatalog, update_proddb  # noqa: F401
from sphenixmisc import binary_contains_bisect

# ============================================================================================
def shell_command(command: str) -> List[str]:
    """Minimal wrapper to hide away subbprocess tedium"""
    DEBUG(f"[shell_command] Command: {command}")
    ret=[]
    try:
        ret = subprocess.run(command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').split()
    except subprocess.CalledProcessError as e:
        WARN("[shell_command] Command failed with exit code:", e.returncode)
    finally:
        pass

    DEBUG(f"[shell_command] Found {len(ret)} matches.")
    return ret

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
    lfind = shutil.which('lfs')
    if lfind is None:
        WARN("'lfs find' not found")
        lfind = shutil.which('find')
    else:
        lfind = f'{lfind} find'
    INFO(f'Using "{lfind}.')

    ##################### DSTs, from lustre to lustre
    # Original output directory, the final destination, and the file name trunk
    dstbase = f'{rule.dsttype}\*{rule.dataset}_{rule.outtriplet}\*'
    # dstbase = f'{rule.dsttype}\*{rule.outtriplet}_{rule.dataset}\*' ## WRONG
    INFO(f'DST files filtered as {dstbase}')
    lakelocation=filesystem['outdir']
    INFO(f"Original output directory: {lakelocation}")

    ### Use or create a list file containing all the existing lake files to work on.
    ### This reduces memory footprint and repeated slow `find` commands for large amounts of files
    # Use the name of the lake directory
    lakelistname=lakelocation
    while lakelistname.endswith("/"):
        lakelistname=lakelistname[0:-1]
    lakelistname+="_lakelist"
    lakelistlock=lakelistname+".lock"
    # First, lock. This way multiple spiders can work on a file without stepping on each others' (8) toes
    if Path(lakelistlock).exists():
        WARN(f"Lock file {lakelistlock} already exists, indicating another spider is running over the same rule.")
        # Safety valve. If it's old, we assume some job didn't end gracefully and proceed anyway.
        mod_timestamp = Path(lakelistlock).stat().st_mtime 
        mod_datetime = datetime.fromtimestamp(mod_timestamp) 
        time_difference = datetime.now() - mod_datetime
        threshold = 8 * 60 * 60
        if time_difference.total_seconds() > threshold:
            WARN(f"lock file is already {time_difference.total_seconds()} seconds old. Overriding.")
        else:
            exit(0)
    if not args.dryrun:
        Path(lakelistlock).touch()
    INFO(f"Looking for existing filelist {lakelistname}")
    if not Path(lakelistname).exists():
        INFO(" ... not found. Creating a new one.")
        findcommand=f"{lfind} {lakelocation} -type f -name {dstbase}\*.root\* > {lakelistname}; wc -l {lakelistname}"
        DEBUG(f"Using:\n{findcommand}")
        ret = shell_command(findcommand)
        INFO(f"Found {ret[0]} matching dsts without cuts in the lake, piped into {ret[1]}")
    else:
        wccommand=f"wc -l {lakelistname}"
        ret = shell_command(wccommand)
        INFO(f" ... found. List contains {ret[0]} files.")

    ### Grab the first N files and work on those.
    nfiles_to_process=500000
    exhausted=False
    lakefiles=[]
    tmpname=f"{lakelistname}.tmp"
    with open(lakelistname,"r") as infile, open(f"{lakelistname}.tmp", "w") as smallerlakefile:
        for _ in range(nfiles_to_process):
            line=infile.readline()
            if line:
                lakefiles.append(line.strip())
            else:
                exhausted=True
                break
        for line in infile:
            smallerlakefile.write(line)
    if not args.dryrun:
        shutil.move(tmpname,lakelistname)
        if exhausted: # Used up the existing list.
            INFO("Used up all previously found lake files. Next call will create a new list")
            Path(lakelistname).unlink(missing_ok=True)
    else:
        Path(tmpname).unlink(missing_ok=True)
    # Done with selecting or creating our chunk, release the lock
    if not args.dryrun:
        Path(lakelistlock).unlink()

    ### Collect root files that satisfy run and dbid requirements
    mvfiles_info=[]
    for file in lakefiles:
        lfn=Path(file).name
        dsttype,run,seg,_=parse_lfn(lfn,rule)
        if run<=66456:
            WARN(f"Deleting {lfn}")
            Path(file).unlink()
            continue

        if binary_contains_bisect(rule.runlist_int,run):
            fullpath,nevents,first,last,md5,size,ctime,dbid = parse_spiderstuff(file)
            if dbid <= 0:
                ERROR("dbid is {dbid}. Can happen for legacy files, but it shouldn't currently.")
                exit(0)
            info=filedb_info(dsttype,run,seg,fullpath,nevents,first,last,md5,size,ctime)
            mvfiles_info.append( (file,info) )
            
    INFO(f"{len(mvfiles_info)} total root files to be processed.")
    
    finaldir_tmpl=filesystem['finaldir']
    INFO(f"Final destination template: {finaldir_tmpl}")

    input_stubs = inputs_from_output[rule.dsttype]
    DEBUG(f"Input stub(s): {input_stubs}")
    dataset = rule.dataset
    INFO(f"Dataset identifier: {dataset}")
    leaf_template = f'{rule.dsttype}'
    if 'raw' in rule.input_config.db:
        leaf_template += '_{host}'
        leaf_types = { f'{leaf_template}'.format(host=host) for host in input_stubs.keys() }
    else:
        leaf_types=[rule.dsttype]
    INFO(f"Destination type template: {leaf_template}")
    DEBUG(f"Destination types: {leaf_types}")
    
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
            if lfn in seen_lfns:
                WARN(f"We already have a file with lfn {lfn}. Deleting {file}.")
                Path(file).unlink()
                continue
            seen_lfns.add(lfn)

            # Check if we recognize the file name
            leaf=None
            for leaf_type in leaf_types:
                if lfn.startswith(leaf_type):
                    leaf=leaf_type
                    break
            if leaf is None:
                ERROR(f"Unknown file name: {lfn}")
                exit(-1)

            ### Fill in templates and save full information
            rungroup= rule.job_config.rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
            finaldir = finaldir_tmpl.format( leafdir=leaf, rungroup=rungroup )
            # Create destination dir if it doesn't exit. Can't be done elsewhere/earlier, we need the full relevant runnumber range
            if not args.dryrun:
                Path(finaldir).mkdir( parents=True, exist_ok=True )

            full_file_path = f'{finaldir}/{lfn}'
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
        upsert_filecatalog(fullinfos=fullinfo_chunk,
                           dryrun=args.dryrun # only prints the query if True
                           )
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
    # time: Sort by the total time spent in the function itself. 
