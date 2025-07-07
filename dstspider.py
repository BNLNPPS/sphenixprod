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
from sphenixdbutils import filedb_info, upsert_filecatalog, update_proddb  # noqa: F401
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

    ### root files without cuts
    lakefiles = shell_command(f"{lfind} {lakelocation} -type f -name {dstbase}\*.root\* | head -n 500000")
    DEBUG(f"Found {len(lakefiles)} matching dsts without cuts in the lake.")
         
    ### Collect root files that satisfy run and dbid requirements
    mvfiles_info=[]
    for file in lakefiles:
        pseudolfn=Path(file).name
        dsttype,run,seg,_=parse_lfn(pseudolfn,rule)
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
        leaf_types=rule.dsttype
    INFO(f"Destination type template: {leaf_template}")
    DEBUG(f"Destination types: {leaf_types}")
    
    ####################################### Start moving and registering DSTs
    tstart = datetime.now()
    tlast = tstart
    when2blurb=2000
    fmax=len(mvfiles_info)
    
    # chunk_size = 500
    chunked_mvfiles = make_chunks(mvfiles_info, when2blurb)
    for i, chunk in enumerate(chunked_mvfiles):
        now = datetime.now()            
        print( f'DST #{i*when2blurb}/{fmax}, time since previous output:\t {(now - tlast).total_seconds():.2f} seconds ({when2blurb/(now - tlast).total_seconds():.2f} Hz). ' )
        print( f'                   time since the start:       \t {(now - tstart).total_seconds():.2f} seconds (cum. {i*when2blurb/(now - tstart).total_seconds():.2f} Hz). ' )
        tlast = now

        for file_and_info in chunk:
            file,info=file_and_info
            dsttype,run,seg,lfn,nevents,first,last,md5,size,time=info
            # Check if we recognize the file name
            leaf=None
            for leaf_type in leaf_types:
                if lfn.startswith(leaf_type):
                    leaf=leaf_type
                    break
            if leaf is None:
                ERROR(f"Unknown file name: {lfn}")
                exit(-1)

            ### Fill in templates
            rungroup= rule.job_config.rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
            finaldir = finaldir_tmpl.format( leafdir=leaf, rungroup=rungroup )

            ### Extract what else we need for file databases
            ### For additional db info. Note: stat is costly. Use only if the determination on the worker node isn't sufficient
            filestat=None
            
            ###### Here be dragons
            full_file_path = f'{finaldir}/{lfn}'
            ### Register first, then move. 
            upsert_filecatalog(lfn=lfn,
                               info=info,
                               full_file_path = full_file_path,
                               filestat=filestat,
                               dataset=rule.dataset,
                               tag=rule.outtriplet,
                               dryrun=args.dryrun # only prints the query if True
                               )
            if args.dryrun:
                continue
            # Create destination dir if it doesn't exit. Can't be done elsewhere/earlier, we need the full relevant runnumber range
            Path(finaldir).mkdir( parents=True, exist_ok=True )
            try:
                shutil.move( file, full_file_path )
                # os.rename( file, full_file_path )
            except Exception as e:
                WARN(e)
                # exit(-1)
            pass # end of chunk loop
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
