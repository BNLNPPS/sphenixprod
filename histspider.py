#!/usr/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import pstats
import subprocess
import sys
import shutil
import os
from typing import List

# from dataclasses import fields
import pprint # noqa F401

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixprodrules import parse_lfn,parse_spiderstuff
from sphenixdbutils import test_mode as dbutils_test_mode
from sphenixdbutils import long_filedb_info, filedb_info, full_db_info, upsert_filecatalog, update_proddb  # noqa: F401
from sphenixmisc import binary_contains_bisect

# ============================================================================================
def shell_command(command: str) -> List[str]:
    """Minimal wrapper to hide away subbprocess tedium"""
    CHATTY(f"[shell_command] Command: {command}")
    ret=[]
    try:
        ret = subprocess.run(command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').split()
    except subprocess.CalledProcessError as e:
        WARN("[shell_command] Command failed with exit code:", e.returncode)
    finally:
        pass

    CHATTY(f"[shell_command] Found {len(ret)} matches.")
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

    ################################  Move histogram files.
    # Very similar to dstspider, use one function for both types.
    # Main difference is that it's easier to identify daqhost/leaf from the path
    # TODO: Dirty hardcoding assuming knowledge of histdir naming scheme
    find = shutil.which('find') # on gpfs, no need for lfs find, use the more powerful generic find
    histdir=filesystem['histdir']
    INFO(f"Histogram directory template: {histdir}")
    
    # All leafs:
    leafparent=histdir.split('/{leafdir}')[0]
    leafdirs = shell_command(f"{find} {leafparent} -type d -name {rule.dsttype}\* -mindepth 1 -a -maxdepth 1")
    DEBUG(f"Leaf directories: \n{pprint.pformat(leafdirs)}")
    allhistdirs = []
    for leafdir in leafdirs :
        allhistdirs += shell_command(f"{find} {leafdir} -name hist -type d")
    CHATTY(f"hist directories: \n{allhistdirs}")

    ### Finally, run over all HIST files in those directories
    # They too have dbinfo and need to be registered and renamed
    foundhists=[]
    for hdir in allhistdirs:        
        tmpfound = shell_command(f"{find} {hdir} -type f -name HIST\* -o -name CALIB\*")
        # Remove files that already end in ".root" - they're already registered
        foundhists += [ file for file in tmpfound if not file.endswith(".root") ]
    INFO(f"Found {len(foundhists)} histograms to register.")
    
    tstart = datetime.now()
    tlast = tstart
    when2blurb=2000
    fmax=len(foundhists)
    for f, file in enumerate(foundhists):
        if f%when2blurb == 0:
            now = datetime.now()
            print( f'HIST #{f}/{fmax}, time since previous output:\t {(now - tlast).total_seconds():.2f} seconds ({when2blurb/(now - tlast).total_seconds():.2f} Hz). ' )
            print( f'                  time since the start      :\t {(now - tstart).total_seconds():.2f} seconds (cum. {f/(now - tstart).total_seconds():.2f} Hz). ' )
            tlast = now
        try:
            lfn,nevents,first,last,md5,size,ctime,dbid = parse_spiderstuff(file)
        except Exception as e:
            WARN(f"Error: {e}")
            continue
        fullpath=str(Path(file).parent)+'/'+lfn
        dsttype,run,seg,_=parse_lfn(lfn,rule)
        
        if binary_contains_bisect(rule.runlist_int,run):
            if dbid <= 0:
                ERROR("dbid is {dbid}. Can happen for legacy files, but it shouldn't currently.")
                exit(0)
            info=filedb_info(dsttype,run,seg,fullpath,nevents,first,last,md5,size,ctime)
        else:
            continue

        ### Extract what else we need for file databases
        full_file_path = fullpath

        fullinfo=full_db_info(
                origfile=file,
                info=info,
                lfn=lfn,
                full_file_path=full_file_path,
                dataset=rule.dataset,
                tag=rule.outtriplet,
                )

        ###### Here be dragons
        ### Register first, then move. 
        upsert_filecatalog(fullinfos=fullinfo,
                           dryrun=args.dryrun # only prints the query if True
                           )
        if args.dryrun:
            continue
        try:
            os.rename( file, full_file_path )
        except Exception as e:
            WARN(e)
            # exit(-1)

        pass # End of HIST loop
    
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
