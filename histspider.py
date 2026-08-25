#!/usr/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import pstats
import sys
import shutil
import os
import time
from typing import List

# from dataclasses import fields
import pprint # noqa F401

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit, shell_command
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig
from sphenixmatching import parse_lfn, parse_spiderstuff
from sphenixdbutils import long_filedb_info, filedb_info, full_db_info, upsert_filecatalog, update_proddb  # noqa: F401
from sphenixmisc import binary_contains_bisect

def stat_size_with_retry(path, retries=3, delay=0.5):
    """Return st_size, retrying on transient misreports. Raises OSError on failure."""
    size = path.stat().st_size
    for _ in range(retries - 1):
        again = path.stat().st_size
        if again == size:
            return size
        time.sleep(delay)
        size = again
    return size

# ============================================================================================

def main():
    ### digest arguments
    args = submission_args()

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
    param_overrides["check_legacy"] = args.check_legacy

    # Rest of the input substitutions
    if args.physicsmode is not None:
        param_overrides["physicsmode"] = args.physicsmode # e.g. physics

    param_overrides["prodmode"] = "production"
    CHATTY(f"Rule substitutions: {param_overrides}")
    INFO("Now loading and building rule configuration.")

    #################### Load specific rule from the given yaml file.
    try:
        rule =  RuleConfig.from_yaml_file( yaml_file=args.config, rule_name=args.rulename, param_overrides=param_overrides )
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
    leafdirs = shell_command(rf"{find} {leafparent} -type d -name {rule.dsttype}\* -mindepth 1 -a -maxdepth 1")
    DEBUG(f"Leaf directories: \n{pprint.pformat(leafdirs)}")
    allhistdirs = []
    for leafdir in leafdirs :
        allhistdirs += shell_command(f"{find} {leafdir} -name hist -type d")
    CHATTY(f"hist directories: \n{allhistdirs}")

    ### Finally, run over all HIST files in those directories
    # They too have dbinfo and need to be registered and renamed
    foundhists=[]
    for hdir in allhistdirs:
        tmpfound = shell_command(rf"{find} {hdir} -type f -name HIST\*root:\* -o -name CALIB\*")

        # Remove files that already end in ".root" - they're already registered
        foundhists += [ file for file in tmpfound if not file.endswith(".root") ]

    # Final cuts
    INFO(f"Found a total of {len(foundhists)} histograms to register. Checking against run constraint")
    act_on_hists=[]
    for loopfile in foundhists:
        try:
            lfn,nevents,first,last,md5,size,ctime,dbid = parse_spiderstuff(loopfile)
        except Exception as e:
            WARN(f"Error: {e}")
            continue
        try:
            dsttype,run,seg,_=parse_lfn(lfn,rule)
        except Exception as e:
            if e.args[0]=="killkillkill":
                WARN(f"{lfn} does not contain run and segment information. Delete.")
                ## DOUBLE-check to not delete already registered files.
                if not loopfile.endswith(".root"):
                    try:
                        Path(loopfile).unlink()
                    except Exception as delete_error:
                        ERROR(f"Failed to delete malformed incoming file {loopfile}: {delete_error}")
                    continue
                else:
                    WARN(f"{loopfile} looks like one we shouldn't have caught here anyway. Keep.")
            WARN(f"Error parsing lfn {lfn}: {e}. Skipped.")
            continue

        fullpath=str(Path(loopfile).parent)+'/'+lfn
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
                origfile=loopfile,
                info=info,
                lfn=lfn,
                full_file_path=full_file_path,
                dataset=rule.dataset,
                tag=rule.outtriplet,
                )
        act_on_hists.append((full_file_path,fullinfo))

    fmax=len(act_on_hists)
    INFO(f"Found {fmax} in the specified run range")

    ###### Here be dragons
    ### Move first, verify, then register. This mirrors dstspider and prevents
    ### FileCatalog from pointing at metadata whose final file was not placed.
    tstart = datetime.now()
    tlast = tstart
    when2blurb=2000
    verified_fullinfos_by_lfn = {}
    for f, (full_file_path,fullinfo) in enumerate(act_on_hists):
        if f%when2blurb == 0:
            now = datetime.now()
            print( f'HIST #{f}/{fmax}, time since previous output:\t {(now - tlast).total_seconds():.2f} seconds ({when2blurb/(now - tlast).total_seconds():.2f} Hz). ' )
            print( f'                  time since the start      :\t {(now - tstart).total_seconds():.2f} seconds (cum. {f/(now - tstart).total_seconds():.2f} Hz). ' )
            tlast = now

        if args.dryrun:
            if not Path(fullinfo.origfile).is_file():
                ERROR(f"Can't see {fullinfo.origfile}")
                exit(1)
            verified_fullinfos_by_lfn[fullinfo.lfn] = fullinfo
            continue

        orig_path = Path(fullinfo.origfile)
        final_path = Path(fullinfo.full_file_path)

        if fullinfo.lfn in verified_fullinfos_by_lfn:
            existing = verified_fullinfos_by_lfn[fullinfo.lfn]
            if fullinfo.ctime <= existing.ctime:
                ERROR(f"Duplicate incoming staged histogram for lfn {fullinfo.lfn}; deleting older-or-equal file (ctime {fullinfo.ctime} <= {existing.ctime})")
                try:
                    orig_path.unlink()
                except Exception as e:
                    ERROR(f"Failed to delete losing duplicate {orig_path}: {e}")
                continue
            ERROR(f"Duplicate incoming staged histogram for lfn {fullinfo.lfn}; replacing with newer file (ctime {fullinfo.ctime} > {existing.ctime})")

        try:
            orig_size = stat_size_with_retry(orig_path)
        except Exception as e:
            ERROR(f"Failed to stat incoming histogram {orig_path}: {e}")
            continue

        incoming_size_ok = fullinfo.size < 0 or orig_size == fullinfo.size

        if final_path.exists():
            if not incoming_size_ok:
                WARN(f"Incoming histogram size wrong ({orig_size} != {fullinfo.size}); keeping existing {final_path}")
                try:
                    orig_path.unlink()
                except Exception as e:
                    ERROR(f"Failed to delete rejected incoming histogram {orig_path}: {e}")
                continue
            INFO(f"Deleting existing final histogram before rename: {final_path}")
            try:
                final_path.unlink()
            except Exception as e:
                ERROR(f"Failed to remove existing final histogram {final_path}: {e}")
                continue
        else:
            if not incoming_size_ok:
                ERROR(f"Incoming histogram size wrong before rename for {orig_path}: expected {fullinfo.size}, got {orig_size}")
                try:
                    orig_path.unlink()
                except Exception as e:
                    ERROR(f"Failed to delete rejected incoming histogram {orig_path}: {e}")
                continue

        try:
            os.rename(orig_path, final_path)
        except Exception as e:
            print(f" {orig_path}\n{final_path}")
            ERROR(e)
            continue

        try:
            final_size = stat_size_with_retry(final_path)
        except Exception as e:
            ERROR(f"Failed to stat final histogram after rename {final_path}: {e}")
            continue
        if fullinfo.size >= 0 and final_size != fullinfo.size:
            ERROR(f"Histogram size changed during rename for {final_path}: expected {fullinfo.size}, got {final_size}")
            continue

        verified_fullinfos_by_lfn[fullinfo.lfn] = fullinfo

    verified_fullinfos = list(verified_fullinfos_by_lfn.values())
    if verified_fullinfos:
        try:
            upsert_filecatalog(fullinfos=verified_fullinfos,
                               dryrun=args.dryrun
                               )
        except Exception as e:
            ERROR(f"histspider moved verified files but FileCatalog registration failed: {e}")

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
