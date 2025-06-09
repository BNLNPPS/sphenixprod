#!/usr/bin/env python

from pathlib import Path
from datetime import datetime
import yaml
import cProfile
import subprocess
import sys
import shutil
import math

# from dataclasses import fields
import pprint # noqa F401

from argparsing import submission_args
from sphenixmisc import setup_rot_handler, should_I_quit
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig,list_to_condition, extract_numbers_to_commastring, inputs_from_output
from sphenixdbutils import test_mode as dbutils_test_mode
from sphenixdbutils import cnxn_string_map, dbQuery

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
    # if test_mode:
    #     Path('.testbed').touch()

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
    rule_substitions = {}
    rule_substitions["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor

    # Rest of the input substitutions
    if args.physicsmode is not None:
        rule_substitions["physicsmode"] = args.physicsmode # e.g. physics

    if args.mangle_dstname:
        DEBUG("Mangling DST name")
        rule_substitions['DST']=args.mangle_dstname

    # filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    rule_substitions["prodmode"] = "production"
    if args.mangle_dirpath:
        rule_substitions["prodmode"] = args.mangle_dirpath

    CHATTY(f"Rule substitutions: {rule_substitions}")
    INFO("Now loading and building rule configuration.")

    #################### Load specific rule from the given yaml file.
    try:
        rule =  RuleConfig.from_yaml_file( yaml_file=args.config, rule_name=args.rulename, rule_substitions=rule_substitions )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        exit(1)

    CHATTY("Rule configuration:")
    CHATTY(yaml.dump(rule.dict))


    # Which find command to use?
    filesystem = rule.job_config.filesystem

    # Lustre's robin hood, rbh-find, doesn't offer advantages for our usecase, and it is more cumbersome to use.
    # But "lfs find" is preferrable to the regular kind.
    lfind = shutil.which('lfs')
    if lfind is None:
        WARN("'lfs find' not found.")
        lfind = shutil.which('find')
    else:
        lfind = f'{lfind} find'
    INFO(f'Using "{lfind}.')
    
    ##################### DSTs, from lustre to lustre
    # Original output directory, the final destination, and the file name trunk
    inlocation=filesystem['outdir']
    finaldir=filesystem['finaldir']
    DEBUG(f"Filesystem: {filesystem}")
    INFO(f"Original output directory: {inlocation}")
    INFO(f"Final destination template: {finaldir}")

    # List of files to process
    findcommand = f"{lfind} {inlocation} -type f -name {rule.rulestem}\* -print"
    INFO(f"Find command: {findcommand}")
    foundfiles = subprocess.run(findcommand, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()

    # Extract information encoded in the file name
    input_stem = inputs_from_output[rule.rulestem]
    INFO(f"Input stem: {input_stem}")
    outstub = rule.outstub
    INFO(f"Output stub: {outstub}")

    dst_type_template = f'{rule.rulestem}'
    if 'raw' in rule.input_config.db:
        dst_type_template += '_{host}'
    dst_types = { f'{dst_type_template}'.format(host=host) for host in input_stem.keys() }
    INFO(f"Destination type template: {dst_type_template}")
    INFO(f"Destination types: {dst_types}")
    INFO(f"Found {len(foundfiles)} files to move.")

    tstart = datetime.now()
    tlast = tstart
    when2blurb=2000
    for f, file in enumerate(foundfiles):
        if f%when2blurb == 0:
            now = datetime.now()            
            print( f'DST #{f}/{len(foundfiles)}, time since previous output:\t {(now - tlast).total_seconds():.2f} seconds ({when2blurb/(now - tlast).total_seconds():.2f} Hz). ' )
            print( f'                  time since the start      :\t {(now - tstart).total_seconds():.2f} seconds (cum. {f/(now - tstart).total_seconds():.2f} Hz). ' )
            tlast = now

        try:
            fullpath,_,nevents,_,first,_,last,_,md5,_,dbid = file.split(':')
        except Exception as e:
            DEBUG(f"Error: {e}")
            continue

        lfn=Path(fullpath).name
        # Check if we recognize the file name
        leaf=None
        for dst_type in dst_types:
            if lfn.startswith(dst_type):
                leaf=dst_type
                break
        if leaf is None:
            # DEBUG(f"Unknown file name: {lfn}")
            continue
        
        # Extract runnumber from the file name
        # Note: I don't love this. It assumes a rigid file name format, and it eats time for every file.
        #       All those splits are pretty unreadable too. Well, still beats a regex.
        #       At least processing time turns out not to be a problem.
        # Logic: first split is at new_nocdbtag_v000, second split isolates the run number, which is between two dashes
        dsttype=lfn.split(f'_{rule.dataset}')[0]
        run = int(lfn.split(rule.dataset)[1].split('-')[1])
        rungroup= rule.job_config.rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
        # Fill in rungroup and optionally leaf directory
        finaldir = finaldir.format( leafdir=leaf, rungroup=rungroup )
        # Between the dash and .root is the segment, used for the db
        segment = int(lfn.split(rule.dataset)[1].split('-')[2].split('.root')[0])

        # --- Extract what we need for the databases
        # for "files"
        full_host_name = "lustre" if 'lustre' in finaldir else 'gpfs'
        full_file_path = f'{finaldir}/{lfn}'
        filestat=Path(file).stat()
        ctimestamp = filestat.st_ctime
        ctimestamp = datetime.fromtimestamp(ctimestamp)
        file_size_bytes = filestat.st_size
        files_table='test_files' if test_mode else 'files'
        insert_files=f"""
    insert into {files_table} (lfn,full_host_name,full_file_path,time,size,md5) 
    values ('{lfn}','{full_host_name}','{full_file_path}','{ctimestamp}',{file_size_bytes},'{md5}')
    on conflict
    on constraint {files_table}_pkey
    do update set 
    time=EXCLUDED.time,
    size=EXCLUDED.size,
    md5=EXCLUDED.md5
    ;
    """
        CHATTY(insert_files)

        # for 'datasets'
        datasets_table='test_datasets' if test_mode else 'datasets'
        insert_datasets=f"""
    insert into {datasets_table} (filename,runnumber,segment,size,dataset,dsttype,events)
    values ('{lfn}',{run},{segment},{file_size_bytes},'{rule.dataset}','{dsttype}',{nevents})
    on conflict
    on constraint {datasets_table}_pkey
    do update set
    runnumber=EXCLUDED.runnumber,
    segment=EXCLUDED.segment,
    size=EXCLUDED.size,
    dsttype=EXCLUDED.dsttype,
    events=EXCLUDED.events
    ;
    """
        CHATTY(insert_datasets)        
        if args.dryrun:
            if f%when2blurb == 0:
                print( f"mv {file} {full_file_path}" )
            continue
        
        # Create destination dir if it doesn't exit. Difficult to move out of the file loop before knowing the full relevant runnumber range
        Path(finaldir).mkdir( parents=True, exist_ok=True )
        # Move the file
        try:
            shutil.move( file, full_file_path )
        except Exception as e:
            WARN(e)
        #Register the file
        # insdsets, ntries_dsets, start_dsets, finish_dsets, ex_dsets, nm_dsets, sv_dsets = dbQuery( cnxn_string_map[ 'fcw' ], insert )
        dbstring = 'testw' if test_mode else 'fcw'
        files_curs = dbQuery( cnxn_string_map[ dbstring ], insert_files )
        files_curs.commit()
        datasets_curs = dbQuery( cnxn_string_map[ dbstring ], insert_datasets )
        datasets_curs.commit()

    # Same thing for histogram files.
    # TODO: Very similar, use one function for both types.
    # Main difference is that it's easier to identify daqhost/leaf from the path
    # TODO: Dirty hardcoding assuming knowledge of histdir naming scheme
    find = shutil.which('find') # on gpfs, no nead for lfs find, use the more powerful generic find
    histdir=filesystem['histdir']
    INFO(f"Histogram directory template: {histdir}")
    
    # # All leafs:
    leafparent=histdir.split('/{leafdir}')[0]
    INFO(f"Leaf directories: \n{leafparent}")

    leaffind = f"{find} {leafparent} -type d -mindepth 1 -a -maxdepth 1 -print"
    INFO(f"Find command: {leaffind}")
    leafdirs = subprocess.run(leaffind, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
    CHATTY(f"Leaf directories: \n{leafdirs}")
    
    allhistdirs = []
    for leafdir in leafdirs :
        hist_find = f"{find} {leafdir} -name hist -type d -print"
        CHATTY(f"hist directory find command: {hist_find}")
        allhistdirs += subprocess.run(hist_find, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
    CHATTY(f"hist directories: \n{allhistdirs}")

    # Finally, run over all HIST files in those directories
    foundfiles=[]
    for hdir in allhistdirs:        
        findcommand = f"{find} {hdir} -type f -name HIST\* -print"
        tmpfound = subprocess.run(findcommand, shell=True, check=True, capture_output=True).stdout.decode('utf-8').splitlines()
        # Remove ".root" files, they're already processed.
        foundfiles += [ file for file in tmpfound if not file.endswith(".root") ]

    tstart = datetime.now()
    tlast = tstart
    when2blurb=2000
    for f, file in enumerate(foundfiles):
        if f%when2blurb == 0:
            now = datetime.now()
            print( f'HIST #{f}/{len(foundfiles)}, time since previous output:\t {(now - tlast).total_seconds():.2f} seconds ({when2blurb/(now - tlast).total_seconds():.2f} Hz). ' )
            print( f'                  time since the start      :\t {(now - tstart).total_seconds():.2f} seconds (cum. {f/(now - tstart).total_seconds():.2f} Hz). ' )
            tlast = now
            
        try:
            fullpath,_,nevents,_,first,_,last,_,md5,_,dbid = file.split(':')
        except Exception as e:
            WARN(f"Error: {e}")
            continue

        # Extract runnumber from the file name
        # Note: I don't love this. It assumes a rigid file name format, and it eats time for every file.
        #       All those splits are pretty unreadable too. Well, still beats a regex.
        #       At least processing time turns out not to be a problem.
        # Logic: first split is at new_nocdbtag_v000, second split isolates the run number, which is between two dashes
        lfn=Path(fullpath).name
        dsttype=lfn.split(f'_{rule.dataset}')[0]
        run = int(lfn.split(rule.dataset)[1].split('-')[1])
        # Between the dash and .root is the segment, used for the db
        segment = int(lfn.split(rule.dataset)[1].split('-')[2].split('.root')[0])
        
        # --- Extract what we need for the databases
        # for "files"
        full_host_name = "lustre" if 'lustre' in histdir else 'gpfs'
        full_file_path = fullpath # f'{finaldir}/{lfn}'
        filestat=Path(file).stat()
        ctimestamp = filestat.st_ctime
        ctimestamp = datetime.fromtimestamp(ctimestamp)
        file_size_bytes = filestat.st_size
        files_table='test_files' if test_mode else 'files'
        insert_files=f"""
    insert into {files_table} (lfn,full_host_name,full_file_path,time,size,md5) 
    values ('{lfn}','{full_host_name}','{full_file_path}','{ctimestamp}',{file_size_bytes},'{md5}')
    on conflict
    on constraint {files_table}_pkey
    do update set 
    time=EXCLUDED.time,
    size=EXCLUDED.size,
    md5=EXCLUDED.md5
    ;
    """
        CHATTY(insert_files)

        # for 'datasets'
        datasets_table='test_datasets' if test_mode else 'datasets'
        insert_datasets=f"""
    insert into {datasets_table} (filename,runnumber,segment,size,dataset,dsttype,events)
    values ('{lfn}',{run},{segment},{file_size_bytes},'{rule.dataset}','{dsttype}',{nevents})
    on conflict
    on constraint {datasets_table}_pkey
    do update set
    runnumber=EXCLUDED.runnumber,
    segment=EXCLUDED.segment,
    size=EXCLUDED.size,
    dsttype=EXCLUDED.dsttype,
    events=EXCLUDED.events
    ;
    """
        CHATTY(insert_datasets)
        if args.dryrun:
            if f%when2blurb == 0:
                print( f"mv {file} {full_file_path}" )
            continue
        
        try:
            shutil.move( file, full_file_path )
        except Exception as e:
            WARN(e)
        #Register the file
        dbstring = 'testw' if test_mode else 'fcw'
        files_curs = dbQuery( cnxn_string_map[ dbstring ], insert_files )
        files_curs.commit()
        datasets_curs = dbQuery( cnxn_string_map[ dbstring ], insert_datasets )
        datasets_curs.commit()

        
# ============================================================================================

if __name__ == '__main__':
    # main()
    # exit(0)

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
