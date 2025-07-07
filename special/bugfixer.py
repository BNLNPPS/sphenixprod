#!/usr/bin/env python

from pathlib import Path
from datetime import datetime
import subprocess
import sys
import shutil
import math
from typing import Tuple,List

from sphenixdbutils import cnxn_string_map, dbQuery
#from sphenixdbutils import filedb_info, upsert_filecatalog, update_proddb


# ============================================================================================
def shell_command(command: str) -> List[str]:
    """Minimal wrapper to hide away subbprocess tedium"""
    # CHATTY(f"[shell_command] Command: {command}")
    ret=[]
    try:
        ret = subprocess.run(command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').split()
    except subprocess.CalledProcessError as e:
        print("[shell_command] Command failed with exit code:", e.returncode)
    finally:
        pass

    return ret

# ============================================================================
def my_parse_spiderstuff(filename: str) -> Tuple[str,...] :
    try:
        # lfn,_,nevents,_,first,_,last,_,md5,_,dbid = filename.split(':')
        lfn = filename
        lfn=Path(lfn).name
    except Exception as e:
        print(f"Error: {e}")
        print(filename)
        print(filename.split(':'))
        exit(-1)

    return lfn,-1,-1,-1,-1,-1

# ============================================================================
def my_parse_lfn(lfn: str, outtriplet:str ):
    try:
        name=lfn.split(':')[0]
        name=Path(name).name # could throw an error instead if we're handed a full path.
         #  split at, and remove, run3auau_new_nocbdtag_v001, remainder is 'DST_...', '-00066582-00000.root' (or .finished)
        dsttype,runsegend=name.split(f'_run3auau_{outtriplet}')
        _,run,segend=runsegend.split('-')
        seg,end=segend.split('.')
    except ValueError as e:
        print(f"[parse_lfn] Caught error {e}")
        print(f"lfn = {lfn}")
        exit(-1)        
    return dsttype,int(run),int(seg),end

# try:
#         dsttype,runsegend=lfn.split(dataset) # 'DST_..._run3auau', '-00066582-00000.root' (or .finished)
#         _,run,segend=runsegend.split('-')
#         seg,end=segend.split('.')
#     except ValueError as e:
#         print(f"[parse_lfn] Caught error {e}")
#         print(f"lfn = {lfn}")
#         print(f"lfn.split(':') = {lfn.split(':')}")
#         print(f"name = {lfn.split(':')[0]}")
#         name=lfn.split(':')[0]
#         print(f"dsttype,runsegend = name.split(rule.dataset) = {name.split(rule.dataset)}")        
#         dsttype,runsegend=name.split(rule.dataset) # 'DST_..._run3auau', '-00066582-00000.root' (or .finished)
#         print(f"_,run,segend = runsegend.split('-') = {runsegend.split('-')}")
#         _,run,segend=runsegend.split('-')
#         print(f"seg,end = segend.split('.') = {segend.split('.')})")
#         seg,end=segend.split('.')
#         exit(-1)
        

#     # "dsttype" as currently used in the datasets table is e.g. DST_STREAMING_EVENT_ebdc01_1_run3auau
#     # We almost have that but need to strip off a trailing "_"
#     if dsttype[-1] == '_':
#         dsttype=dsttype[0:-1]

#    return dsttype,int(run),int(seg),end

# ============================================================================================

update_files_tmpl="""
update files
set full_file_path='{full_file_path}'
where lfn='{lfn}'
;
"""

# ============================================================================================

def main():
    rungroup_tmpl = "run_{a:08d}_{b:08d}"
    
    prod='physics'
    if prod=='physics':
        prodname='run3auau'
    elif prod=='cosmics':
        prodname='run3cosmics'
    elif prod=='line_laser':
        prodname='run3line_laser'
    else:
        print("don't know that prod")
        exit(-1)

    # type='DST_TRIGGERED'
    type='DST_STREAMING'
    outtriplet='new_nocdbtag_v001'
    if 'TRKR' in type:
        outtriplet='new_newcdbtag_v001'
    wrongdir=f'/sphenix/lustre01/sphnxpro/production/run3auau/{prod}/{outtriplet}'
    print(wrongdir)
    exit()
    
    dpattern=f'{type}_\*'
    # tmpfound = shell_command(f"find {wrongdir} -type f -name {pattern}")
    cmd=f'find {wrongdir} -maxdepth 1 -type d -name {dpattern}'
    tdirs=shell_command(cmd)
    for tdir in tdirs:
        cmd=f'find {tdir} -maxdepth 1 -type d -name run_\*'
        rdirs=shell_command(cmd)
        for rdir in rdirs:
            print('--- Working on {rdir}')
            chunksize=10000
            chunkcmd = f'/usr/bin/ls -1 {rdir} | grep -v :: | head -n {chunksize}'
            while True:
                chunk=shell_command(chunkcmd)
                if len(chunk)==0:
                    break
                for lfn in chunk:
                    dsttype,run,seg,_=my_parse_lfn(lfn,outtriplet=outtriplet)
                    leaf=dsttype.split(f"_{prodname}")[0]
                    rungroup=rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
                    #rightdir= 
                    
    exit()
    
    foundhists = [ file for file in tmpfound ]
    print(f"Found {len(foundhists)} histograms to register and move.")
        # dsttype,run,seg,_=my_parse_lfn(lfn,dataset='new_nocdbtag_v001')
        # leaf=dsttype.split(f"_{prodname}")[0]
        # leaf=leaf.split("HIST_")[1]
        # rungroup=rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
        # rightdir=f'/sphenix/data/data02/sphnxpro/production/run3auau/{prod}/new_nocdbtag_v001/{leaf}/{rungroup}/hist'
        # fullpath=rightdir+'/'+lfn
 
        # ### For additional db info. Note: stat is costly. Could be omitted with filestat=None
        # # Do it before the mv.
        # filestat=Path(file).stat()

        # ### Extract what else we need for file databases
        # full_file_path = fullpath

        # dryrun=False
        # dbstring = 'fcw'
        # ### Move
        # if dryrun:
        #     if f%when2blurb == 0:
        #         print( f"Dryrun: Pretending to do:\n mv {file} {full_file_path}" )
        # else:           
        #     # Move (rename) the file
        #     try:
        #         shutil.move( file, full_file_path )
        #     except Exception as e:
        #         WARN(e)

        
        # ### ... and update files catalog table
        # update_files=update_files_tmpl.format(
        #     full_file_path=full_file_path,
        #     lfn=lfn
        #     )
        # dbstring
        # if not dryrun:
        #     files_curs = dbQuery( cnxn_string_map[ dbstring ], update_files )
        #     files_curs.commit()
        # else:
        #     print(update_files)
    
# ============================================================================================

if __name__ == '__main__':
    main()
    exit(0)

    


