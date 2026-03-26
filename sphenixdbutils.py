#!/usr/bin/env python
import pyodbc
from pathlib import Path
import pprint # noqa: F401

import time
from datetime import datetime
import random
import os
import argparse

from typing import overload, List, Union
from collections import namedtuple

def get_parser():
    parser = argparse.ArgumentParser(description='sPHENIX DB utilities')
    subparsers = parser.add_subparsers(dest='command', required=True)

    # jobstarted subcommand
    parser_jobstarted = subparsers.add_parser('jobstarted', help='Mark a job as started')
    parser_jobstarted.add_argument('--dbid', required=False, type=int, default=None, help='Database ID of the job. Defaults to $PRODDB_DBID.')
    parser_jobstarted.add_argument('--dryrun', action='store_true', help='Do not perform database updates.')

    # jobended subcommand
    parser_jobended = subparsers.add_parser('jobended', help='Mark a job as ended')
    parser_jobended.add_argument('--dbid', required=False, type=int, default=None, help='Database ID of the job. Defaults to $PRODDB_DBID.')
    parser_jobended.add_argument('--exit-code', required=False, type=int, default=0, help='Exit code of the job.')
    parser_jobended.add_argument('-n','--dryrun', action='store_true', help='Do not perform database updates.')

    return parser.parse_args()

filedb_info = namedtuple('filedb_info', ['dsttype','run','seg','lfn','nevents','first','last','md5','size','ctime'])
long_filedb_info = namedtuple('long_filedb_info', [
    'origfile',                                                               # for moving
    'lfn','full_host_name','full_file_path','ctime','size','md5',             # for files
    'run','seg','dataset','dsttype','nevents','first','last','status','tag',  # addtl. for datasets
])

from simpleLogger import WARN, ERROR, DEBUG, INFO, CHATTY  # noqa: E402, F401

"""
This module provides an interface to the sPHENIX databases.
Used both by submission scripts and by the production payload scripts themselves,
so it should remain lightweight and not depend on any other package modules.
Also, it needs a robust way to establish things like testbed vs. production mode.
"""

# ============================================================================

#################### Test mode? Multiple ways to turn it on
test_mode = (
        False
        or 'testbed' in str(Path(".").absolute()).lower()
        or Path(".testbed").exists()
        or Path("SPHNX_TESTBED_MODE").exists()
    )

prod_mode = Path("SPHNX_PRODUCTION_MODE").exists()
if ( prod_mode ):
    dsnprodr = 'Production_read'
    dsnprodw = 'Production_write'
    dsnfilec = 'FileCatalog'
elif ( test_mode ):
    dsnprodr = 'ProductionStatus'
    dsnprodw = 'ProductionStatusWrite'
    dsnfilec = 'FileCatalog'
else:
    INFO("Neither production nor testbed mode set. Default to PRODUCTION.  YMMV.")
    dsnprodr = 'Production_read'
    dsnprodw = 'Production_write'
    dsnfilec = 'FileCatalog'

# ============================================================================
cnxn_string_map = {
    'fcw'         : f'DSN={dsnfilec};UID=phnxrc',
    'fcr'         : f'DSN={dsnfilec};READONLY=True;UID=phnxrc',
    'statr'       : f'DSN={dsnprodr};READONLY=True;UID=argouser',
    'statw'       : f'DSN={dsnprodw};UID=argouser',
    'daqr'        :  'DSN=daq;READONLY=True;UID=phnxrc',
    'rawr'        :  'DSN=RawdataCatalog_read;READONLY=True;UID=phnxrc',
    'testw'       :  'DSN=FileCatalogTest;UID=phnxrc',

}

# Hack to test locally on Mac
if os.uname().sysname=='Darwin' :
    cnxn_string_map = {
        'fcw'         : 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DATABASE=filecatalogdb;UID=eickolja',
        'fcr'         : 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DATABASE=filecatalogdb;READONLY=True;UID=eickolja',
        'statr'       : 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DATABASE=productiondb;READONLY=True;UID=eickolja',
        'statw'       : 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DATABASE=productiondb;UID=eickolja',
        'rawr'        : 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DATABASE=rawdatacatalogdb;READONLY=True;UID=eickolja',
    }

# # Hack to use local PostgreSQL database from inside a docker container
# if Path('/.dockerenv').exists() :
#     driverstring='DRIVER=PostgreSQL;SERVER=host.docker.internal;'
#     cnxn_string_map = {
#         'fcw'         : f'{driverstring}DATABASE=filecatalogdb;UID=eickolja',
#         'fcr'         : f'{driverstring}DATABASE=filecatalogdb;READONLY=True;UID=eickolja',
#         'statr'       : f'{driverstring}DATABASE=productiondb;READONLY=True;UID=eickolja',
#         'statw'       : f'{driverstring}DATABASE=productiondb;UID=eickolja',
#         'rawr'        : f'{driverstring}DATABASE=rawdatacatalogdb;READONLY=True;UID=eickolja',
#     }

# ============================================================================================
def full_db_info(origfile: str, info: filedb_info, lfn: str, full_file_path: str, dataset: str, tag: str) -> long_filedb_info:
    return long_filedb_info(
        origfile=origfile,
        lfn=lfn,
        full_host_name = "lustre" if 'lustre' in full_file_path else 'gpfs',
        full_file_path=full_file_path,
        ctime=info.ctime,
        size=info.size,
        md5=info.md5,
        run=info.run,
        seg=info.seg,
        dataset=dataset,
        dsttype=info.dsttype,
        nevents=info.nevents,first=info.first,last=info.last,
        status=1,
        tag=tag,
    )

# ============================================================================================
files_db_line = "('{lfn}','{full_host_name}','{full_file_path}','{ctimestamp}',{file_size_bytes},'{md5}')"
insert_files_tmpl="""
insert into {files_table} (lfn,full_host_name,full_file_path,time,size,md5)
values
{files_db_lines}
on conflict
on constraint {files_table}_pkey
do update set
time=EXCLUDED.time,
size=EXCLUDED.size,
md5=EXCLUDED.md5
;
"""

# ---------------------------------------------------------------------------------------------
datasets_db_line="('{lfn}',{run},{segment},{file_size_bytes},'{dataset}','{dsttype}',{nevents},{firstevent},{lastevent},'{tag}')"
insert_datasets_tmpl="""
insert into {datasets_table} (filename,runnumber,segment,size,dataset,dsttype,events,firstevent,lastevent,tag)
values
{datasets_db_lines}
on conflict
on constraint {datasets_table}_pkey
do update set
runnumber=EXCLUDED.runnumber,
segment=EXCLUDED.segment,
size=EXCLUDED.size,
dsttype=EXCLUDED.dsttype,
events=EXCLUDED.events,
firstevent=EXCLUDED.firstevent,
lastevent=EXCLUDED.lastevent,
tag=EXCLUDED.tag
;
"""

# ---------------------------------------------------------------------------------------------
#def upsert_filecatalog(lfn: str, info: filedb_info, full_file_path: str, dataset: str, tag: str, filestat=None, dryrun=True ):
# ---------------------------------------------------------------------------------------------
@overload
def upsert_filecatalog(fullinfos: long_filedb_info, dryrun=True ):
    ...
@overload
def upsert_filecatalog(fullinfos: List[long_filedb_info], dryrun=True ):
    ...

def upsert_filecatalog(fullinfos: Union[long_filedb_info,List[long_filedb_info]], dryrun=True ):
    if isinstance(fullinfos, long_filedb_info):
        fullinfos=[fullinfos]
    elif isinstance(fullinfos, list):
        pass
    else:
        raise TypeError("Unsupported data type")

    files_db_lines = []
    datasets_db_lines=[]
    for fullinfo in fullinfos:
        files_db_lines.append( files_db_line.format(
            lfn=fullinfo.lfn,
            full_host_name = fullinfo.full_host_name,
            full_file_path = fullinfo.full_file_path,
            ctimestamp = datetime.fromtimestamp(fullinfo.ctime),
            file_size_bytes = fullinfo.size,
            md5=fullinfo.md5,
        ))
        datasets_db_lines.append( datasets_db_line.format(
            lfn=fullinfo.lfn,
            md5=fullinfo.md5,
            run=fullinfo.run, segment=fullinfo.seg,
            file_size_bytes=fullinfo.size,
            dataset=fullinfo.dataset,
            dsttype=fullinfo.dsttype,
            nevents=fullinfo.nevents,
            firstevent=fullinfo.first,
            lastevent=fullinfo.last,
            tag=fullinfo.tag,
        ))

    files_db_lines = ",\n".join(files_db_lines)
    insert_files=insert_files_tmpl.format(
        files_table='test_files' if test_mode else 'files',
        files_db_lines = files_db_lines,
    )
    CHATTY(insert_files)

    datasets_db_lines=",\n".join(datasets_db_lines)
    insert_datasets=insert_datasets_tmpl.format(
        datasets_table='test_datasets' if test_mode else 'datasets',
        datasets_db_lines=datasets_db_lines,
    )
    CHATTY(insert_datasets)
    if not dryrun:
        dbstring = 'testw' if test_mode else 'fcw'
        files_curs = dbQuery( cnxn_string_map[ dbstring ], insert_files )
        if files_curs:
            files_curs.commit()
        else:
            ERROR(f"Failed to insert file(s)into database {dbstring}. Line was:")
            ERROR(f"{insert_files}")
            exit(1)
        datasets_curs = dbQuery( cnxn_string_map[ dbstring ], insert_datasets )
        if datasets_curs:
            datasets_curs.commit()
        else:
            ERROR(f"Failed to insert dataset(s)into database {dbstring}. Line was:")
            ERROR(f"{insert_datasets}")
            exit(1)

# ============================================================================================

# ---------------------------------------------------------------------------------------------
def update_proddb( dbid: int, filestat=None, dryrun=True ):
        ended = datetime.fromtimestamp(filestat.st_ctime) if filestat else str(datetime.now().replace(microsecond=0))
        update_jobs = f"""
update production_jobs
set status='finished', finished='{ended}'
where id={dbid}
;
"""
        CHATTY(update_jobs)
        if not dryrun:
            dbstring = 'statw'
            jobs_curs = dbQuery( cnxn_string_map[ dbstring ], update_jobs )
            if jobs_curs:
                jobs_curs.commit()
            else:
                ERROR(f"Failed to update production_jobs for id={dbid} in database {dbstring}")
                exit(1)

# ============================================================================================
def jobstarted(dbid: int, dryrun: bool = False):
    """
    Marks a job as started in the production database.
    This includes setting the status to 'running', recording the start time,
    and capturing execution node and ProcId from the Condor job ad.
    """
    execution_node = "UNKNOWN"
    proc_id = None
    condor_job_ad_file = os.getenv("_CONDOR_JOB_AD")
    if condor_job_ad_file and os.path.exists(condor_job_ad_file):
        with open(condor_job_ad_file, 'r') as f:
            for line in f:
                if line.startswith("RemoteHost"):
                    val = line.split(" = ", 1)[1].strip().strip('"')
                    # RemoteHost = "slot1@bnl-fn1.local" -> bnl-fn1.local
                    execution_node = val.split('@')[-1]
                elif line.startswith("ProcId"):
                    try:
                        proc_id = int(line.split(" = ", 1)[1].strip())
                    except ValueError:
                        pass

    now = str(datetime.now().replace(microsecond=0))
    set_clauses = [
        "status = 'running'",
        f"started = '{now}'",
        f"execution_node = '{execution_node}'",
    ]
    if proc_id is not None:
        set_clauses.append(f"ProcId = {proc_id}")

    update_jobs_sql = f"""
        UPDATE production_jobs
        SET {', '.join(set_clauses)}
        WHERE id = {dbid};
    """
    DEBUG(update_jobs_sql)
    if not dryrun:
        dbstring = 'statw'
        prodstate_curs = dbQuery(cnxn_string_map[dbstring], update_jobs_sql, maintenance_wait=1500)
        if prodstate_curs:
            prodstate_curs.commit()
        else:
            ERROR(f"Failed to update production_jobs for id={dbid} in database {dbstring}")
            # Do not exit, as the job might still be able to run
            # and we don't want to cause a job failure just for a DB update failure.


def jobended(dbid: int, exit_code: int, dryrun: bool = False):
    """
    Marks a job as ended in the production database.
    The final status is determined by the exit_code. Resource usage metrics
    are read from the Condor job ad if available.
    """
    status = 'finished' if exit_code == 0 else 'failed'
    now = str(datetime.now().replace(microsecond=0))

    # Parse resource metrics from the condor job ad
    ad_floats = {'RemoteUserCpu': None, 'RemoteSysCpu': None}
    ad_ints   = {'MemoryUsage': None, 'DiskUsage': None, 'ExitCode': None}
    condor_job_ad_file = os.getenv("_CONDOR_JOB_AD")
    if condor_job_ad_file and os.path.exists(condor_job_ad_file):
        with open(condor_job_ad_file, 'r') as f:
            for line in f:
                key, _, val = line.partition(" = ")
                key = key.strip()
                val = val.strip().strip('"')
                if key in ad_floats:
                    try: ad_floats[key] = float(val)
                    except ValueError: pass
                elif key in ad_ints:
                    try: ad_ints[key] = int(val)
                    except ValueError: pass

    set_clauses = [
        f"status = '{status}'",
        f"finished = '{now}'",
        f"ExitCode = {exit_code}",
    ]
    for col, val in ad_floats.items():
        if val is not None:
            set_clauses.append(f"{col} = {val}")
    for col, val in ad_ints.items():
        if val is not None and col != 'ExitCode':  # ExitCode already set from arg
            set_clauses.append(f"{col} = {val}")

    update_jobs_sql = f"""
        UPDATE production_jobs
        SET {', '.join(set_clauses)}
        WHERE id = {dbid};
    """
    CHATTY(update_jobs_sql)
    if not dryrun:
        dbstring = 'statw'
        prodstate_curs = dbQuery(cnxn_string_map[dbstring], update_jobs_sql, maintenance_wait=1500)
        if prodstate_curs:
            prodstate_curs.commit()
        else:
            ERROR(f"Failed to update production_jobs for id={dbid} in database {dbstring}")
            # Do not exit, as we want to avoid causing further issues at the end of a job.

def printDbInfo( cnxn_string, title ):
    conn = pyodbc.connect( cnxn_string )
    name=conn.getinfo(pyodbc.SQL_DATA_SOURCE_NAME)
    serv=conn.getinfo(pyodbc.SQL_SERVER_NAME)
    print(f"with {cnxn_string}\n   connected {name} from {serv} as {title}")

# ============================================================================================
def dbQuery( cnxn_string, query, ntries=5, maintenance_wait=0 ):
    """
    Execute a query with retry logic for transient errors.

    Args:
        ntries:            Maximum number of attempts per phase.
        maintenance_wait:  If >0 and all ntries attempts fail, sleep this many seconds
                           (to ride out a DB maintenance window) then retry ntries more
                           times before giving up. Set to 0 (default) for fast failure.
    """
    CHATTY(f'[cnxn_string] {cnxn_string}')
    CHATTY(f'[query      ]\n{query}')

    # SQLSTATE codes that are transient and worth retrying
    retryable_states = {
        '40001',  # serialization failure (deadlock)
        '53300',  # too_many_connections
        '57P03',  # cannot_connect_now (DB starting up)
        '08006',  # connection failure
        '08001',  # unable to establish connection
    }

    def attempt_query(phase):
        for itry in range(ntries):
            try:
                conn = pyodbc.connect( cnxn_string )
                curs = conn.cursor()
                curs.execute( query )
                return curs
            except pyodbc.Error as E:
                state = E.args[0]
                ERROR(f"Phase {phase}, attempt {itry+1}/{ntries} failed: {E}")
                if state in retryable_states:
                    delay = min(60, (2 ** itry) * (0.5 + random.random()))
                    WARN(f"Retrying in {delay:.1f}s...")
                    time.sleep(delay)
                else:
                    ERROR(f"Non-retryable odbc error: {E}")
                    exit(11)
            except Exception as E:
                ERROR(f"Non-retryable error during database query: {E}")
                exit(11)
        return None

    start = datetime.now()
    curs = attempt_query(phase=1)

    if curs is None and maintenance_wait > 0:
        WARN(f"All {ntries} attempts failed. Waiting {maintenance_wait}s for maintenance window to pass...")
        time.sleep(maintenance_wait)
        curs = attempt_query(phase=2)

    if curs is None:
        ERROR(f"Exhausted all attempts. Stop.")
        exit(11)

    CHATTY(f'[query time ] {(datetime.now() - start).total_seconds():.2f} seconds' )
    return curs

# ============================================================================================
def list_to_condition(lst: List[int], name: str="runnumber")  -> str :
    """
    Generates a condition string usable in a SQL query from a list of values.

    This function takes a list (`lst`) and a field name (`name`) and constructs a
    string that can be used as a `WHERE` clause condition in a SQL query.

    Args:
        lst: A list of positive integers. Usually runnumbers.
        name: The name of the field/column in the database (usually runnumber)

    Returns:
        A string representing a (an?) SQL condition, or "" if the list is empty.

    Examples:
        - list_to_condition([123], "runnumber") returns "and runnumber=123", [123]
        - list_to_condition([100, 200], "runnumber") returns "and runnumber>=100 and runnumber<=200", [100, 101, ..., 200]
        - list_to_condition([1, 2, 3], "runnumber") returns "and runnumber in ( 1,2,3 )", [1, 2, 3]
        - list_to_condition([], "runnumber") returns None
    """

    if isinstance(lst,int):
        lst=[ lst ]
    elif isinstance(lst,list):
        pass
    else:
        ERROR(f"list_to_condition: input argument is {type(lst)}")
        exit(1)

    length=len( lst )
    if length==0:
        return ""

    if length>20000:
        ERROR(f"Run list has {length} entries. Not a good idea. Bailing out.")
        exit(-1)

    if length==1:
        return f"{name}={lst[0]}"

    # range?
    if length==2:
        lst=sorted(lst) # fix user error
        return f"{name}>={lst[0]} and {name}<={lst[-1]}"

    # --> list, possibly with gaps
    strlist=map(str,lst)
    return f"{name} in  ( {','.join(strlist)} )"

def main():
    args = get_parser()

    dbid = args.dbid if args.dbid is not None else int(os.getenv("PRODDB_DBID", -1))
    if dbid < 0:
        ERROR("No dbid provided via --dbid and PRODDB_DBID is not set.")
        exit(1)

    if args.command == 'jobstarted':
        jobstarted(dbid, args.dryrun)
    elif args.command == 'jobended':
        jobended(dbid, getattr(args, 'exit_code', 1), args.dryrun)


if __name__ == '__main__':
    main()

