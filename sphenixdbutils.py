import pyodbc
from pathlib import Path
import pprint # noqa: F401

import time
from datetime import datetime
import random
import os

from typing import overload, List, Union
from collections import namedtuple
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
### TODO: ".slurp" is outdated as a name, just using it for backward compatibility
test_mode = ( 
        False
        or 'testbed' in str(Path(".").absolute()).lower()
        or Path(".slurp/testbed").exists() # deprecated
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
    # for key in cnxn_string_map.keys() :
    #     DEBUG(f"Changing {key} to use DSN=eickolja")
    #     cnxn_string_map[key] = 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DSN=eickolja;READONLY=True;UID=eickolja'

# Hack to use local PostgreSQL database from inside a docker container
if Path('/.dockerenv').exists() :
    driverstring='DRIVER=PostgreSQL;SERVER=host.docker.internal;'
    cnxn_string_map = {
        'fcw'         : f'{driverstring}DATABASE=filecatalogdb;UID=eickolja',
        'fcr'         : f'{driverstring}DATABASE=filecatalogdb;READONLY=True;UID=eickolja',
        'statr'       : f'{driverstring}DATABASE=productiondb;READONLY=True;UID=eickolja',
        'statw'       : f'{driverstring}DATABASE=productiondb;UID=eickolja',
        'rawr'        : f'{driverstring}DATABASE=rawdatacatalogdb;READONLY=True;UID=eickolja',
    }

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
            ERROR(f"Failed to insert file {lfn} into database {dbstring}")
            exit(1)
        datasets_curs = dbQuery( cnxn_string_map[ dbstring ], insert_datasets )
        if datasets_curs:
            datasets_curs.commit()
        else:
            ERROR(f"Failed to insert dataset {lfn} into database {dbstring}")
            exit(1)

# ============================================================================================

update_prodstate_tmpl = """
update production_status
set status='{status}', ended='{ended}'
where 
id={dbid}
;
"""
# ---------------------------------------------------------------------------------------------
def update_proddb( dbid: int, filestat=None, dryrun=True ):
        # for "files"
        update_prodstate=update_prodstate_tmpl.format(
            dbid=dbid,
            status='finished',
            ended=datetime.fromtimestamp(filestat.st_ctime) if filestat else str(datetime.now().replace(microsecond=0)),
        )
        CHATTY(update_prodstate)
        if not dryrun:
            dbstring = 'statw'
            prodstate_curs = dbQuery( cnxn_string_map[ dbstring ], update_prodstate )
            if prodstate_curs:
                prodstate_curs.commit()
            else:
                ERROR(f"Failed to update production status for {dbid} in database {dbstring}")
                exit()

# ============================================================================================
def printDbInfo( cnxn_string, title ):
    conn = pyodbc.connect( cnxn_string )
    name=conn.getinfo(pyodbc.SQL_DATA_SOURCE_NAME)
    serv=conn.getinfo(pyodbc.SQL_SERVER_NAME)
    print(f"with {cnxn_string}\n   connected {name} from {serv} as {title}")

# ============================================================================================
def dbQuery( cnxn_string, query, ntries=10 ):

    # Guard rails - should not be needed, because only Readonly connections should be used
    assert( 'delete' not in query.lower() )
    #assert( 'insert' not in query.lower() )
    #assert( 'update' not in query.lower() )    
    #assert( 'select'     in query.lower() )

    CHATTY(f'[cnxn_string] {cnxn_string}')
    CHATTY(f'[query      ]\n{query}')

    start=datetime.now()
    last_exception = None
    ntries = 1
    curs=None
    # Attempt to connect up to ntries
    for itry in range(0,ntries):
        try:
            conn = pyodbc.connect( cnxn_string )
            curs = conn.cursor()
            curs.execute( query )
            break
        except Exception as E:
            ntries = ntries + 1
            last_exception = str(E)
            ERROR(f"Attempt {itry} failed: {last_exception}")
            exit(1)
            delay = (itry + 1 ) * random.random()
            time.sleep(delay)
            DEBUG(f"Attempt {itry} failed: {last_exception}")
    #TODO: Handle connn failure more gracefully
    CHATTY(f'[query time ] {(datetime.now() - start).total_seconds():.2f} seconds' )
    
    return curs
