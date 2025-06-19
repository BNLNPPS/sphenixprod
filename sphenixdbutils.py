import pyodbc
from pathlib import Path
import pprint # noqa: F401

import time
from datetime import datetime
import random
import os

from collections import namedtuple
filedb_info = namedtuple('filedb_info', ['dsttype','run','seg','fullpath','nevents','first','last','md5'])

from simpleLogger import WARN, ERROR, DEBUG, INFO, CHATTY # noqa: F401

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

insert_files_tmpl="""
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
# ---------------------------------------------------------------------------------------------
insert_datasets_tmpl="""
insert into {datasets_table} (filename,runnumber,segment,size,dataset,dsttype,events)
values ('{lfn}',{run},{segment},{file_size_bytes},'{dataset}','{dsttype}',{nevents})
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
# ---------------------------------------------------------------------------------------------
def upsert_filecatalog(lfn: str, info: filedb_info, full_file_path: str, dataset: str, filestat=None, dryrun=True ):
        # for "files"
        insert_files=insert_files_tmpl.format(
            files_table='test_files' if test_mode else 'files',
            lfn=lfn,
            md5=info.md5,
            full_host_name = "lustre" if 'lustre' in full_file_path else 'gpfs',
            full_file_path = full_file_path,
            ctimestamp = datetime.fromtimestamp(filestat.st_ctime) if filestat else str(datetime.now().replace(microsecond=0)),
            file_size_bytes = filestat.st_size if filestat else 'NONE'
        )
        CHATTY(insert_files)
        insert_datasets=insert_datasets_tmpl.format(
            datasets_table='test_datasets' if test_mode else 'datasets',
            lfn=lfn,
            md5=info.md5,
            run=info.run, segment=info.seg,
            file_size_bytes=filestat.st_size if filestat else 'NONE',
            dataset=dataset,
            dsttype=info.dsttype,
            nevents=info.nevents
        )
        CHATTY(insert_datasets)
        if not dryrun:
            dbstring = 'testw' if test_mode else 'fcw'
            files_curs = dbQuery( cnxn_string_map[ dbstring ], insert_files )
            files_curs.commit()
            datasets_curs = dbQuery( cnxn_string_map[ dbstring ], insert_datasets )
            datasets_curs.commit()

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
            prodstate_curs.commit()

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

    DEBUG(f'[cnxn_string] {cnxn_string}')
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
    DEBUG(f'[query time ] {(datetime.now() - start).total_seconds():.2f} seconds' )
    
    return curs
