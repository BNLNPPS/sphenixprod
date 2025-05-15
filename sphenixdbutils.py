import pyodbc
import pathlib
import pprint # noqa: F401

import time
import random
import os

from simpleLogger import WARN, DEBUG, ERROR

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
        or 'testbed' in str(pathlib.Path(".").absolute()).lower()
        or pathlib.Path(".slurp/testbed").is_file() # deprecated
        or pathlib.Path(".testbed").is_file()
        or pathlib.Path("SPHNX_TESTBED_MODE").is_file()
    )

prod_mode = pathlib.Path("SPHNX_PRODUCTION_MODE").is_file()
if ( prod_mode ):
    dsnprodr = 'Production_read'
    dsnprodw = 'Production_write'
    dsnfilec = 'FileCatalog'
elif ( test_mode ):
    dsnprodr = 'ProductionStatus'
    dsnprodw = 'ProductionStatusWrite'
    dsnfilec = 'FileCatalog'
else:
    WARN("Neither production nor testbed mode set.  Default to testbed.  YMMV.")
    dsnprodr = 'ProductionStatus'
    dsnprodw = 'ProductionStatusWrite'
    dsnfilec = 'FileCatalog'

# ============================================================================
cnxn_string_map = {
    'fcw'         : f'DSN={dsnfilec};UID=phnxrc',
    'fcr'         : f'DSN={dsnfilec};READONLY=True;UID=phnxrc',
    'statr'       : f'DSN={dsnprodr};READONLY=True;UID=argouser',
    'statw'       : f'DSN={dsnprodw};UID=argouser',
    'rawr'        :  'DSN=RawdataCatalog_read;READONLY=True;UID=phnxrc',
}

# Hack to test locally on Mac
if os.uname().sysname=='Darwin' :
    cnxn_string_map = {
        'fcw'         : 'DSN=filecatalogdb;UID=eickolja',
        'fcr'         : 'DSN=filecatalogdb;READONLY=True;UID=eickolja',
        'statr'       : 'DSN=productiondb;READONLY=True;UID=eickolja',
        'statw'       : 'DSN=productiondb;UID=eickolja',
        'rawr'        : 'DSN=rawdatacatalogdb;READONLY=True;UID=eickolja',
    }
    # for key in cnxn_string_map.keys() :
    #     DEBUG(f"Changing {key} to use DSN=eickolja")
    #     cnxn_string_map[key] = 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DSN=eickolja;READONLY=True;UID=eickolja'

# Hack to use local PostgreSQL database from inside a docker container
if os.path.exists('/.dockerenv') :
    driverstring='DRIVER=PostgreSQL;SERVER=host.docker.internal;'
    cnxn_string_map = {
        'fcw'         : f'{driverstring}DSN=filecatalogdb;UID=eickolja',
        'fcr'         : f'{driverstring}DSN=filecatalogdb;READONLY=True;UID=eickolja',
        'statr'       : f'{driverstring}DSN=productiondb;READONLY=True;UID=eickolja',
        'statw'       : f'{driverstring}DSN=productiondb;UID=eickolja',
        'rawr'        : f'{driverstring}DSN=rawdatacatalogdb;READONLY=True;UID=eickolja',
    }
    # for key in cnxn_string_map.keys() :
    #     DEBUG(f"Changing {key} to use DSN=eickolja")
    #     cnxn_string_map[key] = 'DRIVER=PostgreSQL;SERVER=host.docker.internal;DSN=eickolja;READONLY=True;UID=eickolja'

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
    DEBUG(f'[query      ]\n{query}')

    now=time.time()
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
    #TODO: Hanquery connn failure more gracefully
    DEBUG(f'[query time ] {time.time() - now:.2g} seconds' )
    
    return curs
