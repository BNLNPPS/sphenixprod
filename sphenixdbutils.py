import pyodbc
import pathlib
import pprint # noqa: F401

import time
import random
import os

from simpleLogger import WARN, DEBUG

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
    #print("Found production mode")
    dsnprodr = 'Production_read'
    dsnprodw = 'Production_write'
    dsnfilec = 'FileCatalog'
elif ( test_mode ):
    #print("Found testbed mode")
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
    'statr'       : f'DSN={dsnprodr};UID=argouser',
    'statw'       : f'DSN={dsnprodw};UID=argouser',

    'daqdb'       :  'DSN=daq;UID=phnxrc;READONLY=True',
    'raw'         :  'DSN=RawdataCatalog_read;UID=phnxrc;READONLY=True',
    'rawdr'       :  'DSN=RawdataCatalog_read;UID=phnxrc;READONLY=True',
}
# Hack to test on Mac
if os.uname().sysname=='Darwin' :
    for key in cnxn_string_map.keys() :
        DEBUG(f"Changing {key} to use DSN=eickolja")
        cnxn_string_map[key] = 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DSN=eickolja;;READONLY=True;UID=eickolja'

# ============================================================================================
def printDbInfo( cnxn, title ):
    name=cnxn.getinfo(pyodbc.SQL_DATA_SOURCE_NAME)
    serv=cnxn.getinfo(pyodbc.SQL_SERVER_NAME)
    print(f"Connected {name} from {serv} as {title}")

# ============================================================================================
def dbQuery( cnxn_string, query, ntries=10 ):

    # Guard rails
    assert( 'delete' not in query.lower() )    
    #assert( 'insert' not in query.lower() )    
    #assert( 'update' not in query.lower() )    
    #assert( 'select'     in query.lower() )

    DEBUG(f'[cnxn_string] {cnxn_string}')
    DEBUG(f'[query      ]\n{query}')

    now =time.time()
    lastException = None
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
            lastException = str(E)
            delay = (itry + 1 ) * random.random()
            time.sleep(delay)
            DEBUG(f"Attempt {itry} failed: {lastException}")
    #TODO: Hanquery connn failure more gracefully
    DEBUG(f'[query time ] {time.time() - now:.2g} seconds' )
    
    return curs
