import pyodbc
import pathlib
import pprint # noqa: F401

import time
import random

from simpleLogger import DEBUG

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
    dsnfiler = 'FileCatalog'
    dsnfilew = 'FileCatalog'    
elif ( test_mode ):
    #print("Found testbed mode")
    dsnprodr = 'ProductionStatus'
    dsnprodw = 'ProductionStatusWrite'
    dsnfiler = 'FileCatalog'
    dsnfilew = 'FileCatalog'
else:
    #print("NOTICE: Neither production nor testbed mode set.  Default to testbed.  YMMV.")
    dsnprodr = 'ProductionStatus'
    dsnprodw = 'ProductionStatusWrite'
    dsnfiler = 'FileCatalog'
    dsnfilew = 'FileCatalog'

# ============================================================================
cnxn_string_map = {
    'fcw'         : f'DSN={dsnfilew};UID=phnxrc',
    'fcr'         : f'DSN={dsnfiler};READONLY=True;UID=phnxrc',
    'statr'       : f'DSN={dsnprodr};UID=argouser',
    'statw'       : f'DSN={dsnprodw};UID=argouser',

    # from slurp.py
    'daq'         :  'DSN=daq;UID=phnxrc;READONLY=True',
    'daqdb'       :  'DSN=daq;UID=phnxrc;READONLY=True',
    'fc'          : f'DSN={dsnfiler};READONLY=True',
    'fccro'       : f'DSN={dsnfiler};READONLY=True',
    'filecatalog' : f'DSN={dsnfiler};READONLY=True',
    'status'      : f'DSN={dsnprodr};UID=argouser',
    'statusw'     : f'DSN={dsnprodw};UID=argouser',
    'raw'         :  'DSN=RawdataCatalog_read;UID=phnxrc;READONLY=True',
    'rawdr'       :  'DSN=RawdataCatalog_read;UID=phnxrc;READONLY=True',
}    

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

    DEBUG(f"[Print cnxn_string] {cnxn_string}")
    DEBUG(f"[Print query      ] {query}")

    # Hack to test on Mac
    import os
    if os.uname().sysname=='Darwin' :
        from simple import sqres
        from cmplx import cqres
        if 'daq' in cnxn_string :
            return cqres
        return sqres

    # Proper db access
    now = time.time()
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
    #TODO: Handle connection failure more gracefully
    DEBUG( f'[Print time       ] {time.time() - now:.2g} seconds' )
    return curs
