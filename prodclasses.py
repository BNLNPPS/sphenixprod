#!/usr/bin/env python

import pyodbc
# import htcondor
# import classad
# import re
# import uuid
# import os
# import pathlib
# import pprint
# import sh
# import argparse
# import datetime
# import time
# import itertools
# from  glob import glob
# import math
# import platform
# from collections import defaultdict
# import random
# import inspect

#from slurp import slurptables
#from slurp.slurptables import SPhnxProductionSetup
#from slurp.slurptables import SPhnxProductionStatus
#from slurp.slurptables import SPhnxInvalidRunList
#from slurp.slurptables import sphnx_production_status_table_def

#import slurptables.SPhnxProductionSetup
#import slurptables.SPhnxProductionStatus
#import slurptables.SPhnxInvalidRunList
#from slurp.slurptables import sphnx_production_status_table_def

from slurptables import SPhnxProductionSetup
from slurptables import SPhnxProductionStatus
from slurptables import SPhnxInvalidRunList


from dataclasses import dataclass, asdict, field

from simpleLogger import DEBUG, INFO, WARN, ERROR, CRITICAL

import logging

SLURPPATH=os.path.dirname( inspect.getfile( SPhnxProductionSetup ) )
pathlib.Path( '.slurp' ).mkdir( exist_ok=True )
with open('.slurp/slurppath.sh', 'w' ) as sp:
    sp.write( f'export SLURPPATH={SLURPPATH}\n' )


# This is the maximum number of DST names / types that will be in production at any one time
MAXDSTNAMES = 100

# List of states which block the job
blocking = ["submitting","submitted","started","running","evicted","failed","finished"]
#blocking = []
args     = None
userargs = None

__frozen__ = True
__rules__  = []

def printDbInfo( cnxn, title ):
    name=cnxn.getinfo(pyodbc.SQL_DATA_SOURCE_NAME)
    serv=cnxn.getinfo(pyodbc.SQL_SERVER_NAME)
    print(f"Connected {name} from {serv} as {title}")

# Check if we are running within a testbed area
PRODUCTION_MODE=False
if pathlib.Path(".slurp/testbed").is_file():
    print("Testbed mode by config")    
    dsnprodr = 'ProductionStatus'
    dsnprodw = 'ProductionStatusWrite'
    dsnfiler = 'FileCatalog'
    dsnfilew = 'FileCatalog'
elif 'testbed' in str(pathlib.Path(".").absolute()).lower():   
    print("Testbed mode by path")
    dsnprodr = 'ProductionStatus'
    dsnprodw = 'ProductionStatusWrite'
    dsnfiler = 'FileCatalog'
    dsnfilew = 'FileCatalog'
else:
    PRODUCTION_MODE=True
    dsnprodr = 'Production_read'
    dsnprodw = 'Production_write'
    dsnfiler = 'FileCatalog'
    dsnfilew = 'FileCatalog'        

    
cnxn_string_map = {
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

if 0:
    pprint.pprint( cnxn_string_map )
    for k,v in cnxn_string_map.items():
        printDbInfo( pyodbc.connect(v), k )

def dbQuery( cnxn_string, query, ntries=10 ):

    # A guard rail
    assert( 'delete' not in query.lower() )    
    #assert( 'insert' not in query.lower() )    
    #assert( 'update' not in query.lower() )    
    #assert( 'select'     in query.lower() )

    lastException = None
    
    # Attempt to connect up to ntries
    for itry in range(0,ntries):
        try:
            conn = pyodbc.connect( cnxn_string )

            if itry>0: printDbInfo( conn, f"Connected {cnxn_string} attempt {itry}" )
            curs = conn.cursor()
            curs.execute( query )
            return curs
                
        except Exception as E:
            lastException = E
            delay = (itry + 1 ) * random.random()
            time.sleep(delay)

    print(lastException)
    exit(0)
            

    

verbose=0

#
# Format strings for run and segment numbers.  n.b. that the "rungroup" which defines the logfile and output file directory structure
# hardcodes "08d" as the run format...  
#
RUNFMT = "%08i"
SEGFMT = "%05i"
DSTFMT = "%s_%s_%s-" + RUNFMT + "-" + SEGFMT + ".root"
DSTFMTv = "%s_%s_%s_%s-" + RUNFMT + "-" + SEGFMT + ".root"

@dataclass
class SPhnxCondorJob:
    """
    Condor submission job template.
    """
    universe:              str = "vanilla"
    executable:            str = f"{SLURPPATH}/jobwrapper.sh"    
    arguments:             str = "$(nevents) $(run) $(seg) $(lfn) $(indir) $(dst) $(outdir) $(buildarg) $(tag) $(ClusterId) $(ProcId)"
    batch_name:            str = "$(name)_$(build)_$(tag)_$(version)"
    output:                str = None 
    error:                 str = None
    log:                   str = f"$(condor)/$(name)_$(build)_$(tag)-$INT(run,{RUNFMT})-$INT(seg,{SEGFMT}).condor"
    periodichold: 	   str = "(NumJobStarts>=1 && JobStatus == 1)"
    priority:              str = "1958"
    job_lease_duration:    str = "3600"
    requirements:          str = '(CPU_Type == "mdc2")';    
    request_cpus:          str = "1"
    request_memory:        str = "$(mem)"
    should_transfer_files: str = "YES"
    output_destination:    str = "file://./output/"
    when_to_transfer_output: str = "ON_EXIT"
    request_disk:          str = None    
    initialdir:            str = None
    accounting_group:      str = None
    accounting_group_user: str = None
    transfer_output_files: str = '""'
    transfer_output_remaps: str = None
    
    transfer_input_files:  str = None
    user_job_wrapper:      str = None
    max_retries:           str = None # No default...
    request_xferslots:     str = None

    transferout:           str = "false"
    transfererr:           str = "false"

    periodicremove:        str = None

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }

    def __post_init__(self):

        pass

@dataclass( frozen= __frozen__ )
class SPhnxRule:
    name:              str            # Name of the rule
    script:            str            # Production script
    build:             str            # Build tag
    tag:               str            # Database tag
    files:             str  = None    # Input files query
    filesdb:           str  = None    # Input files DB to query
    runlist:           str  = None    # Input run list query from daq
    direct:            str  = None    # Direct path to input files (supercedes filecatalog)
    lfn2pfn:           str  = "lfn2pfn"  # could be lfn2lfn
    job:               SPhnxCondorJob = SPhnxCondorJob()
    resubmit:          bool = False   # Set true if job should overwrite existing job
    buildarg:          str  = ""      # The build tag passed as an argument (leaves the "." in place).
    payload:           str = "";      # Payload directory (condor transfers inputs from)
    limit:    int = 0                 # maximum number of matches to return 0=all
    runname:           str = None     # eg run2pp, extracted from name or ...
    version:          str = None     # eg v001 

    def __eq__(self, that ):
        return self.name == that.name
    
    def __post_init__(self):
        # Verify the existence of the production script
        #    ... no guarentee that the script is actually at this default path ...
        #    ... it could be sitting in the intialdir of the job ...
        path_ = ""
        if self.job.initialdir:
            path_ = self.job.initialdir + "/"
        #assert( pathlib.Path( path_ + self.script ).exists() )

        object.__setattr__(self, 'buildarg', self.build)
        b = self.build
        b = b.replace(".","")
        object.__setattr__(self, 'build', b)        

        if self.runname==None:
            object.__setattr__(self, 'runname', self.name.split('_')[-1])

        # Add to the global list of rules
        __rules__.append(self)

        

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }        
        

@dataclass( frozen = __frozen__ )
class SPhnxMatch:
    name:     str = None;        # Name of the matching rule
    script:   str = None;        # The run script
    lfn:      str = None;        # Logical filename that matches
    dst:      str = None;        # Transformed output
    run:      str = None;        # Run #
    seg:      str = None;        # Seg #
    build:    str = None;        # Build
    tag:      str = None;        # DB tag
    mem:      str = None;        # Required memory
    disk:     str = None;        # Required disk space
    payload:  str = None;        # Payload directory (condor transfers inputs from)
    stdout:   str = None; 
    stderr:   str = None; 
    condor:   str = None;
    buildarg: str = None;
    inputs:   str = None;
    ranges:   str = None;
    rungroup: str = None;
    firstevent: str = None;
    lastevent: str = None;
    runs_last_event: str = None;
    neventsper : str = None
    streamname : str = None
    streamfile : str = None
    version: str = None

    def __eq__( self, that ):
        return self.run==that.run and self.seg==that.seg

    def __post_init__(self):

        object.__setattr__(self, 'buildarg', self.build)
        b = self.build
        b = b.replace(".","")
        object.__setattr__(self, 'build', b)                
        run = int(self.run)
        object.__setattr__(self, 'rungroup', f'{100*math.floor(run/100):08d}_{100*math.ceil((run+1)/100):08d}')

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }


#def table_exists( tablename ):
#    """
#    """ 
#    result = False
#    if statusdbr.tables( table=tablename.lower(), tableType='TABLE' ).fetchone():
#        result = True
#    return result



def fetch_production_status( setup, runmn=0, runmx=-1 ):
    """
    Given a production setup, returns the production status table....
    """
    result = [] # of SPhnxProductionStatus

    query = f"select * from production_status where true"
    if ( runmn>runmx ): 
        query = query + f" and run>={runmn}"
    else              : 
        query = query + f" and run>={runmn} and run<={runmx}"

    query=query+";"

    dbresult = dbQuery( cnxn_string_map['statusw'], query )

    #print( cnxn_string_map['statusw'] )
    #print( query )
    #for db in dbresult:
    #    pprint.pprint(db)
    
    # Transform the list of tuples from the db query to a list of prouction status dataclass objects
    result = [ SPhnxProductionStatus( *db ) for db in dbresult ]


    return result

#def fetch_invalid_run_entry( dstname, run, seg ):
#    query = f"""
#    select 
#    ,   id
#    ,   dstname
#    ,   first_run
#    ,   last_run
#    ,   first_segment
#    ,   last_segment
#        expires_at at time zone 'utc' as expires 
#        from invalid_run_list
#    where 
#        (dstname='{dstname}' or dstname='all' or dstname='ALL' ) and first_run<={run} and ( last_run>={run} or last_run=-1 ) and first_segment<={segment} and last_segment>={segment};       
#    """
#
#
#    return [ 
#        SPhnxInvalidRunList(*db) 
#        for db in 
#               statusdbr.execute( query ).fetchall() 
#    ]

def getLatestId( tablename, dstname, run, seg ):

    cache="cups.cache"
    
    # We are limiting to the list of all productions for a given run,segment pair.

    result  = 0
    query=f"""
    select id,dstname from {tablename} where run={run} and segment={seg} order by id desc limit {MAXDSTNAMES};
    """
    # Find the most recent ID with the given dstname

    for r in dbQuery( cnxn_string_map[ 'statusw' ], query ):
        if r.dstname == dstname:
            result = r.id
            break

    # Widen the search if needed...
    if result==0:
        query=f"""
        select id,dstname from {tablename} where run={run} and segment={seg} order by id desc limit {MAXDSTNAMES*10};
        """
        for r in dbQuery( cnxn_string_map[ 'statusw' ], query ):
            if r.dstname == dstname:
                result = r.id
                break

    if result==0:
        print(f"Warning: could not find {dstname} with run={run} seg={seg}... this may not end well.")

    return result

def update_production_status( matching, setup, condor, state ):

    # Condor map contains a dictionary keyed on the "output" field of the job description.
    # The map contains the cluster ID, the process ID, the arguments, and the output log.
    # (This is the condor.stdout log...)
    condor_map = {}
    for ad in condor:
        clusterId = ad['ClusterId']
        procId    = ad['ProcId']
        out       = ad['Out'].split('/')[-1]   # discard anything that looks like a filepath
        ulog      = ad['UserLog'].split('/')[-1] 
        key       = ulog.split('.')[0].lower()  # lowercase b/c referenced by file basename

        condor_map[key]= { 'ClusterId':clusterId, 'ProcId':procId, 'Out':out, 'UserLog':ulog }

    # TODO: version???  does setup get the v000 string or just 0?
    name = sphenix_dstname( setup.name, setup.build, setup.dbtag, setup.version )

    updates = []
    for m in matching:
        run     = int(m['run'])
        segment = int(m['seg'])
        name    = str(m['name'])
        version = m.get('version',None)

        streamname = m.get( 'streamname', None )
        name_ = name
        if streamname:
            name_ = name.replace("$(streamname)",streamname)

        # Does revison need to go into here???  YES YES YES.
        dsttype = name_
        dstname = dsttype +'_'+setup.build.replace(".","")+'_'+setup.dbtag
        if setup.version is not None:
            dstname = dstname + '_' + setup.version
        dstfile = ( dstname + '-' + RUNFMT + '-' + SEGFMT ) % (run,segment)

        key     = dstfile

        try:
            cluster = condor_map[ key.lower() ][ 'ClusterId' ]
            process = condor_map[ key.lower() ][ 'ProcId'    ]
        except KeyError:
            ERROR("Key Error getting cluster and/or process number from the class ads map.")
            ERROR(f"  key={key}")
            ERROR("Assuming this is an issue with condor, setting cluster=0, process=0 and trying to continue...")
            cluster=0
            process=0
        

        # 1s time resolution
        timestamp=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )
        id_ = getLatestId( 'production_status', dstname, run, segment )

        update=f"""
        update  production_status
        set     status='{state}',{state}='{timestamp}',cluster={cluster},process={process}
        where id={id_};
        """
        updates.append( update )

    curs = dbQuery( cnxn_string_map[ 'statusw' ], ';'.join(updates)  )
    curs.commit()

def insert_production_status( matching, setup, cursor ):

    state='submitting'

    # Prepare the insert for all matches that we are submitting to condor
    values = []
    for m in matching:
        run     = int(m['run'])
        segment = int(m['seg'])
        name    = m['name']
        streamname = m.get( 'streamname', None )
        name_ = name
        if streamname:
            name_ = name.replace("$(streamname)",streamname)

        dstfileinput = m['lfn'].split('.')[0]

        if m['inputs']:
            dstfileinput=m['inputs']

        # TODO: version ???
        # TODO: is dstfile and key redundant ???
        version = m.get('version',None)
        dsttype = name_
        dstname = dsttype +'_'+setup.build.replace(".","")+'_'+setup.dbtag
        if version:
            dstname = dstname + "_" + version  
        dstfile = ( dstname + '-' + RUNFMT + '-' + SEGFMT ) % (run,segment)        

        # TODO: version???
        key = sphenix_base_filename( setup.name, setup.build, setup.dbtag, run, segment, version )
        
        prod_id = setup.id

        # Cluster and process are unset during at this pint
        cluster = 0
        process = 0

        status  = state        

        timestamp=str( datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)  )

        # TODO: Handle conflict
        node=platform.node().split('.')[0]

        value = f"('{dsttype}','{dstname}','{dstfile}',{run},{segment},0,'{dstfileinput}',{prod_id},{cluster},{process},'{status}', '{timestamp}', 0, '{node}' )" 

        if streamname:
            value = value.replace( '$(streamname)', streamname )

        values.append( value )
       
    insvals = ','.join(values)    

    # Inserts the production status lines for each match, returning the list of IDs associated with each match.
    insert = f"""
    insert into production_status
           (dsttype, dstname, dstfile, run, segment, nsegments, inputs, prod_id, cluster, process, status, submitting, nevents, submission_host )
    values 
           {insvals}

    returning id
    """

    # TODO: standardized query
    try:
        cursor.execute(insert)    # commit is deferred until the update succeeds
    except Exception as E:
        print(insert)
        raise(E)

    result=[ int(x.id) for x in cursor ]

    return result
    

        


def submit( rule, maxjobs, **kwargs ):

    # Will return cluster ID
    result = 0

    actions = [ "dump" ]

    dump = kwargs.get( "dump", False )
    if dump:
        del kwargs["dump"];

    if args:
        kwargs['resubmit'] = args.resubmit

    # Build list of LFNs which match the input
    matching, setup, runlist = matches( rule, kwargs )

    nmatches = len(matching)
    if len(matching)==0:
        return result

    #
    # Resubmit is only a manual operation.  Existing files must be removed or the DB query adjusted to avoid
    # stomping on previous output files.
    #
    if kwargs.get('resubmit',False):
        reply = None
        while reply not in ['y','yes','Y','YES','Yes','n','N','no','No','NO']:
            reply = "N"
            reply = input("Warning: resubmit option may overwrite previous production.  Continue (y/N)?")
        if reply in ['n','N','No','no','NO']:
            return result
 
    #
    # An unclean setup is also cause for manual intervention.  It will hold up any data production.
    #    (but we will allow override with the batch flag)
    #
    if not ( setup.is_clean and setup.is_current ) and args.batch==False:
        if setup.is_clean  ==False: WARN("Uncommitted local changes detected")
        if setup.is_current==False: WARN("Local repo needs to be updated")

        reply=None
        while reply not in ['y','yes','Y','YES','Yes','n','N','no','No','NO']:
            reply = "N"
            reply = input("Continue (y/N)?")
        if reply in ['n','N','No','no','NO']:
            return result        


    INFO("Get the job dictionary")
    jobd = rule.job.dict()

    # If we are using a wrapper, the user script becomes the first argument
    if jobd['executable']==f'{SLURPPATH}/jobwrapper.sh':
        INFO(f"Setting up general jobwrapper script.  Adding user script {rule.script} as first argument")
        jobd['arguments']= rule.script + ' ' + jobd['arguments']
        INFO(f"  {jobd['arguments']}")

    # Append $(cupsid) as the last argument
    jobd['arguments'] = jobd['arguments'] + ' $(cupsid)'

    leafdir = rule.name.replace( f'_{rule.runname}', "" )
    jobd['arguments'] = jobd['arguments'].replace( '{leafdir}', leafdir )

    # And b/c the condor log is special...
    jobd['log'] = jobd['log'].replace( '{leafdir}', leafdir )


    INFO("Passing job to htcondor.Submit")
    submit_job = htcondor.Submit( jobd )
    if verbose>0:
        INFO(submit_job)
        if verbose>10:
            for m in matching:
                pprint.pprint(m)

    dispatched_runs = []


    #
    # At this point in the code, matching jobs are storred in the array 'matching'.
    # All jobs in this array are ripe for submission.  If maxjobs is defined, this
    # is the point where we can truncate the matches...
    #
    if maxjobs:
        INFO(f"Truncating the number of jobs to maxjobs={maxjobs}")        
        matching = matching[:int(maxjobs)]


    if dump==False:
        if verbose==-10:
            INFO(submit_job)
        
        schedd = htcondor.Schedd()    

        runtypes = {}
        streams  = {}
        # Strip out unused $(...) condor macros
        INFO("Converting matches to list of dictionaries for schedd...")
        mymatching = []
        for m in iter(matching):
            d = {}

            # TODO:  This should be accessed from the run table / daqdb
            runtype='none'
            d['runtype']='unset'
            d['runname']=rule.runname

            leafdir=m["name"].replace(f"_{rule.runname}","")

            # massage the inputs from space to comma separated
            if m.get('inputs',None): 
                m['inputs']= ','.join( m['inputs'].split() )
                if '/physics/' in m['inputs']: # physics can appear twice by mistake...
                    runtype = 'physics'
                    d['runtype']=runtype
                if '/beam/' in m['inputs']: # beam supercedes...
                    runtype = 'beam'
                    d['runtype']=runtype
                if '/cosmics/' in m['inputs']: # beam supercedes...
                    runtype = 'cosmics'
                    d['runtype']=runtype
                if '/calib/' in m['inputs']: # beam supercedes...
                    runtype = 'calib'
                    d['runtype']=runtype

                
            runtypes[runtype]=1 # register the runtype for directory creation below

            if m.get('ranges',None):
                m['ranges']= ','.join( m['ranges'].split() )

            for k,v in m.items():

                if k in ['outdir','logdir','histdir','condor']:
                    m[k] = v.format( **locals() )

                if k in str(submit_job) or k=='streamname': # b/c it may not be declared in the arglist
                    d[k] = m[k]
                    if k=='streamname': streams[ m[k] ] = 1
               
                if args.dbinput: 
                    d['inputs']= 'dbinput'            
                    d['ranges']= 'dbranges'


            mymatching.append(d)        
            dispatched_runs.append( (d['run'],d['seg']) )
                
        run_submit_loop=30
        schedd_query = None

        # Insert jobs into the production status table and add the ID to the dictionary
        INFO("... insert")

        # Grab a cursor
        cursorips = dbQuery( cnxn_string_map['statusw'], 'select id from production_status where false;' )

        # Perform the insert
        cupsids = insert_production_status( matching, setup, cursor=cursorips ) 
        for i,m in zip(cupsids,mymatching):
            m['cupsid']=str(i)

        
        INFO("Preparing to submitting the jobs to condor")
        try:

            INFO("... creating directories if they do not exist")
            for outname in [ 'outdir', 'logdir', 'condor', 'histdir' ]:

                outdir=kwargs.get(outname,None)
                if outdir==None: continue

                outdir = outdir.replace('file:/','')
                outdir = outdir.replace('//','/')

                outdir = outdir.replace( '$(rungroup)', '{rungroup}')
                outdir = outdir.replace( '$(build)',    '{rule.build}' )
                outdir = outdir.replace( '$(tag)',      '{rule.tag}' )
                outdir = outdir.replace( '$(name)',     '{rule.name}' )
                outdir = outdir.replace( '$(version)',  '{rule.version}' )
                outdir = outdir.replace( '$(runname)',  '{rule.runname}' )
                outdir = outdir.replace( '$(runtype)',  '{runtype}' )

                outdir = f'f"{outdir}"'

                rungroups = {}
                madedir = {}
                for run in runlist:
                    mnrun = 100 * ( math.floor(run/100) )
                    mxrun = mnrun+100
                    rungroup=f'{mnrun:08d}_{mxrun:08d}'                

                    for runtype in runtypes.keys():  # runtype is a possible KW in the yaml file that can be substituted
                        targetdir = eval(outdir)

                        if '$(streamname)' in targetdir: # ... 
                            
                            for mystreamname in streams.keys():
                                
                                if madedir.get( targetdir, False )==False:
                                    td =  targetdir.replace('$(streamname)',mystreamname )
                                    pathlib.Path( td ).mkdir( parents=True, exist_ok=True )
                                    INFO(f"mkdir {td}")
                                    madedir[ td ]=True                                
                                
                        else:

                            if madedir.get( targetdir, False )==False:
                                pathlib.Path( eval(outdir) ).mkdir( parents=True, exist_ok=True )            
                                INFO(f"mkdir {eval(outdir)}")
                                madedir[targetdir]=True

            # submits the job to condor
            INFO("... submitting to condor")

            submit_result = schedd.submit(submit_job, itemdata=iter(mymatching))  # submit one job for each item in the itemdata
            # commits the insert done above
            cursorips.commit()

        except:
            # if condor did not accept the jobs, rollback to the previous state and 
            cursorips.rollback()
            raise
            
        INFO("Getting back the cluster and process IDs")
        schedd_query = schedd.query(
            constraint=f"ClusterId == {submit_result.cluster()}",
            projection=["ClusterId", "ProcId", "Out", "UserLog", "Args" ]
        )

        # Update DB IFF we have a valid submission
        INFO("Insert and update the production_status")
        if ( schedd_query ):
            

            INFO("... result")
            result = submit_result.cluster()            

            INFO("... update")
            update_production_status( matching, setup, schedd_query, state="submitted" )


    else:
        order=["script","name","nevents","run","seg","lfn","indir","dst","outdir","buildarg","tag","stdout","stderr","condor","mem"]           
        with open( "submit.job", "w" ) as f:
            f.write( str(submit_job) )
            line = "queue ";
            for k in order:
                line += str(k) + ", "
            line += "from submit.in\n"
            f.write(line)
            
        with open( "submit.in", "w" ) as f:
            for m in matching:
                line = []
                for k in order:                    
                    line.append( str(m[k]) )
                line = ','.join(line)                
                f.write(line+"\n")

    return dispatched_runs

def fetch_production_setup( name_, build, dbtag, repo, dir_, hash_, version=None ):
    """
    Fetches the production setup from the database for the given (name,build,dbtag,hash).
    If it doesn't exist in the DB it is created.  Queries the git repository to verify 
    that the local repo is clean and up to date with the remote.  Returns production setup
    object.

    If provided, we look for a user-specified version number.

    """

    # version is a string of the form v000... we need to convert in some places...
    version_ = int(version.replace('v',''))

    name=name_
    if '$(streamname)' in name:
        name = name.replace('$(streamname)','_X_')

    result = None # SPhnxProductionSetup

    query = ""
    if version is None:
        query="""
        select id,hash from production_setup 
               where name='%s'  and 
                   build='%s' and 
                   dbtag='%s' and 
                   hash='%s'
                   limit 1;
        """%( name, build, dbtag, hash_ )
    else:


        
        query="""
        select id,hash from production_setup 
               where name='%s'  and 
                   build='%s' and 
                   dbtag='%s' and 
                   hash='%s'  and
                   revision=%i        
                   limit 1;
        """%( name, build, dbtag, hash_, version_ )        
    
    array = [ x for x in dbQuery( cnxn_string_map['statusw'], query ) ]
    assert( len(array)<2 )

    if   len(array)==0:
        insert=""
        if version is None:
            insert="""
            insert into production_setup(name,build,dbtag,repo,dir,hash)
                   values('%s','%s','%s','%s','%s','%s');
            """%(name,build,dbtag,repo,dir_,hash_)
        else:
            insert="""
            insert into production_setup(name,build,dbtag,repo,dir,hash,revision)
                   values('%s','%s','%s','%s','%s','%s',%i);
            """%(name,build,dbtag,repo,dir_,hash_,version_)

        curs = dbQuery( cnxn_string_map['statusw'], insert )
        curs.commit()

        result = fetch_production_setup(name, build, dbtag, repo, dir_, hash_, version)

    elif len(array)==1:

        # Check to see if the payload has any local modifications
        is_clean = len( sh.git("-c","color.status=no","status","-uno","--short",_cwd=dir_).strip().split('\n') ) == 0;

        # git show origin/main --format=%h -s
        remote_hash = sh.git("show","origin","--format=%h","-s", _cwd=dir_).strip()
        is_current = (hash_ == remote_hash)

        id_ = int( array[0][0] )
        old_hash = str( array[0][1] )
        is_current = ( hash_ == old_hash and remote_hash == old_hash )

        # We reach this point in the code under two conditions... 1) the production_setup
        # was found in the DB, or 2) this is a recursive call after we just created
        # the setup.  So...
        #
        # We can return the setup based on the ID provided in the DB.
        # We can return a setup based on the arguments passed to the function b/c
        # 1) If it did not exist in the DB already, it was just created
        # 2) OR it exists... the repo and local directories do not matter... but if the hash
        #    has changed it is a problem...  
        # Should issue a warning before submitting in case clean or current is violated...
        result=SPhnxProductionSetup( id_, name, build, dbtag, repo, dir_, hash_, is_clean, is_current, version )

    return result


def sphenix_dstname( dsttype, build, dbtag, version=None ):
    if type(version)==int:
        version_ = f'v{version:03d}'
    else:
        version_ = version
    result = '_'.join( [x for x in [dsttype, build, dbtag, version_] if x] )
    return result

def sphenix_base_filename( dsttype, build, dbtag, run, segment, version=None ):
    if type(version)==int:
        version_ = f'v{version:03d}'
    else:
        version_ = version    
    result = ("%s-" + RUNFMT + "-" + SEGFMT) % ( sphenix_dstname(dsttype, build, dbtag, version), int(run), int(segment) )
    return result
    

def matches( rule, kwargs={} ):
    """
    
    Apply rule... extract files from DB according to the specified query
    and build the matches.  Return list of matches.  Return the production
    setup from the DB.
    
    """
    global args

    result = []

    name      = kwargs.get('name',      rule.name)       # Can we handle multiple names (eg DST_STREAMING_EVENT_TPCnn_run2pp) in a single submission?
    build     = kwargs.get('build',     rule.build)      # TODO... correct handling from submit.  build=ana.xyz --> build=anaxyz buildarg=ana.xyz
    buildarg  = kwargs.get('buildarg',  rule.buildarg)
    tag       = kwargs.get('tag',       rule.tag)
    script    = kwargs.get('script',    rule.script)
    resubmit  = kwargs.get('resubmit',  rule.resubmit)
    payload   = kwargs.get('payload',   rule.payload)
    update    = kwargs.get('update',    True ) # update the DB

    version  = rule.version

    outputs = []

    # Build list of possible outputs from filelist query... (requires run,sequence as 2nd and 3rd
    # elements in the query result)
    fc_result  = []

    rl_result = None
    rl_map    = None

    lfn_lists  = {}  # LFN lists per run requested in the input query
    pfn_lists  = {}  # PFN lists per run existing on disk
    rng_lists  = {}  # LFN:firstevent:lastevent

    runMin=999999
    runMax=0
    INFO("Building candidate inputs")

    dstnames = {}

    if rule.files:
        # curs      = cursors[ rule.filesdb ]

        inputquery = dbQuery( cnxn_string_map[ rule.filesdb ], rule.files )

        outputs = [] # WARNING: len(outputs) and len(fc_result) must be equal

        # This will hold the list of datasets which are present in the input query
        input_datasets = {}

        # Matches the dsttype runtype 
        regex_dset = re.compile( '(DST_[A-Z0-9_]+_[a-z0-9]+)_([a-z0-9]+_(\d\d\d\dp\d\d\d|nocdbtag))_*(v\d\d\d)*' )
        
        INFO(f"... {len(fc_result)} inputs")
        for f in inputquery:
            fc_result.append(f) # cache the query
            run     = f.runnumber
            segment = f.segment
            runsegkey = f"{run}-{segment}"

            streamname = getattr( f, 'streamname', None )
            name_ = name
            if streamname:
                name_ = name.replace( '$(streamname)',streamname ) # hack in condor replacement
                runsegkey = f"{run}-{segment}-{streamname}"


            # This is where the candidate output filename is built...  
            output_ = DSTFMT %(name_,build,tag,int(run),int(segment))

            if version:
                output_ = DSTFMTv %(name_,build,tag,str(version),int(run),int(segment))

            outputs.append( output_ )

            dstnames[ f"{name_}_{build}_{tag}" ] = (f'{name_}',f'{build}_{tag}')


            if run>runMax: runMax=run
            if run<runMin: runMin=run

            if lfn_lists.get(run,None) == None:
                lfn_lists[ runsegkey ] = f.files.split()
                rng_lists[ runsegkey ] = getattr( f, 'fileranges', '' ).split()
            else:
                # If we hit this result, then the db query has resulted in two rows with identical
                # run numbers.  Violating the implicit submission schema.
                ERROR(f"Run number {runsegkey} reached twice in this query...")
                ERROR(rule.files)
                exit(1)

            #
            # Drop the run and segment numbers and leading stuff and just pull the datasets.  Note.  When
            # we switch up to versioning of the files, this will sweep up the version number as well.
            # Do we want version to be part of the dataset, or a separate entity on its own?
            #
            # Additionally... we can no longer rely on just doing a split here UNLESS we are planning to
            # have a complete break with backwards compatability... The dataset convention goes from
            #
            # anaIII_202JpKKK --> anaIII_202JpKKK_vMMM
            #
            # I can use a regex here instead.  But do we need to?  Do we want to?  I could see us making
            # a complete break here... so that the old naming convention is just simply dropped dropped dropped
            # and we reprocess.
            #
            # ... but we don't need to build this if we are using direct lookup
            if rule.direct==None:            

                for fn in f.files.split():
                    base1 = fn.split('-')[0]
                    rematch = regex_dset.match( base1 )
                    dset = rematch.group(1)
                    dtype = rematch.group(2)
                    vnum = rematch.group(4)
                    if vnum:
                        dtype = dtype + '_' + vnum
                    input_datasets[ ( dset, dtype ) ] = 1
                        
    
    if len(lfn_lists)==0: return [], None, []  # Early exit if nothing to be done

    #
    # Build dictionary of DSTs existing in the datasets table of the file catalog.  For every DST that is in this list,
    # we know that we do not have to produce it if it appears w/in the outputs list.
    #
    dsttype="%s_%s_%s"%(name,build,tag)  # dsttype aka name above
    
    exists = {}
    INFO(f"Building list of existing outputs: # dstnames={len(dstnames.items())}")

    for buildnametag, tuple_ in dstnames.items():
        dt, ds = tuple_
        exists.update( 
            { 
                c.filename : ( c.runnumber, c.segment ) for c in 
                dbQuery( cnxn_string_map['fccro'], f"select filename, runnumber, segment from datasets where runnumber>={runMin} and runnumber<={runMax} and dsttype='{dt}' and dataset='{ds}'" )
            }
        )
    INFO(f"... {len(exists.keys())} existing outputs")



    #
    # lfn2pfn provides a mapping between physical files on disk and the corresponding lfn
    # (i.e. the pfn with the directory path stripped off).
    #
    # It is possible to run sPHENIX software such that it is responsible for looking up the
    # pfn... in which case we can omit building the lfn2pfn mapping and pass down just the
    # logical filenames.  (Note that this requires that there can only be a 1:1 mapping
    # between LFN and PFN)...
    #    
    # A few notes.  This mapping will not be constrained to the set of input files provided
    # by the query.  It will either be all files in the direct search path specified in the
    # yaml file, OR it will be all files contained in the input data set(s).
    #
    # When running from the direct path there is the risk of duplicate entries... ymmv.
    #
    # When running from the file catalog... this will pull in all lfn/pfns from the
    # datasets which appear in the input datasets.  We could extend this to use a
    # tuple as the key, where the tuple is the dataset, dsttype pair.
    #
    lfn2pfn = {}
    if rule.direct:
        INFO(f"Building lfn2pfn map from filesystem {rule.direct}")
        lfn2pfn = { pfn.split("/")[-1] : pfn for pfn in glob(rule.direct+'/*') }
        INFO(f"done {len(lfn2pfn)}")

    else:

        INFO("Building lfn2pfn map from filecatalog")

        for mydatasettuple in input_datasets.keys():

            mydataset=mydatasettuple[1]
            mydsttype=mydatasettuple[0]

            INFO( f'lfn map query for {mydataset} {mydsttype}' )

            fcquery=f"""

            with lfnlist as (
   
            select filename from datasets where 

            runnumber>={runMin}   and 
            runnumber<={runMax}   and 
            dataset='{mydataset}' and 
            dsttype='{mydsttype}'

            )

            select lfn,full_file_path as pfn from 

            lfnlist join files

            on lfnlist.filename=files.lfn;        
            """

            if rule.lfn2pfn=="lfn2pfn":
                lfn2pfn.update( { r.lfn : r.pfn for r in dbQuery( cnxn_string_map['fccro'],fcquery ) } )
            elif rule.lfn2pfn=="lfn2lfn":
                lfn2pfn.update( { r.lfn : r.lfn } )

                    
    # Build lists of PFNs available for each run
    INFO(f"Building PFN lists {len(lfn_lists)}")
    for runseg,lfns in lfn_lists.items():

        lfns_ = [ f"'{x}'" for x in lfns ]
        list_of_lfns = ','.join(lfns_)

        # Add a new entry in the pfn_lists lookup table
        if pfn_lists.get(runseg,None)==None:
            pfn_lists[runseg]=[]

        # Build list of PFNs via direct lookup and append the results
        try:
            pfn_lists[runseg] = [lfn2pfn[lfn] for lfn in lfns] 

        except KeyError:
            print( "No PFN for all LFNs in the input query.")
            print(f"direct_path: {str(rule.direct)}")
            print( "    ... if it is None, you should specify the directory paths where input files can be found")
            print( "        in the input query.")
            print( "    ... if it specifies one or more directories, then your list is incomplete, or there are missing input files.")
            raise KeyError

    INFO(f"... {len(pfn_lists.keys())} pfn lists")

    # 
    # The production setup will be unique based on (1) the specified analysis build, (2) the specified DB tag,
    # and (3) the hash of the local github repository where the payload scripts/macros are found.
    #
    repo_dir  = payload 
    repo_hash = sh.git('rev-parse','--short','HEAD',_cwd=payload).rstrip()
    repo_url  = sh.git('config','--get','remote.origin.url',_cwd=payload ).rstrip()  # TODO: fix hardcoded directory


    if PRODUCTION_MODE:
        # git branch --show-current
        localbranch = sh.git( 'branch', '--show-current', _cwd=payload ).strip()

        #localhash = sh.git('show','--format=%H','-s','--no-abbrev-commit',_cwd=payload).strip()[:40]
        localhash    = sh.git('rev-parse','HEAD', _cwd=payload).strip()[:40]
        remotehashes = [ f[:40] for f in sh.git('rev-list','--all',f'origin/{localbranch}', _cwd=payload).split('\n') ]

        if localhash.strip() in remotehashes:
            INFO( f"Local and remote hash match in the payload directory {localhash}.  You may proceed." )
        elif build=='new': 
            INFO( f"Local hash not found in remote {localhash} ... we are running under new, so go for it!" )
        elif args.doit:
            WARN("The darkside is a pathway to many abilities that some consider unnatural...")
        else:
            WARN( f"""

            YOU ARE IN A PRODUCTION ENVIRONMENT.

            Local hash DOES NOT match any hash on the remote for the payload directory.

            {localhash}
            
            In order to ensure reproducibility of results we require that the payload area is under 
            version control (git), and that the local hash is found in the remote repo.
            
            If you need to test a small change, you should place them on a branch.  (Do a git stash, 
            create the new branch, do a git stash pop and add your codes to the branch.  Push to
            the remote and run your jobs).

            If you are making a physics-analysis-meaningful change that needs to be tracked, consider 
            also incrementing the version number of the production and reproducing the data sample.
                        
            """ )
            exit(0)

    else:

        WARN("You are running in testbed mode... so no consistency with the remote is required.")


    

    # Question is whether the production setup can / should have name replacement with the input stream.  
    # Perhaps a placeholder substitution in the fetch / update / create methods.
    #
    # Version???
    #
    INFO("Fetching production setup")
    setup = fetch_production_setup( name, buildarg, tag, repo_url, repo_dir, repo_hash, version )
    
    #
    # Returns the production status table from the database
    #
    if runMin>runMax:
        runMax=999999
        runMin=0

    INFO("Fetching production status")
    prod_status = fetch_production_status ( setup, runMin, runMax )  # between run min and run max inclusive

    #
    # Map the production status table onto the output filename.  We use this map later on to determine whether
    # the proposed candidate output DST in the outputs list is currently being produced by a condor job, or
    # has failed and needs expert attention.
    #
    prod_status_map = {}
    INFO("Building production status map")    
    for stat in prod_status:
        prod_status_map[stat.dstfile] = stat.status 

    INFO("Production status map")

    #
    # Build the list of matches.  We iterate over the fc_result zipped with the set of proposed outputs
    # which derives from it.  Keep a list of all runs we are about to submit.
    #
    list_of_runs = []
    INFO("Building matches")

    assert( len(fc_result)==len(outputs) ) 
    for (fc,dst) in zip(fc_result,outputs):


        lfn = fc.source
        run = fc.runnumber
        seg = fc.segment
        firstevent = getattr(fc,'firstevent',None)
        lastevent  = getattr(fc,'lastevent',None)
        runs_last_event = getattr(fc,'runs_last_event',None)
        streamname = getattr(fc,'streamname',None)
        streamfile = getattr(fc,'streamfile',None)

        if firstevent: firstevent=str(firstevent)
        if lastevent: lastevent=str(lastevent)
        if runs_last_event: runs_last_event=str(runs_last_event)
        if streamname: streamname=str(streamname)
        if streamfile: streamfile=str(streamfile)

        neventsper = getattr(fc,'neventsper',None)

        runsegkey = f"{run}-{seg}"
        if streamname:
            runsegkey = f"{run}-{seg}-{streamname}"
                
        #
        # Get the production status from the proposed output name
        #
        # TODO: Shouldn't we replace all suffixes here?
        #
        x    = dst.replace(".root","").strip()
        stat = prod_status_map.get( x, None )

        #
        # There is a master list of states which result in a DST producion job being blocked.  By default
        # this is (or ought to be) the total list of job states.  Jobs can end up failed, so there exist
        # options to ignore the a blocking state... which will remove it from the blocking list.
        #
        if stat in blocking:
            if args.batch==False:           WARN("%s is blocked by production status=%s, skipping."%( dst, stat ))
            continue
        
        #
        # Next we check to see if the job has alread been produced (i.e. it is registered w/in the file catalog).
        # If it exists, we will not reproduce the DST.  Unless the resubmit option overrides.
        #
        test=exists.get( dst, None )
        if test and not resubmit:
            if args.batch==False:           WARN("%s has already been produced, skipping."%dst)
            continue

        #
        # Check consistentcy between the LFN list (from input query) and PFN list (from file catalog query) 
        # for the current run.  Verify that the two lists are consistent.
        #
        num_lfn = len( lfn_lists[ runsegkey ] )
        num_pfn = len( pfn_lists[ runsegkey ] )
        sanity = True
        pfn_check = [ x.split('/')[-1] for x in pfn_lists[runsegkey] ]
        for x in pfn_check:
            if x not in lfn_lists[ runsegkey ]:
                sanity = False
                break

        #
        # If there are more LFNs requested than exist on disk, OR if the lfn list does
        # not match the pfn list, then reject.
        #
        if num_lfn > num_pfn or sanity==False:
            WARN(f"LFN list and PFN list are different.  Skipping this run {runsegkey}")
            WARN( f"{num_lfn} {num_pfn} {sanity}" )
            for i in itertools.zip_longest( lfn_lists[runsegkey], pfn_lists[runsegkey] ):
                print(i)
            continue


        inputs_ = pfn_lists[ runsegkey ]
        ranges_ = rng_lists[ runsegkey ]
        

        #
        # If the DST has been produced (and we make it to this point) we issue a warning that
        # it will be overwritten.
        #
        if test and resubmit:
            WARN("%s exists and will be overwritten"%dst)

        if True:

            if verbose>10:
                INFO (lfn, run, seg, dst, "\n");

            myinputs = None
            myranges = None

            if inputs_:
                myinputs = ' '.join(inputs_) ### ??????

            if ranges_:
                myranges = ' '.join(ranges_)

            
            # 
            # Build the rule-match data structure and immediately convert it to a dictionary.
            #    

            match = SPhnxMatch(
                name,                   # name of the DST, e.g. DST_CALO
                script,                 # script which will be run on the worker node
                lfn,                    # lfn of the input file
                dst,                    # name of the DST output file
                str(run),               # run number
                str(seg),               # segment number
                buildarg,               # sPHENIX software build (preserve the "." when building the match)
                tag,                    # database tag.
                "4096MB",               # default memory requirement
                "10GB",                 # default disk space requirement
                payload,                # payload directory
                inputs=myinputs,        # space-separated list of input files
                ranges=myranges,        # space-separated list of input files with first and last event separated by :
                firstevent=firstevent,
                lastevent=lastevent,
                runs_last_event=runs_last_event,
                neventsper=neventsper,
                streamname=streamname,
                streamfile=streamfile,
                version=version
                )

            match = match.dict()

            #
            # Add / override with kwargs.  This is where (for instance) the memory and disk requirements
            # can be adjusted.
            #
            for k,v in kwargs.items():
                match[k]=str(v)              # coerce any ints to string
            
            result.append(match)

            if int(run) not in list_of_runs: list_of_runs.append(run)

            #
            # Terminate the loop if we exceed the maximum number of matches
            #
            if rule.limit and len(result)>= rule.limit:
                break    

    INFO(f"Matched {len(result)} jobs to the rule")

    return result, setup, list_of_runs

#__________________________________________________________________________________________________
#
arg_parser = argparse.ArgumentParser()    
arg_parser.add_argument( "--batch", default=False, action="store_true",help="Batch mode...")
arg_parser.add_argument( '-u', '--unblock-state', nargs='*', dest='unblock',  choices=["submitting","submitted","started","running","evicted","failed","finished"] )
arg_parser.add_argument( '-r', '--resubmit', dest='resubmit', default=False, action='store_true', 
                         help='Existing filecatalog entry does not block a job')

arg_parser.add_argument( "--dbinput", default=True, action="store_true",help="Passes input filelist through the production status db rather than the argument list of the production script." )
arg_parser.add_argument( "--no-dbinput", dest="dbinput", action="store_false",help="Unsets dbinput flag." )

arg_parser.add_argument( "--batch-name", dest="batch_name", default=None ) #default="$(name)_$(build)_$(tag)_$(version)"
arg_parser.add_argument( "--doit", dest="doit", action="store_true", default=False )

def warn_options( args, userargs ):
    if args.dbinput==False:
        WARN("Option --no-dbinput is deprecated, and will be retired in the future.  All workflows should become db aware.")

def parse_command_line():
    global blocking
    global args
    global userargs

    args, userargs = arg_parser.parse_known_args()
    #blocking_ = ["submitting","submitted","started","running","evicted","failed","finished"]

    warn_options( args, userargs )

    if args.unblock:
        blocking = [ b for b in blocking if b not in args.unblock ]

    return args, userargs

        

        
        
