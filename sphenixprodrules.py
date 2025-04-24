import yaml
import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Any, Optional
import itertools
import operator

import pathlib
import pprint # noqa: F401

from sphenixdbutils import cnxn_string_map, dbQuery
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixjobdicts import InputsFromOutput

from collections import namedtuple
FileStreamRunSeg = namedtuple('FileStreamRunSeg',['filename','streamname','runnumber','segment'])

""" This file contains the dataclasses for the rule configuration and matching.
    It encapsulates what is tedious but hopefully easily understood instantiation
    from a YAML file, with some values being changed or completed by command line arguments of the caller.
    RuleConfig  represents a single rule configuration.
    InputConfig represents the input configuration sub-block.
    JobConfig   represents the job configuration sub-block.

    The MatchConfig is the steering class for db queries to 
    find appropriate input files and name the output files.
    It is constructed from a RuleConfig object.

    IMPORTANT: The only interesting business logic in this file is in the MatchConfig.matches method.

"""

# Striving to keep Dataclasses immutable (frozen=True)
# All modifications and utions should be done in the constructor

# ============================================================================
# shared format strings and default filesystem paths
RUNFMT = '%08i'
SEGFMT = '%05i'
VERFMT = '03d'
pRUNFMT = RUNFMT.replace('%','').replace('i','d')
pSEGFMT = SEGFMT.replace('%','').replace('i','d')


# "{leafdir}" needs to stay changeable.Typical leafdir: DST_STREAMING_EVENT_TPC20 or DST_TRKR_CLUSTER 
# Target example: 
# /sphenix/lustre01/sphnxpro/production/$(period) / $(runtype) / $(build)_$(tag)_$(version) / {leafdir} / run_$(rungroup)/dst
# /sphenix/lustre01/sphnxpro/production/ run3auau  /   cosmics  / new_nocdbtag_v000          / {leafdir} / run_00057900_00058000/dst
_default_filesystem = {
        'outdir'  :           "/sphenix/lustre01/sphnxpro/{prodmode}/{period}/{mode}/{lfnsnippet}/{leafdir}/run_$(rungroup)/dst"
    ,   'logdir'  : "file:///sphenix/data/data02/sphnxpro/{prodmode}/{period}/{mode}/{lfnsnippet}/{leafdir}/run_$(rungroup)/log"
    ,   'histdir' :        "/sphenix/data/data02/sphnxpro/{prodmode}/{period}/{mode}/{lfnsnippet}/{leafdir}/run_$(rungroup)/hist"
    ,   'condor'  :                                 "/tmp/{prodmode}/{period}/{mode}/{lfnsnippet}/{leafdir}/run_$(rungroup)/log"    
}

# ============================================================================
def check_params(params_data: Dict[str, Any], required: List[str], optional: List[str] ) -> bool:
    """
    Check that all required parameters are present, and no unexpected ones.
    """
    check_clean = True
    for f in required:
        if f not in params_data:
            check_clean = False
            raise ValueError(f"Missing required field '{f}'.")
    # Have to iterate over a copy since we are deleting fields
    for f in params_data.copy():
        if f not in optional + required:
            WARN( f"Unexpected field '{f}' in params. Removing, but you should clean up the yaml")
            # raise ValueError(f"Unexpected field '{f}'.")
            check_clean = False
            del params_data[f]
    return check_clean

# ============================================================================================
def extract_numbers_to_commastring(filepath):
    """
    Extracts all numbers from a file, combines them into a comma-separated string,
    and returns the string. Numbers can be separated by whitespace including newlines.

    Args:
        filepath: The path to the file.

    Returns:
        A string containing a comma-separated list of numbers, or None if the file
        does not exist or no numbers are found.
    """
    try:
        with open(filepath, 'r') as file:
            content = file.read()
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None

    # Find all integer numbers. Could catch mistakes better
    numbers = re.findall(r"[-+]?\d+", content)
    return ','.join(numbers) if numbers else None
        
# ============================================================================================
def list_to_condition(lst, name) :
    """
    Generates a condition string usable in a SQL query from a list of values.

    This function takes a list (`lst`) and a field name (`name`) and constructs a 
    string that can be used as a `WHERE` clause condition in a SQL query. It 
    handles different list lengths to create appropriate conditions.
    No effort is made to ensure that inputs are numbers and properly ordered.

    Args:
        lst: A list of values supplied via CLI (run numbers or segment numbers).
        name: The name of the field/column in the database

    Returns:
        A string representing a SQL-like condition, or None if the list is empty.

    Examples:
        - list_to_condition([123], "runnumber") returns "and runnumber=123"
        - list_to_condition([100, 200], "runnumber") returns "and runnumber>=100 and runnumber<=200"
        - list_to_condition([1, 2, 3], "runnumber") returns "and runnumber in ( 1,2,3 )"
        - list_to_condition([], "runnumber") returns None
    """
    condition = ""
    if  len( lst )==1:
        condition = f"and {name}={lst[0]}"
    elif len( lst )==2:
        condition = f"and {name}>={lst[0]} and {name}<={lst[1]}"
    elif len( lst )>=3 :
        condition = f"and {name} in ( %s )" % ','.join( lst )
    else: 
        return None
    
    return condition

# ============================================================================
@dataclass( frozen = True )
class InputConfig:
    """Represents the input configuration block in the YAML."""
    db: str
    table: str
    prodIdentifier: Optional[str] = None # run3auau, run3cosmics
    
    query_constraints: Optional[str] = None  # Additional constraints for the query
    direct_path: Optional[str] = None  # Make direct_path optional

    ## Query can dynamically use any field that's in params (via format(**params))
    # Powerful but dangerous, so enforce explicit (but optional) fields that users can use
    # Adding to them then becomes is a more conscious decision
    # This used to contain things like mnrun, mxrun
    # All those are now removed, but if we need them back, those parameters should go here

# ============================================================================

@dataclass( frozen = True )
class JobConfig:
    """Represents the job configuration block in the YAML."""
    script: str         # run script on the worker node
    arguments: str
    output_destination: str ### needed? used?
    log: str
    neventsper: int     # number of events per job
    payload: str        # Working directory on the node; transferred by condor
    rsync: str          # additional files to rsync to the node
    mem: str            # "4000MB"
    comment: str        # arbitrary comment
    priority: str
    accounting_group: str = 'group_sphenix.mdc2'
    accounting_group_user: str = 'sphnxpro'
    batch_name: Optional[str] = None

# ============================================================================
@dataclass( frozen = True )
class RuleConfig:
    """Represents a single rule configuration in the YAML."""

    # Direct input (explictly provided by yaml file + command line arguments)
    rulestem: str       # DST_CALOFITTING
    period: str         # run3auau
    build: str          # for output; ex. ana.472
    dbtag: str          # for output; ex. 2025p000, nocdbtag 
    version: int        # for output; ex. 0, 1, 2. Can separate repeated productions
    ## Note that optional basic fields are further down! (e.g. outstub, resubmit)

    # Inferred
    build_string: str
    version_string: str
    logbase: str
    outbase: str

    # Nested dataclasses
    inputConfig: InputConfig
    jobConfig:   JobConfig

    ### Optional fields have to be down here to allow for default values
    outstub: str         # e.g. run3cosmics for 'DST_STREAMING_EVENT_%_run3cosmics' in run3auau root directory
    filesystem: dict     # base filesystem paths
    # resubmit: bool = False

    # ------------------------------------------------
    def dict(self) -> Dict[str, Any]:
        """Convert to a dictionary, handling nested dataclasses."""
        data = asdict(self)
        data['input'] = asdict(self.inputConfig)
        data['job']   = asdict(self.jobConfig)
        return data

    # ------------------------------------------------
    @classmethod
    def from_yaml(cls, yaml_data: Dict[str, Any], rule_name: str, rule_substitions=None) -> "RuleConfig":
        """
        Constructs a RuleConfig object from a YAML data dictionary.

        Args:
            yaml_data: The dictionary loaded from the YAML file.
            rule_name: The name of the rule to extract from the YAML.
            rule_substitions: A dictionary (usually originating from argparse) to override the YAML data and fill in placeholders.

        Returns:
            A RuleConfig object.
        """
        try:
            rule_data = yaml_data[rule_name]
        except KeyError:
            raise ValueError(f"Rule '{rule_name}' not found in YAML data.")

        ### Extract and validate top level rule parameters
        params_data = rule_data.get("params", {})
        check_params(params_data
                    , required=["rulestem", "period",  "build", "dbtag", "version"]
                    , optional=["outstub", "filesystem"] )
        
        
        ### Fill derived data fields
        build_string=params_data["build"].replace(".","")
        version_string = f'v{params_data["version"]:{VERFMT}}'
        lfnsnippet = f'{build_string}_{params_data["dbtag"]}_{version_string}' # internal variable
        outstub    = params_data["outstub"] if "outstub" in params_data else params_data["period"]
        outbase    = f'{params_data["rulestem"]}_{outstub}'
        if "STREAMING" in params_data["rulestem"]:
            outbase += "_STREAMNAME"  # Replace with streamname later
        outbase += f"_{lfnsnippet}"
        if 'DST' in rule_substitions:
            outbase=outbase.replace('DST',rule_substitions['DST'])
            DEBUG(f"outbase is mangled to {outbase}")
        logbase    = outbase+f"-$INT(run,{RUNFMT})-$INT(seg,{SEGFMT})".format(RUNFMT=RUNFMT,SEGFMT=SEGFMT)
        DEBUG(f"outbase is {outbase}")
        DEBUG(f"logbase is {logbase}")

        ### Optionals
        comment = params_data.get("comment", "")
        # Default filesystem paths contain placeholders for substitution
        filesystem = rule_data.get("filesystem")
        if filesystem is None:
            DEBUG("Using default filesystem paths")
            filesystem = _default_filesystem

        for key in filesystem:
            filesystem[key]=filesystem[key].format(   prodmode=rule_substitions["prodmode"]
                                                    , period=params_data["period"]
                                                    , mode=rule_substitions["mode"] 
                                                    , lfnsnippet=lfnsnippet
                                                    , leafdir='{leafdir}'
                                                    )
            DEBUG(f"Filesystem: {key} is {filesystem[key]}")
        
        ###### Now handle InputConfig and JobConfig 
        ### Extract and validate input
        input_data = rule_data.get("input", {})
        check_params(input_data
                    , required=["db", "table"]
                    , optional=["direct_path"] )

        # Rest of the input substitutions, like database name and direct path
        # DEBUG (f"Using database {rule.inputConfig.db}")
        input_direct_path = input_data.get("direct_path")
        if input_direct_path is not None and "mode" in rule_substitions:
            input_direct_path = input_direct_path.format(**rule_substitions)
            DEBUG (f"Using direct path {input_direct_path}")

        if input_data.get("prodIdentifier") is None:
            prodIdentifier = outstub

        input_query_constraints = input_data.get("query_constraints", "")
        input_query_constraints += rule_substitions.get("input_query_constraints")        

        ### Extract and validate job
        job_data = rule_data.get("job", {})
        check_params(job_data
                    , required=[
                    "script", "payload", "neventsper", "rsync", "mem",
                    "arguments", "output_destination","log","priority",
                    ]
                    , optional=["batch_name","accounting_group","accounting_group_user",
                ] )

        ### Add to transfer list
        rsync       = job_data["rsync"] + rule_substitions["append2rsync"]        
        neventsper  = job_data["neventsper"]
        comment     = job_data.get("comment", "")
    
        # Substitute rule_substitions into the job data

        for field in 'batch_name', 'arguments','output_destination','log':
            subsval = job_data.get(field)
            print(f"Field {field} is {subsval}")
            if isinstance(subsval, str): # don't try changing None or dictionaries
                subsval = subsval.format( 
                          **rule_substitions
                        , outbase=outbase
                        , logbase=logbase
                        , **filesystem
                        , PWD=pathlib.Path(".").absolute()
                        , **params_data
                        , rsync=rsync
                        , comment=comment
                        , neventsper=neventsper
                )
                job_data[field] = subsval

        ### With all preparations done, construct the constant RuleConfig object
        return cls(
            rulestem=params_data["rulestem"],
            period=params_data["period"],
            outstub=params_data.get("outstub"),
            build=params_data.get("build"),
            dbtag=params_data.get("dbtag"),
            version=params_data["version"],
            build_string=build_string,
            version_string=version_string,
            logbase=logbase,
            outbase=outbase,
            inputConfig=InputConfig(
                    db=input_data["db"],
                    table=input_data["table"],
                    direct_path=input_direct_path,
                    prodIdentifier= prodIdentifier,
                    query_constraints=input_query_constraints,
            ),
            filesystem=filesystem,
            jobConfig=JobConfig(
                script=job_data["script"],
                payload=job_data["payload"],
                neventsper=neventsper,
                rsync=job_data["rsync"],
                mem=job_data["mem"],
                comment=comment,
                batch_name=job_data.get("batch_name"),  # batch_name is optional
                arguments=job_data["arguments"],
                output_destination=job_data["output_destination"],
                log=job_data["log"],
                accounting_group=job_data.get("accounting_group", 'group_sphenix.mdc2'),
                accounting_group_user=job_data.get("accounting_group_user", 'sphnxpro'),
                priority=job_data["priority"],
            ),
            # dataset=params_data.get("dataset"),
            # resubmit=rule_substitions.get("resubmit", False), # Get resubmit from caller's CLI arguments
        )

    # ------------------------------------------------
    @classmethod
    def from_yaml_file(cls, yaml_file: str, rule_name: str, rule_substitions=None) -> "RuleConfig":
        """
        Constructs a dictionary of RuleConfig objects from a YAML file.

        Args:
            yaml_file: The path to the YAML file.

        Returns:
            A RuleConfig objects, keyed by rule name.
        """
        try:
            with open(yaml_file, "r") as stream:
                yaml_data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise ValueError(f"Error parsing YAML file: {exc}")
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML file not found: {yaml_file}")

        # Could build dictionary where keys are rule names and values are RuleConfig objects.
        # rules = {}
        # for rule_name in yaml_data:
        #     rules[rule_name] = cls.from_yaml(yaml_data, rule_name, rule_substitions)
        # return rules
        return cls.from_yaml(yaml_data, rule_name, rule_substitions)
        
# ============================================================================

@dataclass( frozen = True )
class MatchConfig:
    # From RuleConfig
    rulestem:       str = None         # Name of the matching rule (e.g. DST_CALO)
    outbase:        str = None         # Name base of the output file, contains all production info and possibly STREAMNAME to be replaced
    inputConfig: InputConfig = None

    # Created
    lfn:       str = None         # Logical filename that matches
    dst:       str = None         # Transformed output
    run:       str = None         # Run #
    seg:       str = None         # Seg #
    diskspace: str = "10GB"         # Required disk space

    inputs:   str = None
    ranges:   str = None
    firstevent: str = None
    lastevent: str = None

    stdout:   str = None 
    stderr:   str = None 
    condor:   str = None
    rungroup: str = None
    runs_last_event: str = None
    neventsper : str = None
    streamname : str = None
    streamfile : str = None

    # ------------------------------------------------
    # TODO: Handle "rungroup" which defines the logfile and output file directory structure
    # hardcodes "08d" as the run format
    # original in __post_init__ of MatchConfig:
    # object.__setattr__(self, 'rungroup', f'{100*math.floor(run/100):08d}_{100*math.ceil((run+1)/100):08d}')
    @classmethod
    def from_rule_config(cls, rule_config: RuleConfig):
        """
        Constructs a MatchConfig object partially from a RuleConfig object.

        Args:
            rule_config: The RuleConfig object to extract data from.

        Returns:
            A MatchConfig object with fields pre-populated from the RuleConfig.
        """

        # Formatted version number, needed to identify repeated new_nocdb productions, 0 otherwise
        return cls(
            rulestem=rule_config.rulestem,
            outbase = rule_config.outbase,
            inputConfig = rule_config.inputConfig,
        )

    # ------------------------------------------------
    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }
                
    # ------------------------------------------------
    def matches (self) :
        # Replacement for the old logic
        # TODO: function will be named sensibly and potentially split up

        # TODO: add to sanity check
        #  payload should definitely be part of the rsync list but the yaml does that explicitly instead, e.g.
        #  payload :   ./ProdFlow/run3auau/streaming/
        #  rsync   : "./ProdFlow/run3auau/streaming/*"

        # TODO: Find the right class to store this
        # update    = kwargs.get('update',    True ) # update the DB
        # updateDb= not args.submit

        INFO('Checking for already existing output...')
        
        ### Match parameters are set, now build up the list of inputs and construct corresponding output file names
        # Despite the "like" clause, this is a very fast query. Extra cuts or substitute cuts like
        # 'and runnumber>={self.runMin} and runnumber<={self.runMax}'
        # can be added if the need arises.
        # Note: If the file database is not up to date, this can be replaced by
        # a filesystem search in the output directory
        # Note: db in the yaml is for input, all output gets logged to the FileCatalog
        outTemplate = self.outbase.replace( 'STREAMNAME', '%' )
        existQuery  = f"""select filename, -1 as streamname,runnumber,segment from datasets where filename like '{outTemplate}%'"""
        existQuery += self.inputConfig.query_constraints

        # We can use various attributes to get the info we need, most straightforward is to use the filename
        # Full info with alreadyHave = [ FileStreamRunSeg(c.filename,c.streamname, c.runnumber,c.segment) for c in dbQuery( cnxn_string_map['fcr'], existQuery ) ]
        alreadyHave = [ c.filename for c in dbQuery( cnxn_string_map['fcr'], existQuery ) ]
        INFO(f"Already have {len(alreadyHave)} output files")
        if len(alreadyHave) > 0 :
            CHATTY(f"First line: \n{alreadyHave[0]}")

        ###### Now get all existing input files
        INFO("Building candidate inputs...")
        InputStem = InputsFromOutput[self.rulestem]
        DEBUG( f'Input files are of the form:\n{pprint.pformat(InputStem)}')
          
        if isinstance(InputStem, dict):
            inTypes = list(InputStem.values())
        else :
            inTypes = InputStem

        # Manipulate the input types to match the database
        if 'raw' in self.inputConfig.db:
            descriminator='hostname'
            inTypes.insert(0,'gl1daq') # all raw daq files need an extra GL1 file
        else:
            descriminator='dsttype'
            inTypes = [ f'{t}_{self.inputConfig.prodIdentifier}' for t in inTypes ]
                    
        # Transform list to ('<v1>','<v2>', ...) format. (one-liner doesn't work in python 3.9)
        inTypes = f'( QUOTE{"QUOTE,QUOTE".join(inTypes)}QUOTE )'
        inTypes = inTypes.replace("QUOTE","'")

        infilequery = f'select filename,{descriminator} as streamname,runnumber,segmentplaceholder as segment from {self.inputConfig.table} where \n\t{descriminator} in {inTypes}\n'
        infilequery += self.inputConfig.query_constraints
        if 'raw' in self.inputConfig.db: # Raw daq uses sequence instead
            infilequery=infilequery.replace('segmentplaceholder','sequence')
            infilequery+="\tand transferred_to_sdcc='t'"
        else:
            infilequery=infilequery.replace('segmentplaceholder','segment')

        DEBUG(f"Input file query is:\n{infilequery}")
        dbresult = dbQuery( cnxn_string_map[ self.inputConfig.db ], infilequery ).fetchall()
        # TODO: Support rule.printquery
        inFiles = [ FileStreamRunSeg(c.filename,c.streamname,c.runnumber,c.segment) for c in dbresult ]

        INFO(f"Total number of available input files: {len(inFiles)}")
        if len(inFiles) > 0 :
            DEBUG(f"First line: \n{inFiles[0]}")
        
        #### Now build up potential output files from what's available
        ruleMatches = {}
        #### Key on runnumber
        inFiles.sort(key=lambda x: (x.runnumber)) # itertools.groupby depends on data being sorted
        filesByRun = {k : list(g) for k, g in itertools.groupby(inFiles, operator.attrgetter('runnumber'))}
        runlist = list(filesByRun.keys())
        DEBUG(f'All available runnumbers:{runlist}')
        if len(filesByRun) > 0 :
            CHATTY(f"First line: \n{filesByRun[next(iter(filesByRun))]}")

        for runnumber in runlist:
            candidates = [ f for f in inFiles if f.runnumber == runnumber ]
            if len(candidates) == 0 :
                # By construction of runlist, every runnumber now should have at least one file
                ERROR(f"No input files found for run {runnumber}. That should not happen at this point. Aborting.")
                exit(-1)
            DEBUG(f"Found {len(candidates)} input files for run {runnumber}.")
            CHATTY(f"First line: \n{candidates[0]}")
            
            ######## Cut up the candidates into streams
            candidates.sort(key=lambda x: (x.runnumber, x.streamname)) # itertools.groupby depends on data being sorted
            FilesForRun = { k : list(g) for 
                           k, g in itertools.groupby(candidates, operator.attrgetter('streamname')) }
            # Remove the files we just processed. May be useful to shorten the search space
            # for the next iteration. Could also be a waste of time
            inFiles = [ f for f in inFiles if f.runnumber != runnumber ] 
            
            # daq file lists all need GL1 files. Pull them out and add them to the others
            if ( 'gl1daq' in inTypes ):
                gl1files = FilesForRun.pop('gl1daq',None)
                if gl1files is None:
                    WARN(f"No GL1 files found for run {runnumber}. Skipping this run.")
                    continue
                CHATTY(f'All GL1 files for for run {runnumber}:\n{gl1files}')
                for stream in FilesForRun:
                    FilesForRun[stream] = gl1files + FilesForRun[stream]
                        
            ######### The next step gets a bit hairy.
            # - Easy: If the input has a segment number, then the output will have the same segment number
            #     - These are downstream objects (input is already a DST)
            #     - This can be 1-1 or many-to-1 (usually 2-1 for SEED + CLUSTER --> TRACKS)
            # - Medium: The input has no segment number but each output is connected to one input stream
            #     - This is currently the case for the streaming daq (tracking)
            #     - In this case, provide ALL input files for the run, and the output will produce its own segment numbers
            # - Hard: The input has no segment number and the output is connected to multiple input streams
            #     - This is currently the case for the triggered daq (calos). 
            #     - There may be some intricate mixing needed to get the right event numbers together. 
            #       - For now, we ignore this case because it may soon be changed to medium difficulty

            # Easy case. One way to identify this case is to see if gl1 is not needed
            # In this case, the inputs should be a plain list
            if 'gl1daq' not in inTypes:
                if isinstance(InputStem, dict):
                    ERROR( "InputStem is a dictionary. Only supported for specific productions from raw daq.")
                    exit(-1)
                CHATTY(f'\nInputStem is a list, {self.rulestem} is the output base, and {descriminator} selected/enumerates \n{inTypes}\nas input')

                # For every segment, there is exactly one output file, and exactly one input file from each stream
                # Sort and group the input files by segment
                # NOTE: We could save a small bit of work by not checking the input files 
                # if the output already exists. But we need the segments for that anyway
                segments = None
                rejected = set()
                for stream in FilesForRun:
                    FilesForRun[stream].sort(key=lambda x: (x.segment))
                    newsegments = list(map(lambda x: x.segment, FilesForRun[stream]))
                    if segments is None:
                        segments = newsegments
                    elif segments != newsegments:
                        rejected.update( set(segments).symmetric_difference(set(newsegments)) )
                        segments = list(set(segments).intersection(newsegments))                    

                if len(rejected) > 0:
                    WARN(f"Run {runnumber}: Removed segments not present in all streams: {rejected}")
    
                # If the output doesn't exist yet, use input files to create the job
                for seg in segments:
                    output = f"{self.outbase}-{runnumber:{pRUNFMT}}-{seg:{pSEGFMT}}.root"
                    if output in alreadyHave:
                        CHATTY(f"Output file {output} already exists. Not submitting.")
                        continue

                    infiles= []
                    for stream in FilesForRun:
                        infiles += [ f.filename for f in FilesForRun[stream] if f.segment == seg ]
                    CHATTY(f"Creating {output} from {infiles}")
                    ruleMatches[output] = infiles

        return ruleMatches
# ============================================================================
