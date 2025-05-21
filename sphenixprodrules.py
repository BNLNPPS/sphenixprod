import yaml
import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Any, Optional
import itertools
import operator
import time
from pathlib import Path
import stat
import subprocess
import pprint # noqa: F401
import os

from sphenixdbutils import cnxn_string_map, dbQuery
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixjobdicts import inputs_from_output
from sphenixcondorjobs import CondorJobConfig

from collections import namedtuple
FileHostRunSegStat = namedtuple('FileHostRunSeg',['filename','daqhost','runnumber','segment','status'])

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


# "{leafdir}" needs to stay changeable.  Typical leafdir: DST_STREAMING_EVENT_TPC20 or DST_TRKR_CLUSTER
# "{rungroup}" needs to stay changeable. Typical rungroup: run_00057900_00058000
# Target example:
# /sphenix/lustre01/sphnxpro/{prodmode} / {period}  / {runtype} / dataset={build}_{dbtag}_{version} / {leafdir}       /     {rungroup}       /dst
# /sphenix/lustre01/sphnxpro/production / run3auau  /  cosmics  /        new_nocdbtag_v000          / DST_CALOFITTING / run_00057900_00058000/dst
_default_filesystem = {
    'outdir'   :    "/sphenix/lustre01/sphnxpro/{prodmode}/dstlake/{period}/{physicsmode}/",
    'finaldir' :    "/sphenix/lustre01/sphnxpro/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/dst",
    'logdir'   : "/sphenix/data/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/log",
    'histdir'  : "/sphenix/data/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/hist",
    'condor'   :                          "/tmp/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/log",
}

if 'minicondor' in os.uname().nodename or 'local' in os.uname().nodename: # Mac 
    _default_filesystem = {
        'outdir'  : "/Users/eickolja/sphenix/lustre01/sphnxpro/{prodmode}/dstlake/{period}/{physicsmode}/",
        'finaldir': "/Users/eickolja/sphenix/lustre01/sphnxpro/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/dst",
        'logdir'  :   "/Users/eickolja/sphenix/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/log",
        'histdir' :   "/Users/eickolja/sphenix/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/hist",
        'condor'  :               "/Users/eickolja/sphenix/tmp/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/log",
    }


# ============================================================================
def is_executable(file_path):
  """
    Checks if a file is executable.

    Args:
        file_path (str or Path): The path to the file.

    Returns:
        bool: True if the file is executable, False otherwise.
  """
  path = Path(file_path)
    
  if not path.is_file():
    return False

  st = path.stat()
  return bool(st.st_mode & stat.S_IXUSR or
              st.st_mode & stat.S_IXGRP or
              st.st_mode & stat.S_IXOTH)

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
    prod_identifier:            Optional[str] = None # run3auau, run3cosmics

    file_query_constraints:     Optional[str] = None  # Additional constraints for the filecatalog query
    status_query_constraints:   Optional[str] = None  # Additional constraints for the production catalog query
    direct_path: Optional[str]                = None  # Make direct_path optional

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

    # Inferred
    build_string: str   # ana472, new
    version_string: str # v000
    dataset: str        # new_2025p000_v000

    # Nested dataclasses
    input_config: InputConfig
    job_config:   CondorJobConfig

    ### Optional fields have to be down here to allow for default values
    physicsmode: str     # cosmics, commissioning, physics (default: physics)
    outstub: str         # run3cosmics for 'DST_STREAMING_EVENT_%_run3cosmics' in run3auau root directory (default: period)

    # ------------------------------------------------
    def dict(self) -> Dict[str, Any]:
        """Convert to a dictionary, handling nested dataclasses."""
        data = asdict(self)

        data['input'] = asdict(self.input_config)
        data['job']   = asdict(self.job_config)
        return data

    # ------------------------------------------------
    @classmethod
    def from_yaml(cls,
                  yaml_file: str, #  Used for paths
                  yaml_data: Dict[str, Any],
                  rule_name: str,
                  rule_substitions=None) -> "RuleConfig":
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

        if rule_substitions is None:
            WARN("No rule substitutions provided. Using empty dictionary bnut this may fail.")
            rule_substitions = {}

        ### Extract and validate top level rule parameters
        params_data = rule_data.get("params", {})
        check_params(params_data
                    , required=["rulestem", "period",  "build", "dbtag", "version"]
                    , optional=["outstub", "physicsmode"] )

        ### Fill derived data fields
        build_string=params_data["build"].replace(".","")
        version_string = f'v{params_data["version"]:{VERFMT}}'
        outstub = params_data["outstub"] if "outstub" in params_data else params_data["period"]
        dataset = f'{build_string}_{params_data["dbtag"]}_{version_string}'

        ### outbase and logbase can depend on the leaf (aka stream or host) name.
        ### Until end of April, we had things like leaf TPC20 =~ daqhost ebdc20, 
        ### but going forward the two may become equal or only differ in capitalization
        ### Details are handled in the sphenixjobdicts module. 
        ### Using the database field names where possible, we have
        # leafdir = f'{params_data['rulestem']}' # DST_STREAMING_EVENT
        # if 'TRIGGERED' in dsttype or 'STREAMING' in dsttype:
        #     leafdir += f'_{leafname}' # DST_STREAMING_EVENT_TPC20 <-- not yet known
        # dsttype = f'{leafdir}_{outstub}' # DST_STREAMING_EVENT_TPC20_run3cosmics
        # With that, output, log, etc, live in
        ## [somebase]/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/[dst,log,hist]/filename
        ## where filename = f'{dsttype}_{dataset}-$INT(run,{RUNFMT})-$INT(seg,{SEGFMT})[.root, .log, .hist]'

        ### Turning off name mangling; directory mangling is sufficient
        # if 'DST' in rule_substitions:
        #     outbase=outbase.replace('DST',rule_substitions['DST'])
        #     DEBUG(f"outbase is mangled to {outbase}")

        ### Optionals
        physicsmode = params_data.get("physicsmode", "physics")
        physicsmode = rule_substitions.get("physicsmode", physicsmode)
        comment = params_data.get("comment", None)

        ###### Now create InputConfig and CondorJobConfig
        # Extract and validate input_config
        input_data = rule_data.get("input", {})
        check_params(input_data
                    , required=["db", "table"]
                    , optional=["direct_path", "prod_identifier", "file_query_constraints","status_query_constraints","physicsmode"] )

        # Substitutions in direct input path, if given
        input_direct_path = input_data.get("direct_path")
        if input_direct_path is not None:
            input_direct_path = input_direct_path.format(mode=physicsmode)
            DEBUG (f"Using direct path {input_direct_path}")

        prod_identifier = input_data.get("prod_identifier",outstub)
        
        # Allow arbitrary query constraints to be added
        file_query_constraints  = input_data.get("file_query_constraints", "")
        file_query_constraints += rule_substitions.get("file_query_constraints", "")
        status_query_constraints = input_data.get("status_query_constraints", "")
        status_query_constraints += rule_substitions.get("status_query_constraints", "")
        DEBUG(f"Input query constraints: {file_query_constraints}" if file_query_constraints!= "" else  None)
        DEBUG(f"Status query constraints: {status_query_constraints}" if status_query_constraints!= "" else  None)
        input_config=InputConfig(
                db=input_data["db"],
                table=input_data["table"],
                direct_path=input_direct_path,
                prod_identifier= prod_identifier,
                file_query_constraints=file_query_constraints,
                status_query_constraints=status_query_constraints
        )

        # Extract and validate job_config
        job_data = rule_data.get("job", {})
        check_params(job_data
                    , required=[
                    "script", "payload", "neventsper", "payload", "mem",
                    "arguments", "log","priority",
                    ]
                    , optional=["batch_name", "comment","filesystem", 
                        # "accounting_group","accounting_group_user",
                    ]
                 )

        # Payload code etc. Prepend by the yaml file's path unless they are direct
        yaml_path = Path(yaml_file).parent.resolve()            
        payload_list = job_data["payload"] + rule_substitions.get("payload_list",[])
        for i,loc in enumerate(payload_list):
            if not loc.startswith("/"):
                payload_list[i]= f'{yaml_path}/{loc}'
        DEBUG(f'List of payload items is {payload_list}')

        # # Filesystem paths contain placeholders for substitution
        filesystem = job_data.get("filesystem")
        if filesystem is None:
            INFO("Using default filesystem paths")
            filesystem = _default_filesystem
        else:
            WARN("Using custom filesystem paths from YAML file")

        # Partial substitutions are possible, but not easier to read
        # from functools import partial; s = partial("{foo} {bar}".format, foo="FOO"); print(s(bar ="BAR"))
        for key in filesystem:
            filesystem[key]=filesystem[key].format( prodmode=rule_substitions["prodmode"],
                                                    period=params_data["period"],
                                                    physicsmode=physicsmode,
                                                    dataset=dataset,
                                                    leafdir='{leafdir}',
                                                    rungroup='{rungroup}',
                                                    )
            DEBUG(f"Filesystem: {key} is {filesystem[key]}")
            #Path(filesystem[key]).mkdir( parents=True, exist_ok=True )

        # Note: If you use globs in the payload list,
        # the executable will (almost certainly) copy those sub-files and subdirectories individually to the working directory
        # But we must have a fully qualified working path to the executable at submission time
        # because that script will execute the actual payload copy on the node.
        # Bit of an annoying walk with python tools, so use unix find instead
        script = job_data["script"]
        if not script.startswith("/"): # Search in the payload
            p = subprocess.Popen(f'/usr/bin/find {" ".join(payload_list)} -type f',
                                 shell=True, # needed to expand "*"
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            allfiles = stdout.decode(errors='ignore').split()
            errfiles = stderr.decode(errors='ignore').splitlines()
            for f in allfiles:
                if script == Path(f).name:
                    script = f
                    break
        INFO(f'Full path to script is {script}')
        if not Path(script).exists() :
            ERROR(f"Executable {script} does not exist")
            exit(1)
        if not is_executable(Path(script)):
            ERROR(f"{script} is not executable")
            exit(1)
        if len(errfiles)>0 :
            WARN("The following errors occured while searching the payload:")
            for errf in errfiles:
                WARN(errf)

        neventsper   = job_data["neventsper"]
        comment      = job_data.get("comment", None)

        # Partially fill rule_substitions into the job data
        for field in 'batch_name', 'arguments','log':
            subsval = job_data.get(field)
            if isinstance(subsval, str): # don't try changing None or dictionaries
                subsval = subsval.format(
                          **rule_substitions
                        , **filesystem
                        , **params_data
                        , payload=",".join(payload_list)
                        , comment=comment
                        , neventsper=neventsper
                        , buildarg=build_string
                        , tag=params_data["dbtag"]
                        , dataset=dataset
                        # pass remaining per-job parameters forward to be replaced later
                        , outbase='{outbase}'
                        , logbase='{logbase}'
                        , run='{run}'
                        , seg='{seg}'
                        , daqhost='{daqhost}'
                        , inputs='{inputs}'
                )
            job_data[field] = subsval
            CHATTY(f"After substitution, {field} is {subsval}")

            job_config=CondorJobConfig(
                executable=script,
                request_memory=job_data["mem"],
                request_disk=job_data.get("request_disk", "10GB"),
                comment=comment,
                neventsper=neventsper,
                priority=job_data["priority"],
                batch_name=job_data.get("batch_name"),
                arguments_tmpl=job_data["arguments"],
                log_tmpl=job_data["log"],
                filesystem=filesystem,
            )

        ### With all preparations done, construct the constant RuleConfig object
        return cls(
            rulestem=params_data["rulestem"],   # Use direct access for required fields
            period=params_data["period"],
            outstub=params_data.get("outstub"), # Use get for optional fields
            build=params_data["build"],
            dbtag=params_data["dbtag"],
            version=params_data["version"],
            build_string=build_string,
            version_string=version_string,
            dataset=dataset,
            input_config=input_config,
            job_config=job_config,
            physicsmode=physicsmode,
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
            with open(yaml_file, "r") as yamlstream:
                yaml_data = yaml.safe_load(yamlstream)
        except yaml.YAMLError as exc:
            raise ValueError(f"Error parsing YAML file: {exc}")
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML file not found: {yaml_file}")

        return cls.from_yaml(yaml_file=yaml_file,
                             yaml_data=yaml_data,
                             rule_name=rule_name,
                             rule_substitions=rule_substitions)

# ============================================================================

@dataclass( frozen = True )
class MatchConfig:
    rulestem:      str
    input_config:  InputConfig
    outstub:       str
    dataset:       str
    physicsmode:   str

    # ------------------------------------------------
    @classmethod
    def from_rule_config(cls, rule_config: RuleConfig):
        """
        Constructs a MatchConfig object partially from a RuleConfig object.

        Args:
            rule_config: The RuleConfig object to extract data from.

        Returns:
            A MatchConfig object with fields pre-populated from the RuleConfig.
        """

        return cls(
            rulestem     = rule_config.rulestem,
            input_config = rule_config.input_config,
            outstub      = rule_config.outstub,
            dataset      = rule_config.dataset,
            physicsmode  = rule_config.physicsmode,
        )

    # ------------------------------------------------
    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }

    # ------------------------------------------------
    def matches(self) :

        INFO('Checking for already existing output...')
        ### Match parameters are set, now build up the list of inputs and construct corresponding output file names
        # Despite the "like" clause, this is a fast query. Extra cuts or substitute cuts like
        # 'and runnumber>={self.runMin} and runnumber<={self.runMax}'
        # can be added if the need arises.
        # Note: If the file database is not up to date, we can use a filesystem search in the output directory
        # Note: The db field in the yaml is for input queries only, all output queries go to the FileCatalog
        dst_type_template = f'{self.rulestem}'
        # This test should be equivalent: if 'TRIGGERED' in self.rulestem or 'STREAMING' in self.rulestem:
        if 'raw' in self.input_config.db:
            dst_type_template += '_%'
        dst_type_template += f'_{self.outstub}' # DST_STREAMING_EVENT_%_run3auau
        dst_type_template += '_%'
        # Files to be created are checked against this list. Could use various attributes but most straightforward is just the filename
        # exist_query  = f"""select filename, -1 as daqhost,runnumber,segment from datasets where dataset='{self.dataset}' and dsttype like '{dst_type_template}'"""
        ## Note: dataset='{self.dataset}' is not needed but may speed up the query 
        exist_query  = f"""select filename from datasets where dataset='{self.dataset}' and dsttype like '{dst_type_template}'"""
        exist_query += self.input_config.file_query_constraints
        existing_output = [ c.filename for c in dbQuery( cnxn_string_map['fcr'], exist_query ) ]
        INFO(f"Already have {len(existing_output)} output files")
        if len(existing_output) > 0 :
            CHATTY(f"First line: \n{existing_output[0]}")

        ### Check production status
        INFO('Checking for output already in production...')
        status_query  = f"""select dstfile,status from production_status where dstname like '{dst_type_template}'"""
        status_query += self.input_config.status_query_constraints
        existing_status = { c.dstfile if c.dstfile.endswith('.root') else c.dstfile+".root" : c.status for c in dbQuery( cnxn_string_map['statr'], status_query ) }
        INFO(f"Already have {len(existing_status)} output files in the production db")
        if len(existing_status) > 0 :
            CHATTY(f"First line: \n{next(iter(existing_status))}")

        ####################################################################################
        ###### Now get all existing input files
        ####################################################################################
        INFO("Building candidate inputs...")
        input_stem = inputs_from_output[self.rulestem]
        DEBUG( f'Input files are of the form:\n{pprint.pformat(input_stem)}')

        if isinstance(input_stem, dict):
            in_types = list(input_stem.values())
        else :
            in_types = input_stem

        # Manipulate the input types to match the database
        if 'raw' in self.input_config.db:
            descriminator='daqhost'
            in_types.insert(0,'gl1daq') # all raw daq files need an extra GL1 file
        else:
            descriminator='dsttype'
            in_types = [ f'{t}_{self.input_config.prod_identifier}' for t in in_types ]

        # Transform list to ('<v1>','<v2>', ...) format. (one-liner doesn't work in python 3.9)
        in_types_str = f'( QUOTE{"QUOTE,QUOTE".join(in_types)}QUOTE )'
        in_types_str = in_types_str.replace("QUOTE","'")

        # Need status==1 for all files in a given run,host combination
        # Easier to check that below than in SQL
        infile_query = f'select filename,{descriminator} as daqhost,runnumber,segment,status from {self.input_config.table} where \n\t{descriminator} in {in_types_str}\n'
        infile_query += self.input_config.file_query_constraints

        if 'raw' in self.input_config.db:
            #infile_query+= f" and filename like '%bbox%{self.physicsmode}%'"
            infile_query+= f" and dataset='{self.physicsmode}'" ## TODO
        else:
            infile_query=infile_query.replace('status','\'1\' as status')

        DEBUG(f"Input file query is:\n{infile_query}")
        db_result = dbQuery( cnxn_string_map[ self.input_config.db ], infile_query ).fetchall()
        # TODO: Support rule.printquery
        in_files = [ FileHostRunSegStat(c.filename,c.daqhost,c.runnumber,c.segment,c.status) for c in db_result ]

        INFO(f"Total number of available input files: {len(in_files)}")
        if len(in_files) > 0 :
            DEBUG(f"First line: \n{in_files[0]}")

        #### Now build up potential output files from what's available
        now=time.time()
        rule_matches = {}
        #### Key on runnumber
        in_files.sort(key=lambda x: (x.runnumber)) # itertools.groupby depends on data being sorted
        files_by_run = {k : list(g) for k, g in itertools.groupby(in_files, operator.attrgetter('runnumber'))}
        runlist = list(files_by_run.keys())
        DEBUG(f'All available runnumbers:{runlist}')
        if len(files_by_run) > 0 :
            CHATTY(f"First line: \n{files_by_run[next(iter(files_by_run))]}")

        for runnumber in files_by_run:
            candidates = files_by_run[runnumber]
            if len(candidates) == 0 :
                # By construction of runlist, every runnumber now should have at least one file
                ERROR(f"No input files found for run {runnumber}. That should not happen at this point. Aborting.")
                exit(-1)
            DEBUG(f"Found {len(candidates)} input files for run {runnumber}.")
            CHATTY(f"First line: \n{candidates[0]}")

            ######## Cut up the candidates into streams/daqhosts
            candidates.sort(key=lambda x: (x.runnumber, x.daqhost)) # itertools.groupby depends on data being sorted
            files_for_run = { k : list(g) for
                           k, g in itertools.groupby(candidates, operator.attrgetter('daqhost')) }
            # Removing  the files we just _could_ be useful to shorten the search space
            # for the next iteration. But this is NOT the way to do it, turned out to be the slowest part of the code
            # in_files = [ f for f in in_files if f.runnumber != runnumber ]

            # daq file lists all need GL1 files. Pull them out and add them to the others
            if ( 'gl1daq' in in_types_str ):
                gl1_files = files_for_run.pop('gl1daq',None)
                if gl1_files is None:
                    WARN(f"No GL1 files found for run {runnumber}. Skipping this run.")
                    continue
                CHATTY(f'All GL1 files for for run {runnumber}:\n{gl1_files}')
                for host in files_for_run:
                    files_for_run[host] = gl1_files + files_for_run[host]

            ## Output, log, etc, live in
            ## [somebase]/{prodmode}/{period}/{physicsmode}/{dataset}/{leafdir}/{rungroup}/[dst,log,hist]/filename
            ## where filename = f'{dsttype}_{dataset}-$INT(run,{RUNFMT})-$INT(seg,{SEGFMT})[.root, .log, .hist]'
            ######### Two (originally three) possibilities:
            # - Easy: If the input has a segment number, then the output will have the same segment number
            #     - These are downstream objects (input is already a DST)
            #     - This can be 1-1 or many-to-1 (usually 2-1 for SEED + CLUSTER --> TRACKS)
            # - Medium: The input has no segment number; each output is produced from all sequences in one input stream
            #     - This is currently the case for the streaming daq (tracking)
            #     - As of 04/29, this is the new scheme for the calo daq as well
            #     - In this case, provide ALL input files for the run, and the output will produce its own segment numbers
            # - Hard: The input has no segment number and the output is connected to multiple input streams
            #     - This is currently the case for the triggered daq (calos).
            #     - There may be some intricate mixing needed to get the right event numbers together.
            #        - For now, we ignore this case because it may soon be changed to medium difficulty
            #        - As of 04/29, this case should now be ignored other than for backwards compatibility
            
            ####### Easy case. One way to identify this case is to see if gl1 is not needed 
            if 'gl1daq' not in in_types_str:
                # In this case, the inputs should be a plain list
                if isinstance(input_stem, dict):
                    ERROR( "Input is a downstream object but input_stem is a dictionary.")
                    exit(-1)
                CHATTY(f'\ninput_stem is a list, {self.rulestem} is the output base, and {descriminator} selected/enumerates \n{in_types_str}\nas input')

                # For every segment, there is exactly one output file, and exactly one input file from each stream
                # Sort and group the input files by segment
                # NOTE: We could save a small bit of work by not checking the input files
                # if the output already exists. But we need the segments for that anyway
                outbase=f'{self.rulestem}_{self.dataset}'
                segments = None
                rejected = set()
                for host in files_for_run:
                    files_for_run[host].sort(key=lambda x: (x.segment))
                    new_segments = list(map(lambda x: x.segment, files_for_run[host]))
                    if segments is None:
                        segments = new_segments
                    elif segments != new_segments:
                        rejected.update( set(segments).symmetric_difference(set(new_segments)) )
                        segments = list( set(segments).intersection(new_segments))

                if len(rejected) > 0:
                    WARN(f"Run {runnumber}: Removed segments not present in all streams: {rejected}")

                # If the output doesn't exist yet, use input files to create the job
                for seg in segments:
                    logbase= f'{outbase}_{runnumber:{pRUNFMT}}-{seg:{pSEGFMT}}'
                    output = f'{outbase}-{runnumber:{pRUNFMT}}-{seg:{pSEGFMT}}.root' # == {logbase}.root but this is more explicit
                    if output in existing_output:
                        CHATTY(f"Output file {output} already exists. Not submitting.")
                        continue

                    in_files_for_seg= []
                    for host in files_for_run:
                        in_files_for_seg += [ f.filename for f in files_for_run[host] if f.segment == seg ]
                    CHATTY(f"Creating {output} from {in_files_for_seg}")
                    rule_matches[output] = in_files_for_seg, outbase, logbase, runnumber, seg, self.rulestem

            ####### Medium case. Streaming and (now also) triggered daq
            if 'gl1daq' in in_types_str:
                if not isinstance(input_stem, dict):
                    ERROR( "Input is raw daq but input_stem is not a dictionary.")
                    exit(-1)
                CHATTY(f'\ninput_stem is a dictionary, {self.rulestem} is the output base, and {descriminator} selected/enumerates \n{in_types_str}\nas input')
                
                # Every runnumber has exactly one output file, and a gaggle of input files with matching daqhost
                # Sort and group the input files by host
                for leaf, daqhost in input_stem.items():
                    if daqhost not in files_for_run:
                        DEBUG(f"No inputs from {daqhost} for run {runnumber}.")
                        continue
                    
                    dsttype = f'{self.rulestem}_{leaf}'
                    dsttype += f'_{self.outstub}' # DST_STREAMING_EVENT_%_run3auau
                    # Example arguments for the combiner script:
                    # DST_STREAMING_EVENT_INTT4_run3auau_new_nocdbtag_v000 \ outbase \
                    # DST_STREAMING_EVENT_INTT4_run3auau_new_nocdbtag_v000-00061162-00000 \ logbase \
                    outbase=f'{dsttype}_{self.dataset}'
                    logbase=f'{outbase}_{runnumber:{pRUNFMT}}-{0:{pSEGFMT}}'
                    # check for one existing output file. 
                    first_output=f'{outbase}-{runnumber:{pRUNFMT}}-{0:{pSEGFMT}}.root' # == {logbase}.root but this is more explicit
                    if first_output in existing_output:
                        CHATTY(f"Output file {first_output} already exists. Not submitting.")
                        continue

                    CHATTY(f"Creating {first_output} for run {runnumber} with {len(files_for_run[daqhost])} input files")
                    
                    files_for_run[daqhost].sort(key=lambda x: (x.segment)) # not needed but tidier
                    rule_matches[first_output] = [file.filename for file in files_for_run[daqhost]], outbase, logbase, runnumber, 0, daqhost, self.rulestem+'_'+leaf
        INFO(f'[Parsing time ] {time.time() - now:.2g} seconds' )

        return rule_matches
# ============================================================================
