import yaml
import re
from typing import Dict, List, Tuple, Any, Optional
import itertools
import operator
from dataclasses import dataclass, asdict
import time
from pathlib import Path
import stat
import subprocess
import pprint # noqa: F401
import os
import psutil

from sphenixdbutils import cnxn_string_map, dbQuery
from simpleLogger import CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixjobdicts import inputs_from_output
from sphenixcondorjobs import CondorJobConfig,CondorJobConfig_fieldnames,glob_arguments_tmpl
from sphenixmisc import binary_contains_bisect

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
# /sphenix/lustre01/sphnxpro/{prodmode} / {period}  / {runtype} / outtriplet={build}_{dbtag}_{version} / {leafdir}       /     {rungroup}       /
# /sphenix/lustre01/sphnxpro/production / run3auau  /  cosmics  /        new_nocdbtag_v000          / DST_CALOFITTING / run_00057900_00058000/
_default_filesystem = {
    # 'outdir'   :    "/sphenix/lustre01/sphnxpro/{prodmode}/dstlake/{period}/{physicsmode}/{dsttype}",
    'outdir'   :    "/sphenix/lustre01/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}",
    'finaldir' :    "/sphenix/lustre01/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}",
    'logdir'   : "/sphenix/data/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/log",
    'histdir'  : "/sphenix/data/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/hist",
    'condor'   : "/sphenix/data/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/log",
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
    if optional:
        for f in params_data.copy():
            if f not in optional + required:
                WARN( f"Unexpected field '{f}' in params. Removing, but you should clean up the yaml")
                # raise ValueError(f"Unexpected field '{f}'.")
                check_clean = False
                del params_data[f]

    return check_clean

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
    length=len( lst )
    if length==0:
        return ""

    if length>100000:
        ERROR(f"List has {length} entries. Not a good idea. Bailing out.")
        exit(-1)

    if length==1:
        return f"{name}={lst[0]}"

    sorted_lst=sorted(lst)
    if (sorted_lst != lst):
        WARN("Original list isn't sorted, that shouldn't happen. Proceeding anyway.")

    # range or list with gaps?
    if list(range(min(lst),max(lst)+1)) == sorted_lst:
        return f"{name}>={lst[0]} and {name}<={lst[-1]}"

    strlist=map(str,lst)
    return f"{name} in  ( {','.join(strlist)} )"

# ============================================================================
@dataclass( frozen = True )
class InputConfig:
    """Represents the input configuration block in the YAML."""
    db: str
    table: str
    # Input descriptors. Optional because not needed for event combining
    intriplet:        str = None # ==tag, i.e. new_nocdbtag_v001
    indsttype:        List[str] = None # ['DST_STREAMING_EVENT_epcd01_0','DST_STREAMING_EVENT_epcd01_1'];
    indsttype_str:    str = None        # " ".join(indsttype) for SQL query
    # Run Quality
    min_run_events:   Optional[int] = None
    min_run_time:     Optional[int] = None
    combine_seg0_only:          Optional[bool] = True  # For combination jobs, use only segment 0. Default is yes. No effect for downstream jobs.    
    choose20:         Optional[bool] = False  # Randomly choose 20% of available files
    infile_query_constraints:   Optional[str] = None  # Additional constraints for the input filecatalog query.
    status_query_constraints:   Optional[str] = None  # Additional constraints for the production catalog query
    direct_path: Optional[str]                = None  # Make direct_path optional
    
# ============================================================================
@dataclass( frozen = True )
class RuleConfig:
    """Represents a single rule configuration in the YAML."""

    # Direct input (explictly provided by yaml file + command line arguments)
    dsttype: str       # DST_CALOFITTING
    period: str         # run3auau
    build: str          # for output; ex. ana.472
    dbtag: str          # for output; ex. 2025p000, nocdbtag
    version: int        # for output; ex. 0, 1, 2. Can separate repeated productions

    # Inferred
    build_string: str   # ana472, new
    version_string: str # v000
    outtriplet: str     # new_2025p000_v000
    runlist_int: List[int] # name chosen to differentiate it from --runlist which points to a text file

    # Nested dataclasses
    input_config: InputConfig
    job_config:   CondorJobConfig

    ### Optional fields have to be down here to allow for default values
    physicsmode: str     # cosmics, commissioning, physics (default: physics)
    dataset: str         # run3cosmics for 'DST_STREAMING_EVENT_%_run3cosmics' in run3auau root directory (default: period)

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
                  param_overrides=None,
                  ) -> "RuleConfig":
        """
        Constructs a RuleConfig object from a YAML data dictionary.

        Args:
            yaml_data: The dictionary loaded from the YAML file.
            rule_name: The name of the rule to extract from the YAML.
            param_overrides: A dictionary (usually originating from argparse) to override the YAML data and fill in placeholders.

        Returns:
            A RuleConfig object.
        """
        try:
            rule_data = yaml_data[rule_name]
        except KeyError:
            raise ValueError(f"Rule '{rule_name}' not found in YAML data.")

        if param_overrides is None:
            WARN("No rule substitutions provided. This may fail.")
            param_overrides = {}

        ### Extract and validate top level rule parameters
        params_data = rule_data.get("params", {})
        check_params(params_data
                    , required=["dsttype", "period","build", "dbtag", "version"]
                    , optional=["dataset", "physicsmode"] )

        ### Fill derived data fields
        build_string=params_data["build"].replace(".","")
        version_string = f'v{params_data["version"]:{VERFMT}}'
        outstub = params_data["dataset"] if "dataset" in params_data else params_data["period"]
        outtriplet = f'{build_string}_{params_data["dbtag"]}_{version_string}'
        
        ### Which runs to process?
        runs=param_overrides["runs"]
        runlist=param_overrides["runlist"]
        INFO(f"runs = {runs}")
        INFO(f"runlist = {runlist}")
        runlist_int=None
        ## By default, run over "physics" runs in run3
        default_runmin=66456
        default_runmax=90000
        if runlist: # white-space separated numbers from a file
            INFO(f"Processing runs from file: {runlist}")
            try:
                with open(runlist, 'r') as file:
                    content = file.read()
            except FileNotFoundError:
                ERROR(f"Error: Runlist file not found at {runlist}")
                exit(-1)
            try:
                number_strings = re.findall(r"[-+]?\d+", content)
                runlist_int=[int(runstr) for runstr in number_strings]
            except Exception as e:
                ERROR(f"Error: Exception parsing runlist file {runlist}: {e}")
        else: # Use "--runs". 0 for all default runs; 1, 2 numbers for a single run or a range; 3+ for an explicit list
            INFO(f"Processing runs argument: {runs}")
            if not runs:
                WARN("Processing all runs.")
                runs=['-1','-1']
            nargs=len( runs )
            if  nargs==1:
                runlist_int=[int(runs[0])]
                if runlist_int[0]<=0 :
                    ERROR(f"Can't run on single run {runlist_int[0]}")
            elif nargs==2:
                runmin,runmax=tuple(map(int,runs))
                if runmin<0:
                    runmin=default_runmin
                    WARN(f"Using runmin={runmin}")
                if runmax<0:
                    runmax=default_runmax
                    WARN(f"Using runmax={runmax}")
                runlist_int=list(range(runmin, runmax+1))
            else :
                # dense command here, all it does is make a list of unique ints, and sort it
                runlist_int=sorted(set(map(int,runs)))
                # Remove non-positive entries while we're at it
                runlist_int=[r for r in runlist_int if r>=0]
        if not runlist_int or runlist_int==[]:
            ERROR("Something's wrong parsing the runs to be processed. Maybe runmax < runmin?")
            exit(-1)
        CHATTY(f"Runlist: {runlist_int}")
        CHATTY(f"Run Condition: {list_to_condition(runlist_int)}")

        ### Optionals
        physicsmode = params_data.get("physicsmode", "physics")
        physicsmode = param_overrides.get("physicsmode", physicsmode)
        comment = params_data.get("comment", None)

        ###### Now create InputConfig and CondorJobConfig
        # Extract and validate input_config
        input_data = rule_data.get("input", {})
        check_params(input_data
                    , required=["db", "table"]
                    , optional=["intriplet",
                                "min_run_events","min_run_time",
                                "direct_path", "dataset",
                                "combine_seg0_only","choose20",
                                "infile_query_constraints",
                                "status_query_constraints","physicsmode"] )
        
        intriplet=input_data.get("intriplet")
        dsttype=params_data["dsttype"]
        input_stem = inputs_from_output[dsttype]
        CHATTY( f'Input files are of the form:\n{pprint.pformat(input_stem)}')
        if isinstance(input_stem, dict):
            indsttype = list(input_stem.values())
        elif isinstance(input_stem, list):            
            indsttype = input_stem
        else:
            ERROR("Unrecognized type of input file descriptor {type(input_stem)}")
            exit(1)
        indsttype_str=",".join(indsttype)
        # indsttype_str=f"('{indsttype_str}')" ## Commented out. Adding parens here doesn't play well with handover to condor

        min_run_events=input_data.get("min_run_events",100000)
        min_run_time=input_data.get("min_run_time",300)

        combine_seg0_only=input_data.get("combine_seg0_only",True) # Default is true
        # If explicitly specified, argv overrides
        argv_combine_seg0_only=param_overrides.get("combine_seg0_only")
        if argv_combine_seg0_only is not None:            
            combine_seg0_only=argv_combine_seg0_only

        choose20=input_data.get("choose20",False)
        argv_choose20=param_overrides.get("choose20")
        if argv_choose20 :
            choose20=True

        ### Use choose20 only for combination jobs.
        if choose20 :
            if 'raw' in input_data["db"]:
                WARN ("Selecting only 20% of good runs.")
            else:
                WARN ("Option 'choose20' ignored for downstream production.")
                choose20=False
            
        # Substitutions in direct input path, if given
        input_direct_path = input_data.get("direct_path")
        if input_direct_path is not None:
            input_direct_path = input_direct_path.format(mode=physicsmode)
            DEBUG (f"Using direct path {input_direct_path}")
        dataset = input_data.get("dataset",outstub)
        
        # Allow arbitrary query constraints to be added
        infile_query_constraints  = input_data.get("infile_query_constraints", "")
        infile_query_constraints += param_overrides.get("infile_query_constraints", "")
        status_query_constraints = input_data.get("status_query_constraints", "")
        status_query_constraints += param_overrides.get("status_query_constraints", "")
        DEBUG(f"Input query constraints: {infile_query_constraints}" )
        DEBUG(f"Status query constraints: {status_query_constraints}" )

        input_config=InputConfig(
            db=input_data["db"],
            table=input_data["table"],            
            intriplet=intriplet,
            indsttype=indsttype,
            indsttype_str=indsttype_str,
            min_run_events=min_run_events,
            min_run_time=min_run_time,
            combine_seg0_only=combine_seg0_only,
            choose20=choose20,
            infile_query_constraints=infile_query_constraints,
            status_query_constraints=status_query_constraints,
            direct_path=input_direct_path,
        )

        # Extract and validate job_config
        job_data = rule_data.get("job", {})
        check_params(job_data
                    , required=[
                        "script", "payload", "neventsper","log","priority",
                        # "request_memory",  ## request_memory should be required; but we'll check on it later because "mem" is a deprecated synonym
                    ],
                     optional=None
                    )

        ### Some yaml parameters don't directly correspond to condor ones. Treat those first.
        # These are just named differently to reflect their template character
        job_data["log_tmpl"]=job_data.pop("log")
        arguments_tmpl=job_data.pop("arguments",None)
        if arguments_tmpl:
            # WARN("Using 'arguments' from the yaml file.")
            ERROR("Yaml rule contains 'arguments' field. That almost certainly means the file is outdated.")
            exit(1)
        else:
            arguments_tmpl=glob_arguments_tmpl
        job_data["arguments_tmpl"]=arguments_tmpl
                 
        # Payload code etc. 
        payload_list  = job_data.pop("payload")
        payload_list += param_overrides.get("payload_list",[])
        # Prepend by the yaml file's path unless they are direct
        yaml_path = Path(yaml_file).parent.resolve()            
        for i,loc in enumerate(payload_list):
            if not loc.startswith("/"):
                payload_list[i]= f'{yaml_path}/{loc}'
        DEBUG(f'List of payload items is {payload_list}')
                        
        # Filesystem paths
        filesystem = job_data.get("filesystem",None)
        if filesystem:
            WARN("Using custom filesystem paths from YAML file")
        else:
            INFO("Using default filesystem paths")
            filesystem = _default_filesystem

        # Partially substitute placeholders.
        for key in filesystem:
            filesystem[key]=filesystem[key].format( prodmode=param_overrides["prodmode"],
                                                    period=params_data["period"],
                                                    physicsmode=physicsmode,
                                                    outtriplet=outtriplet,
                                                    leafdir='{leafdir}',
                                                    rungroup='{rungroup}',
                                                    )
            DEBUG(f"{key}:\t {filesystem[key]}")
        job_data["filesystem"]=filesystem 
        
        # The executable
        script = job_data.pop("script")
        # Adjust the executable's path
        if not script.startswith("/"): # Search in the payload unless script has an absolute path
            p = subprocess.Popen(f'/usr/bin/find {" ".join(payload_list)} -type f',
                                 shell=True, # needed to expand "*"
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            errfiles = stderr.decode(errors='ignore').splitlines()
            if errfiles:
                WARN("The following errors occurred while searching the payload:")
                for errf in errfiles:
                    WARN(errf)
            allfiles = stdout.decode(errors='ignore').split()
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
        job_data["executable"]=script

        # Some tedium to deal with a now deprecated field.
        mem            = job_data.pop("mem",None)
        request_memory = job_data.get("request_memory",None)
        if mem:
            WARN("'mem' is deprecated, use 'request_memory' instead.")
            if not request_memory:
                job_data["request_memory"]=mem
            elif request_memory != mem:
                ERROR("Conflicting 'mem' (deprecated) and  'request_memory' fields.")
                exit(1)

        # for k,v in job_data.items():
        #     print(f"{k}:\t {v}")

        # Partially fill param_overrides into the job data
        ## This isn't particularly elegant since it's self-referential.
        ## And you can't pass **job_data, which would be ideal, because of name clashes
        for field in 'batch_name', 'arguments_tmpl','log_tmpl':
            subsval = job_data.get(field)
            if not isinstance(subsval, str): # don't try changing None or dictionaries
                continue
            subsval = subsval.format(
                nevents=param_overrides["nevents"],
                **params_data,
                **filesystem,
                **asdict(input_config),
                payload=",".join(payload_list),
                comment=job_data.get("comment",None),
                neventsper=job_data.get("neventsper"),
                buildarg=build_string,
                tag=params_data["dbtag"],
                outtriplet=outtriplet,
                # pass remaining per-job parameters forward to be replaced later
                outbase='{outbase}',
                logbase='{logbase}',
                inbase='{inbase}',
                run='{run}',
                seg='{seg}',
                daqhost='{daqhost}',
                inputs='{inputs}',
            )
            job_data[field] = subsval
            DEBUG(f"After substitution, {field} is {subsval}")
        environment=f'SPHENIXPROD_SCRIPT_PATH={param_overrides.get("script_path","None")}'
        job_data["environment"]=environment

        # catch different production branches - prepend by branch if not main            
        branch_name="main"
        try:
            result = subprocess.run(
                [f"git -C {Path(__file__).parent} rev-parse --abbrev-ref HEAD"],
                shell=True,
                capture_output=True, 
                text=True, 
                check=True
            )
            branch_name = result.stdout.strip()
            CHATTY(f"Current Git branch: {branch_name}")
        except Exception as e:
            print(f"An error occurred: {e}")
        batch_name=job_data.pop("batch_name")
        job_data["batch_name"]=f"{branch_name}.{batch_name}"

        # Fill in all class fields.
        condor_job_dict={}
        for param in job_data:
            if not param in CondorJobConfig_fieldnames:
                WARN( f"Unexpected field '{param}' in params. Removing, but you should clean up the yaml")
                # raise ValueError(f"Unexpected field '{param}'.")
                continue
            condor_job_dict[param] = job_data[param]
        del job_data         # Kill job_data - it's stale now and easily used accidentally
            
        ## Any remaining overrides
        priority=param_overrides.get("priority",None)
        if priority:
            condor_job_dict["priority"]=priority

        if param_overrides.get("request_memory",None):
            condor_job_dict["request_memory"]=param_overrides["request_memory"]

        request_memory=condor_job_dict.get("request_memory",None)         # Ensure sanity after the mem juggling act
        if not request_memory:
            raise ValueError(f"Missing required field 'request_memory'.")

        #####  Now instantiate the main condor config object for all jobs
        job_config=CondorJobConfig(**condor_job_dict) # Do NOT forget the ** for Dictionary Unpacking
        DebugString="CondorJobConfig:\n"
        for k,v in asdict(job_config).items():
            DebugString += f"{k}:\t {v} \n"
        DEBUG(DebugString)        
        
        ### With all preparations done, construct the constant RuleConfig object
        return cls(
            dsttype=dsttype,
            period=params_data["period"],
            physicsmode=physicsmode,
            dataset=params_data.get("dataset"), 
            build=params_data["build"],
            dbtag=params_data["dbtag"],
            version=params_data["version"],
            build_string=build_string,
            version_string=version_string,
            outtriplet=outtriplet,
            runlist_int=runlist_int,
            input_config=input_config,
            job_config=job_config,
        )

    # ------------------------------------------------
    @classmethod
    def from_yaml_file(cls, yaml_file: str, rule_name: str, param_overrides=None ) -> "RuleConfig":
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
                             param_overrides=param_overrides,
                            )

# ============================================================================

@dataclass( frozen = True )
class MatchConfig:
    dsttype:        str
    runlist_int:    str
    input_config:   InputConfig
    dataset:        str
    outtriplet:     str
    physicsmode:    str
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
            dsttype      = rule_config.dsttype,
            runlist_int  = rule_config.runlist_int,
            input_config = rule_config.input_config,
            dataset      = rule_config.dataset,
            outtriplet   = rule_config.outtriplet,
            physicsmode  = rule_config.physicsmode,
        )

    # ------------------------------------------------
    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }

    # ------------------------------------------------
    def matches(self) :
        ### Match parameters are set, now build up the list of inputs and construct corresponding output file names
        # Despite the "like" clause, this is a fast query. Extra cuts or substitute cuts like
        # 'and runnumber>={self.runMin} and runnumber<={self.runMax}'
        # can be added if the need arises.
        # Note: If the file database is not up to date, we can use a filesystem search in the output directory
        # Note: The db field in the yaml is for input queries only, all output queries go to the FileCatalog
        dst_type_template = f'{self.dsttype}'
        # This test should be equivalent: if 'raw' in self.input_config.db:
        if 'TRIGGERED' in self.dsttype or 'STREAMING' in self.dsttype:
            dst_type_template += '_%'
        dst_type_template += '%'

        ### Which runs to process        
        runlist_int=self.runlist_int
        run_condition=list_to_condition(runlist_int)
        
        # # Files to be created are checked against this list. Could use various attributes but most straightforward is just the filename
        # ## Note: Not all constraints are needed, but they may speed up the query 
        # INFO('Checking for already existing output...')
        # INFO(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
        # exist_query  = f"""select filename from datasets 
        # where tag='{self.outtriplet}'
        # and dataset='{self.dataset}'
        # and dsttype like '{dst_type_template}'"""
        # if run_condition!="" :
        #     exist_query += f"\n\tand {run_condition}"
        # existing_output = [ c.filename for c in dbQuery( cnxn_string_map['fcr'], exist_query ) ]
        # INFO(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")

        # INFO(f"Already have {len(existing_output)} output files")
        # if len(existing_output) > 0 :
        #     CHATTY(f"First line: \n{existing_output[0]}")

        ### Check production status
        INFO('Checking for output already in production...')
        # dst_type_template doesn't contain "new_nodcbtag_v000". It's not needed; this gets caught later.
        # However, let's tighten the query anyway
        # Could construct a different template but I'm lazy today, just add a second dstname pattern
        status_query  = f"""select dstfile,status from production_status 
        where dstname like '{dst_type_template}' 
        and dstname like '%{self.outtriplet}%'"""
        if run_condition!="" :
            status_query += f"\n\tand {run_condition.replace('runnumber','run')}"
        status_query += self.input_config.status_query_constraints
        existing_status = { c.dstfile : c.status for c in dbQuery( cnxn_string_map['statr'], status_query ) }
        INFO(f"Already have {len(existing_status)} output files in the production db")
            
        ####################################################################################
        ###### Now get all existing input files
        ####################################################################################
        DEBUG("Building candidate inputs for run {runnumber}")
        CHATTY(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
        ### Run quality
        # Here is a good spot to check against golden or bad runlists and to enforce quality cuts on the runs
        # RuleConfig and existing output query is too early for that, distclean, spider, earlier productions may want to be less restricted
        INFO("Checking runlist against run quality cuts.")        
        run_quality_tmpl="""
select distinct(runnumber) from run 
 where 
runnumber>={runmin} and runnumber <= {runmax}
 and 
runtype='{physicsmode}'
 and
eventsinrun >= {min_run_events}
 and
EXTRACT(EPOCH FROM (ertimestamp-brtimestamp)) >={min_run_time}
order by runnumber
;
"""
        run_quality_query=run_quality_tmpl.format(
            runmin=min(runlist_int),
            runmax=max(runlist_int),
            physicsmode=self.physicsmode,
            min_run_events=self.input_config.min_run_events,
            min_run_time=self.input_config.min_run_time,
        )
        goodruns=[ int(r) for (r,) in dbQuery( cnxn_string_map['daqr'], run_quality_query).fetchall() ]
        # tighten run condition now
        runlist_int=[ run for run in runlist_int if run in goodruns ]
        if runlist_int==[]:
            return {}
        run_condition=list_to_condition(runlist_int)
        INFO(f"{len(runlist_int)} runs pass run quality cuts.")
        DEBUG(f"Runlist: {runlist_int}")

        ### Assemble leafs, where needed
        input_stem = inputs_from_output[self.dsttype]
        CHATTY( f'Input files are of the form:\n{pprint.pformat(input_stem)}')        
        if isinstance(input_stem, dict):
            in_types = list(input_stem.values())
        else :
            in_types = input_stem
            
        # TODO: Support rule.printquery
        # Manipulate the input types to match the database
        if 'raw' in self.input_config.db:
            descriminator='daqhost'
            in_types.insert(0,'gl1daq') # all raw daq files need an extra GL1 file
        else:
            descriminator='dsttype'

        # Transform list to ('<v1>','<v2>', ...) format. (one-liner doesn't work in python 3.9)
        in_types_str = f'( QUOTE{"QUOTE,QUOTE".join(in_types)}QUOTE )'
        in_types_str = in_types_str.replace("QUOTE","'")

        # Need status==1 for all files in a given run,host combination
        # Easier to check that after the SQL query
        infile_query = f"""select filename,{descriminator} as daqhost,runnumber,segment,status
        from {self.input_config.table}
        where \n\t{descriminator} in {in_types_str}\n
        """
        intriplet=self.input_config.intriplet
        if intriplet and intriplet!="":
            infile_query+=f"\tand tag='{intriplet}'"
        if 'raw' in self.input_config.db:
            infile_query+= f" and dataset='{self.physicsmode}'"
        else:
            infile_query=infile_query.replace('status','\'1\' as status')
        infile_query += self.input_config.infile_query_constraints
        # Keeping the run condition as a fallback; it should never matter though
        if run_condition!="" :
            infile_query += f"\n\tand {run_condition}"
            
        #### Do existence queries in the run loop. More db reads but much smaller RAM usage
        #### Now build up potential output files from what's available
        now=time.time()
        rule_matches = {}

        ### Runnumber is the prime differentiator
        INFO(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
        for runnumber in runlist_int:
            # Files to be created are checked against this list. Could use various attributes but most straightforward is just the filename
            ## Note: Not all constraints are needed, but they may speed up the query 
            exist_query  = f"""select filename from datasets 
            where tag='{self.outtriplet}'
            and dataset='{self.dataset}'
            and dsttype like '{dst_type_template}'"""
            if run_condition!="" :
                exist_query += f"\n\tand {run_condition}"
            exist_query +=  f"\n\tand runnumber={runnumber}"
            existing_output = [ c.filename for c in dbQuery( cnxn_string_map['fcr'], exist_query ) ]
            existing_output.sort()
            DEBUG(f"Already have {len(existing_output)} output files for run {runnumber}")

            # Potential input files for this run
            run_query = infile_query + f"\n\t and runnumber={runnumber} "
            CHATTY(f"run_query:\n{run_query}")
            db_result = dbQuery( cnxn_string_map[ self.input_config.db ], run_query ).fetchall()
            candidates = [ FileHostRunSegStat(c.filename,c.daqhost,c.runnumber,c.segment,c.status) for c in db_result ]
            CHATTY(f"Run: {runnumber}, Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
            if len(candidates) == 0 :
                # # By construction of runlist, every runnumber now should have at least one file
                # TODO: No longer true, check 
                # ERROR(f"No input files found for run {runnumber}. That should not happen at this point. Skipping run.")
                DEBUG(f"No input files found for run {runnumber}. Skipping run.")
                continue
            DEBUG(f"Found {len(candidates)} input files for run {runnumber}.")

            ### Simplest case, 1-to-1:For every segment, there is exactly one output file, and exactly one input file from the previous step
            # If the output doesn't exist yet, use input files to create the job
            # TODO: or 'CALOFITTING' or many other job types
            if 'TRKR_SEED' in self.dsttype:
                for infile in candidates:
                    outbase=f'{self.dsttype}_{self.dataset}_{self.outtriplet}'                
                    logbase= f'{outbase}-{infile.runnumber:{pRUNFMT}}-{infile.segment:{pSEGFMT}}'
                    dstfile = f'{logbase}.root'
                    if binary_contains_bisect(existing_output,dstfile):
                        CHATTY(f"Output file {dstfile} already exists. Not submitting.")
                        continue
                    if dstfile in existing_status:
                        WARN(f"Production status of {dstfile} is {existing_status[dstfile]}. Not submitting.")
                        continue
                    in_files_for_seg=[infile]
                    CHATTY(f"Creating {dstfile} from {in_files_for_seg}")
                    #rule_matches[dstfile] = in_types_str, outbase, logbase, infile.runnumber, infile.segment, "dummy", self.dsttype
                    rule_matches[dstfile] = ["dbinput"], outbase, logbase, infile.runnumber, infile.segment, "dummy", self.dsttype
                continue    

            ####### NOT 1-1, requires more work:
            # For every segment, there is exactly one output file, and exactly one input file _from each stream_ OR from the previous step
            ######## Cut up the candidates into streams/daqhost≈ƒs
            candidates.sort(key=lambda x: (x.runnumber, x.daqhost)) # itertools.groupby depends on data being sorted

            files_for_run = { k : list(g) for
                              k, g in itertools.groupby(candidates, operator.attrgetter('daqhost')) }

            # daq file lists all need GL1 files. Pull them out and add them to the others
            if ( 'gl1daq' in in_types_str ):
                gl1_files = files_for_run.pop('gl1daq',None)
                if gl1_files is None:
                    WARN(f"No GL1 files found for run {runnumber}. Skipping this run.")
                    continue
                CHATTY(f'All GL1 files for for run {runnumber}:\n{gl1_files}')
                
                ### Important change, 07/15/2025: By default, only care about segment 0!
                segswitch="seg0fromdb"
                if not self.input_config.combine_seg0_only:
                    DEBUG("Using, and requiring, all input segments")
                    segswitch="allsegsfromdb"
                    for host in files_for_run:
                        files_for_run[host] = gl1_files + files_for_run[host]
                        any_zero_status = any(file_tuple.status == 0 for file_tuple in files_for_run[host])
                        # Now enforce status!=0 for all files from this host
                        if any_zero_status :
                            files_for_run[host]=[]
                    # Done with the non-default. 
                else: ### Use only segment 0; this is actually a bit harder
                    CHATTY("Using only input segment 0")
                    # GL1 file?
                    gl1file0=None
                    for f in gl1_files:
                        if f.segment==0 and f.status!=0:
                            gl1file0=f
                            break
                    if not gl1file0:
                        CHATTY(f"No segment 0 GL1 file found for run {runnumber}. Skipping this run.")
                        for host in files_for_run:
                            files_for_run[host]=[]
                        continue

                    # With a segment0 gl1 file, we can now go over the other hosts
                    for host in files_for_run:
                        for f in files_for_run[host]:
                            if f.segment==0 and f.status!=0:
                                files_for_run[host]=[gl1file0,f]
                                break
                        else:  # remember that python's for-else executes when the break doesn't
                            CHATTY(f"No segment 0 file found for run {runnumber}, host {host}. Skipping this run.")
                            files_for_run[host]=[]
                # \combine_seg0_only
            # \if gl1daq in intypes

            ####### "Easy" case. One way to identify this case is to see if gl1 is not needed 
            #  If the input has a segment number, then the output will have the same segment number
            #  - These are downstream objects (input is already a DST)
            #  - This can be 1-1 or many-to-1 (usually 2-1 for SEED + CLUSTER --> TRACKS)
            if 'gl1daq' not in in_types_str:
                # In this case, the inputs should be a plain list
                if isinstance(input_stem, dict):
                    ERROR( "Input is a downstream object but input_stem is a dictionary.")
                    exit(-1)
                CHATTY(f'\ninput_stem is a list, {self.dsttype} is the output base, and {descriminator} selected/enumerates \n{in_types_str}\nas input')
                
                ### Get available input
                DEBUG("Getting available daq hosts for run {runnumber}")
                ## TODO: Split between seb-like and not seb-like for tracking and calo!
                daqhost_query=f"""
select hostname from hostinfo 
where hostname not like 'seb%' and hostname not like 'gl1%'
and runnumber={runnumber}"""
                available_hosts=set([ c.hostname for c in dbQuery( cnxn_string_map['daqr'], daqhost_query).fetchall() ])
                ### Here we could enforce both mandatory and masked hosts
                DEBUG(f"available_hosts = {available_hosts}")

                # FIXME: More TPC hardcoding
                # 1. require at least N=30 out of the 48 ebdc_[0-24]_[01] to be turned on in the run
                #    This is an early breakpoint to see if the run can be used for tracking
                #    run db doesn't have _[01] though
                TPCset = set( f'ebdc{n:02}' for n in range(0,24) )
                available_tpc_hosts=available_hosts.intersection(TPCset)
                DEBUG(f"available TPC hosts: {available_tpc_hosts}")
                DEBUG(f"  len(available_tpc_hosts) = {len(available_tpc_hosts)}")
                minNTPC=48 / 2
                if len(available_tpc_hosts) < minNTPC and not self.physicsmode=='cosmics':
                    INFO(f"Skip run. Only {2*len(available_tpc_hosts)} TPC detectors turned on in the run.")
                    continue
                
                # 2. How many are TPC hosts are actually there in this run.
                #    Not necessarily the same as above, if input DSTs aren't completely produced yet.
                #    Other reason could be if the daq db is wrong.
                present_tpc_files=set()
                for host in files_for_run:
                    for available in available_tpc_hosts:
                        if available in host:
                            present_tpc_files.add(host)
                            continue
                if len(present_tpc_files) < minNTPC and not self.physicsmode=='cosmics':
                    WARN(f"Skip run {runnumber}. Only {len(present_tpc_files)} TPC detectors actually in the run.")
                    continue

                # 3. For INTT, MVTX, enforce that they're all available if possible
                available_other_hosts=available_hosts.symmetric_difference(TPCset)
                present_other_files=set(files_for_run).symmetric_difference(present_tpc_files)
                CHATTY(f"Available non-TPC hosts in the daq db: {available_other_hosts}")
                CHATTY(f"Present non-TPC leafs: {present_other_files}")
                ### TODO: Only checking length here. Probably okay forever though.
                if len(present_other_files) != len(available_other_hosts) and not self.physicsmode=='cosmics': 
                    WARN(f"Skip run. Only {len(present_other_files)} non-TPC detectors actually in the run. {len(available_other_hosts)} possible.")
                    WARN(f"Available non-TPC hosts in the daq db: {available_other_hosts}")
                    WARN(f"Present non-TPC leafs: {present_other_files}")
                    continue

                # Sort and group the input files by segment. Reject if not all hosts are present in the segment yet
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
                        
                if len(rejected) > 0  and not self.physicsmode=='cosmics' :
                    DEBUG(f"Run {runnumber}: Removed {len(rejected)} segments not present in all streams.")
                    CHATTY(f"Rejected segments: {rejected}")

                # If the output doesn't exist yet, use input files to create the job
                # outbase=f'{self.dsttype}_{self.outtriplet}_{self.outdataset}'
                outbase=f'{self.dsttype}_{self.dataset}_{self.outtriplet}'
                for seg in segments:
                    logbase= f'{outbase}-{runnumber:{pRUNFMT}}-{seg:{pSEGFMT}}'
                    dstfile = f'{logbase}.root'
                    if dstfile in existing_output:
                        CHATTY(f"Output file {dstfile} already exists. Not submitting.")
                        continue
                    if dstfile in existing_status:
                        WARN(f"Output file {dstfile} already has production status {existing_status[dstfile]}. Not submitting.")
                        continue                      
                    # in_files_for_seg= []
                    # for host in files_for_run:
                    #     in_files_for_seg += [ f.filename for f in files_for_run[host] if f.segment == seg ]
                    # in_files_for_seg=[ "foo", "bar", "baz" ]
                    # CHATTY(f"Creating {dstfile} from {in_files_for_seg}")
                    ## in_types as first return?                    
                    rule_matches[dstfile] = ["dbinput"], outbase, logbase, runnumber, seg, "dummy", self.dsttype

            ######## Streaming and triggered daq combination
            # In this case, provide ALL input files for the run, and the output will produce its own segment numbers
            # Output and input segment number have no correlation. Not possible to check for all possible existing outfiles
            # so we have to assume if one exists for segment0, it exists for all. This is then the file we key on in prod db as well.
            if 'gl1daq' in in_types_str:
                if not isinstance(input_stem, dict):
                    ERROR( "Input is raw daq but input_stem is not a dictionary.")
                    exit(-1)
                CHATTY(f'\ninput_stem is a dictionary, {self.dsttype} is the output base, and {descriminator} selected/enumerates \n{in_types_str}\nas input')
                
                ### Important change, 07/15/2025: By default, only care about segment 0!
                # Sort and group the input files by host
                for leaf, daqhost in input_stem.items():
                    if daqhost not in files_for_run:
                        CHATTY(f"No inputs from {daqhost} for run {runnumber}.")
                        continue
                    if files_for_run[daqhost]==[]:
                        continue
                    dsttype  = f'{self.dsttype}_{leaf}'
                    dsttype += f'_{self.dataset}' # DST_STREAMING_EVENT_%_run3auau
                    outbase=f'{dsttype}_{self.outtriplet}'
                    # Use segment 0 as key for logs and for existing output
                    logbase=f'{outbase}-{runnumber:{pRUNFMT}}-{0:{pSEGFMT}}'
                    dstfile=f'{logbase}.root'
                    if dstfile in existing_output:
                        CHATTY(f"Output file {dstfile} already exists. Not submitting.")
                        continue
                    
                    if dstfile in existing_status:
                        WARN(f"Output file {dstfile} already has production status {existing_status[dstfile]}. Not submitting.")
                        continue                    

                    DEBUG(f"Creating {dstfile} for run {runnumber} with {len(files_for_run[daqhost])} input files")
                    
                    files_for_run[daqhost].sort(key=lambda x: (x.segment)) # not needed but tidier
                    rule_matches[dstfile] = [segswitch], outbase, logbase, runnumber, 0, daqhost, self.dsttype+'_'+leaf
                # \if gl1daq, i.e. combining or not
            # \for run 
        INFO(f'[Parsing time ] {time.time() - now:.2g} seconds' )

        return rule_matches
# ============================================================================
def parse_lfn(lfn: str, rule: RuleConfig) -> Tuple[str,...] :
    # Notably, input is not necessarily a true lfn, but:
    # If there's a colon, throw everything away after the first one; that's another parser's problem
    try:
        name=lfn.split(':')[0]
        name=Path(name).name # could throw an error instead if we're handed a full path.
        #  split at, and remove, run3auau_new_nocbdtag_v001, remainder is 'DST_...', '-00066582-00000.root' (or .finished)
        # dsttype,runsegend=name.split(f'_{rule.outtriplet}_{rule.dataset}')
        dsttype,runsegend=name.split(f'_{rule.dataset}_{rule.outtriplet}')        
        if runsegend=='.root':
            raise ValueError("killkillkill")    
        _,run,segend=runsegend.split('-')
        seg,end=segend.split('.')
    except ValueError as e:
        print(f"[parse_lfn] Caught error {e}")
        print(f"lfn = {lfn}")
        raise
        # else:
        #     exit(-1)        
    return dsttype,int(run),int(seg),end


# ============================================================================
def parse_spiderstuff(filename: str) -> Tuple[str,...] :
    try:
        size=-1
        ctime=-1
        if 'size' in filename and 'ctime'in filename:
            lfn,_,nevents,_,first,_,last,_,md5,_,size,_,ctime,_,dbid = filename.split(':')
        else:
            lfn,_,nevents,_,first,_,last,_,md5,_,dbid = filename.split(':')

        lfn=Path(lfn).name
    except Exception as e:
        ERROR(f"Error: {e}")
        print(filename)
        print(filename.split(':'))
        exit(-1)

    return lfn,int(nevents),int(first),int(last),md5,int(size),int(ctime),int(dbid)

# ============================================================================
