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
# /sphenix/lustre01/sphnxpro/{prodmode} / {period}  / {runtype} / outtriplet={build}_{dbtag}_{version} / {leafdir}       /     {rungroup}       /
# /sphenix/lustre01/sphnxpro/production / run3auau  /  cosmics  /        new_nocdbtag_v000          / DST_CALOFITTING / run_00057900_00058000/
_default_filesystem = {
    'outdir'   :    "/sphenix/lustre01/sphnxpro/{prodmode}/dstlake/{period}/{physicsmode}/{dsttype}",
    'finaldir' :    "/sphenix/lustre01/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}",
    'logdir'   : "/sphenix/data/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/log",
    'histdir'  : "/sphenix/data/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/hist",
    'condor'   : "/sphenix/data/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/log",
}

if 'minicondor' in os.uname().nodename or 'local' in os.uname().nodename: # Mac 
    _default_filesystem = {
        'outdir'  : "/Users/eickolja/sphenix/lustre01/sphnxpro/{prodmode}/dstlake/{period}/{physicsmode}/{dsttype}",
        'finaldir': "/Users/eickolja/sphenix/lustre01/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}",
        'logdir'  :   "/Users/eickolja/sphenix/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/log",
        'histdir' :   "/Users/eickolja/sphenix/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/hist",
        'condor'  :   "/Users/eickolja/sphenix/data02/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/log",
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
    intriplet:        Optional[str] = "" # ==tag, i.e. new_nocdbtag_v001; optional only because it's not needed for event combiners
    min_run_events:   Optional[int] = None
    min_run_time:     Optional[int] = None
    prod_identifier:  Optional[str] = None # run3auau, run3cosmics

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
                  rule_substitutions=None) -> "RuleConfig":
        """
        Constructs a RuleConfig object from a YAML data dictionary.

        Args:
            yaml_data: The dictionary loaded from the YAML file.
            rule_name: The name of the rule to extract from the YAML.
            rule_substitutions: A dictionary (usually originating from argparse) to override the YAML data and fill in placeholders.

        Returns:
            A RuleConfig object.
        """
        try:
            rule_data = yaml_data[rule_name]
        except KeyError:
            raise ValueError(f"Rule '{rule_name}' not found in YAML data.")

        if rule_substitutions is None:
            WARN("No rule substitutions provided. Using empty dictionary bnut this may fail.")
            rule_substitutions = {}

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
        runs=rule_substitutions["runs"]
        runlist=rule_substitutions["runlist"]
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

        ### Turning off name mangling; directory mangling is sufficient
        # if 'DST' in rule_substitutions:
        #     outbase=outbase.replace('DST',rule_substitutions['DST'])
        #     DEBUG(f"outbase is mangled to {outbase}")

        ### Optionals
        physicsmode = params_data.get("physicsmode", "physics")
        physicsmode = rule_substitutions.get("physicsmode", physicsmode)
        comment = params_data.get("comment", None)

        ###### Now create InputConfig and CondorJobConfig
        # Extract and validate input_config
        input_data = rule_data.get("input", {})
        check_params(input_data
                    , required=["db", "table"]
                    , optional=["intriplet",
                                "min_run_events","min_run_time",
                                "direct_path", "prod_identifier",
                                "infile_query_constraints",
                                "status_query_constraints","physicsmode"] )

        intriplet=input_data.get("intriplet")
        min_run_events=input_data.get("min_run_events",100000)
        min_run_time=input_data.get("min_run_time",300)
        # Substitutions in direct input path, if given
        input_direct_path = input_data.get("direct_path")
        if input_direct_path is not None:
            input_direct_path = input_direct_path.format(mode=physicsmode)
            DEBUG (f"Using direct path {input_direct_path}")
        prod_identifier = input_data.get("prod_identifier",outstub)
        
        # Allow arbitrary query constraints to be added
        infile_query_constraints  = input_data.get("infile_query_constraints", "")
        infile_query_constraints += rule_substitutions.get("infile_query_constraints", "")
        status_query_constraints = input_data.get("status_query_constraints", "")
        status_query_constraints += rule_substitutions.get("status_query_constraints", "")
        DEBUG(f"Input query constraints: {infile_query_constraints}" )
        DEBUG(f"Status query constraints: {status_query_constraints}" )
        input_config=InputConfig(
            db=input_data["db"],
            table=input_data["table"],
            intriplet=intriplet,
            min_run_events=min_run_events,
            min_run_time=min_run_time,
            direct_path=input_direct_path,
            prod_identifier= prod_identifier,
            infile_query_constraints=infile_query_constraints,
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
                                "request_cpus",
                        # "accounting_group","accounting_group_user",
                    ]
                 )

        # Payload code etc. Prepend by the yaml file's path unless they are direct
        yaml_path = Path(yaml_file).parent.resolve()            
        payload_list = job_data["payload"] + rule_substitutions.get("payload_list",[])
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
            filesystem[key]=filesystem[key].format( prodmode=rule_substitutions["prodmode"],
                                                    period=params_data["period"],
                                                    physicsmode=physicsmode,
                                                    outtriplet=outtriplet,
                                                    dsttype=params_data["dsttype"],
                                                    leafdir='{leafdir}',
                                                    rungroup='{rungroup}',
                                                    )
            DEBUG(f"Filesystem: {key} is {filesystem[key]}")

        # Note: If you use globs in the payload list,
        # the executable will (almost certainly) copy those sub-files and subdirectories individually to the working directory
        # But we must have a fully qualified working path to the executable at submission time
        # because that script will execute the actual payload copy on the node.
        # Bit of an annoying walk with python tools, so use unix find instead
        script = job_data["script"]
        errfiles = []
        if not script.startswith("/"): # Search in the payload unless script has an absolute path
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
        if errfiles:
            WARN("The following errors occurred while searching the payload:")
            for errf in errfiles:
                WARN(errf)

        neventsper   = job_data["neventsper"]
        comment      = job_data.get("comment", None)

        # Partially fill rule_substitutions into the job data
        for field in 'batch_name', 'arguments','log':
            subsval = job_data.get(field)
            if isinstance(subsval, str): # don't try changing None or dictionaries
                subsval = subsval.format(
                          **rule_substitutions
                        , **filesystem
                        , **params_data
                        , payload=",".join(payload_list)
                        , comment=comment
                        , neventsper=neventsper
                        , buildarg=build_string
                        , tag=params_data["dbtag"]
                        , outtriplet=outtriplet
                        # pass remaining per-job parameters forward to be replaced later
                        , outbase='{outbase}'
                        , logbase='{logbase}'
                        , inbase='{inbase}'
                        , run='{run}'
                        , seg='{seg}'
                        , daqhost='{daqhost}'
                        , inputs='{inputs}'
                )
            job_data[field] = subsval
            CHATTY(f"After substitution, {field} is {subsval}")
            request_memory=rule_substitutions.get("mem")
            if request_memory is None:
                request_memory=job_data["mem"]

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
            batch_name=job_data.get("batch_name")
            if branch_name!="main":
                batch_name=f"{branch_name}.{batch_name}"
                
            job_config=CondorJobConfig(
                executable=script,
                request_memory=request_memory,
                request_disk=job_data.get("request_disk", "10GB"),
                request_cpus=job_data.get("request_cpus", "1"),
                comment=comment,
                neventsper=neventsper,
                priority=job_data["priority"],
                batch_name=batch_name,
                arguments_tmpl=job_data["arguments"],
                log_tmpl=job_data["log"],
                filesystem=filesystem,
            )

        ### With all preparations done, construct the constant RuleConfig object
        return cls(
            dsttype=params_data["dsttype"],
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
    def from_yaml_file(cls, yaml_file: str, rule_name: str, rule_substitutions=None) -> "RuleConfig":
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
                             rule_substitutions=rule_substitutions)

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
        
        # Files to be created are checked against this list. Could use various attributes but most straightforward is just the filename
        ## Note: Not all constraints are needed, but they may speed up the query 
        INFO('Checking for already existing output...')
        exist_query  = f"""select filename from datasets 
        where tag='{self.outtriplet}'
        and dataset='{self.dataset}'
        and dsttype like '{dst_type_template}'"""
        if run_condition!="" :
            exist_query += f"\n\tand {run_condition}"
        existing_output = [ c.filename for c in dbQuery( cnxn_string_map['fcr'], exist_query ) ]
        INFO(f"Already have {len(existing_output)} output files")
        if len(existing_output) > 0 :
            CHATTY(f"First line: \n{existing_output[0]}")

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
        existing_status = { c.dstfile if c.dstfile.endswith('.root') else c.dstfile : c.status for c in dbQuery( cnxn_string_map['statr'], status_query ) }
        INFO(f"Already have {len(existing_status)} output files in the production db")
        if len(existing_status) > 0 :
            CHATTY(f"First line: \n{next(iter(existing_status))}")
            
        ####################################################################################
        ###### Now get all existing input files
        ####################################################################################
        INFO("Building candidate inputs...")
        INFO(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
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
        DEBUG( f'Input files are of the form:\n{pprint.pformat(input_stem)}')        
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
            in_types = [ f'{t}' for t in in_types ]

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


        ### Change on July 9 2025: Getting all runs at once is marginally faster
        ### while blowing up resident memory size from 200MB to 10GB or more (unlimited; scales with number of good runs)
        ### So stop doing it that way... 
        # if run_condition!="" :
        #     infile_query += f"\n\tand {run_condition}"
        # infile_query += self.input_config.infile_query_constraints
        # DEBUG(f"infile_query:\n{infile_query}")
        # INFO(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
        # db_result = dbQuery( cnxn_string_map[ self.input_config.db ], infile_query ).fetchall()
        # in_files = [ FileHostRunSegStat(c.filename,c.daqhost,c.runnumber,c.segment,c.status) for c in db_result ]
        # DEBUG(in_files)
        # print(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
        # INFO(f"Total number of available input files: {len(in_files)}")
        # in_files.sort(key=lambda x: (x.runnumber)) # itertools.groupby depends on data being sorted
        # files_by_run = {k : list(g) for k, g in itertools.groupby(in_files, operator.attrgetter('runnumber'))}
        # runlist = list(files_by_run.keys())
        # DEBUG(f'All available runnumbers:{runlist}')
        # for runnumber in files_by_run:
        #     candidates = files_by_run[runnumber]
        #     ...
        # exit()

        ### ... and instead, move the query into the run loop
        
        #### Now build up potential output files from what's available
        now=time.time()
        rule_matches = {}

        ### Runnumber is the prime differentiator
        INFO(f"Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
        for runnumber in runlist_int:
            run_query = infile_query + f" and runnumber={runnumber} "
            CHATTY(f"run_query:\n{run_query}")
            db_result = dbQuery( cnxn_string_map[ self.input_config.db ], run_query ).fetchall()
            candidates = [ FileHostRunSegStat(c.filename,c.daqhost,c.runnumber,c.segment,c.status) for c in db_result ]
            DEBUG(f"Run: {runnumber}, Resident Memory: {psutil.Process().memory_info().rss / 1024 / 1024} MB")
            if len(candidates) == 0 :
                # By construction of runlist, every runnumber now should have at least one file
                ERROR(f"No input files found for run {runnumber}. That should not happen at this point. Skipping run.")
                continue
            DEBUG(f"Found {len(candidates)} input files for run {runnumber}.")
            # CHATTY(f"First line: \n{candidates[0]}")

            ### Simplest case, 1-to-1:For every segment, there is exactly one output file, and exactly one input file from the previous step
            # If the output doesn't exist yet, use input files to create the job
            # TODO: or 'CALOFITTING'
            if not 'TRKR_CLUSTER' in self.dsttype:
                for infile in candidates:
                    outbase=f'{self.dsttype}_{self.dataset}_{self.outtriplet}'                
                    logbase= f'{outbase}-{infile.runnumber:{pRUNFMT}}-{infile.segment:{pSEGFMT}}'
                    output = f'{logbase}.root'
                    if output in existing_output:
                        CHATTY(f"Output file {output} already exists. Not submitting.")
                        continue
                    if output in existing_status:
                        WARN(f"Output file {output} already has production status {existing_status[output]}. Not submitting.")
                        continue
                    in_files_for_seg=[infile]
                    CHATTY(f"Creating {output} from {in_files_for_seg}")
                    rule_matches[output] = in_types, outbase, logbase, infile.runnumber, infile.segment, "dummy", self.dsttype
                continue    
                    
                    
            # For every segment, there is exactly one output file, and exactly one input file _from each stream_ OR from the previous step
            ######## Cut up the candidates into streams/daqhosts
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
                for host in files_for_run:
                    files_for_run[host] = gl1_files + files_for_run[host]
                    any_zero_status = any(file_tuple.status == 0 for file_tuple in files_for_run[host])
                    # Now enforce status!=0 for all files from this host
                    if any_zero_status :
                        files_for_run[host]=[]
                    
            ## Output, log, etc, live in
            ## [somebase]/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/<log|hist/>filename
            ## where filename = f'{dsttype}_{dataset}-$INT(run,{RUNFMT})-$INT(seg,{SEGFMT})[.root, .out, .err]'
            ## root files on gpfs can also start with HIST_ or CALIB_
            ######### Two possibilities:
            # - Easy-ish: If the input has a segment number, then the output will have the same segment number
            #     - These are downstream objects (input is already a DST)
            #     - This can be 1-1 or many-to-1 (usually 2-1 for SEED + CLUSTER --> TRACKS)
            #     - UPDATE: not _that_ easy. How to decide that all needed inputs are there? --> needs extra db query
            # - Medium: The input has no segment number; each output is produced from all sequences in one input stream
            #     - This is currently the case for the streaming daq (tracking)
            #     - As of 04/29, this is the new scheme for the calo daq as well
            #     - In this case, provide ALL input files for the run, and the output will produce its own segment numbers
            
            ####### "Easy" case. One way to identify this case is to see if gl1 is not needed 
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
                ## TODO: Split between seb-like and not seb-like for tracking and calo!
                ### Here we could enforce both mandatory and masked hosts
                DEBUG(f"available_hosts = {available_hosts}")
                # Note: "hostname" means different things for different circumstances. Sometimes it's the full leaf
                # This is very pedestrian, there's probably a more pythonic way:

                # FIXME: More TPC hardcoding
                # 1. require at least N=30 out of the 48 ebdc_[0-24]_[01] to be turned on in the run
                #    This is an early breakpoint to see if the run can be used for tracking
                #    run db doesn't have _[01] though
                TPCset = set( f'ebdc{n:02}' for n in range(0,24) )
                available_tpc_hosts=available_hosts.intersection(TPCset)
                DEBUG(f"available TPC hosts: {available_tpc_hosts}")
                DEBUG(f"  len(available_tpc_hosts) = {len(available_tpc_hosts)}")
                minNTPC=30 / 2
                if len(available_tpc_hosts) < minNTPC:
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
                if len(present_tpc_files) < minNTPC:
                    WARN(f"Skip run {runnumber}. Only {len(present_tpc_files)} TPC detectors actually in the run.")
                    continue

                # 3. For INTT, MVTX, enforce that they're all available if possible
                available_other_hosts=available_hosts.symmetric_difference(TPCset)
                present_other_files=set(files_for_run).symmetric_difference(present_tpc_files)
                CHATTY(f"Available non-TPC hosts in the daq db: {available_other_hosts}")
                CHATTY(f"Present non-TPC leafs: {present_other_files}")
                ### TODO: Only checking length here. Probably okay forever though.
                if len(present_other_files) != len(available_other_hosts) :
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
                        
                if len(rejected) > 0:
                    DEBUG(f"Run {runnumber}: Removed {len(rejected)} segments not present in all streams.")
                    CHATTY(f"Rejected segments: {rejected}")

                # If the output doesn't exist yet, use input files to create the job
                # outbase=f'{self.dsttype}_{self.outtriplet}_{self.outdataset}'
                outbase=f'{self.dsttype}_{self.dataset}_{self.outtriplet}'
                for seg in segments:
                    logbase= f'{outbase}-{runnumber:{pRUNFMT}}-{seg:{pSEGFMT}}'
                    output = f'{logbase}.root'
                    if output in existing_output:
                        CHATTY(f"Output file {output} already exists. Not submitting.")
                        continue
                    if output in existing_status:
                        WARN(f"Output file {output} already has production status {existing_status[output]}. Not submitting.")
                        continue
                    in_files_for_seg= []
                    for host in files_for_run:
                        in_files_for_seg += [ f.filename for f in files_for_run[host] if f.segment == seg ]
                    CHATTY(f"Creating {output} from {in_files_for_seg}")
                    #rule_matches[output] = in_files_for_seg, outbase, logbase, runnumber, seg, daqhost, self.dsttype
                    rule_matches[output] = in_types, outbase, logbase, runnumber, seg, "dummy", self.dsttype
                    
            ####### Medium case. Streaming and (now also) triggered daq
            if 'gl1daq' in in_types_str:
                if not isinstance(input_stem, dict):
                    ERROR( "Input is raw daq but input_stem is not a dictionary.")
                    exit(-1)
                CHATTY(f'\ninput_stem is a dictionary, {self.dsttype} is the output base, and {descriminator} selected/enumerates \n{in_types_str}\nas input')
                
                # Every runnumber has exactly "one" output file (albeit with many segments), and a gaggle of input files with matching daqhost
                # Sort and group the input files by host
                for leaf, daqhost in input_stem.items():
                    if daqhost not in files_for_run:
                        CHATTY(f"No inputs from {daqhost} for run {runnumber}.")
                        continue
                    ### Would be more elegant to del(files_for_run[host]) higher up where we check the status
                    ### But that changes the dictionary during iteration etc. Easier to just check here for []
                    if files_for_run[host]==[]:
                        continue
                    dsttype  = f'{self.dsttype}_{leaf}'
                    dsttype += f'_{self.dataset}' # DST_STREAMING_EVENT_%_run3auau
                    outbase=f'{dsttype}_{self.outtriplet}'
                    seg=0
                    logbase=f'{outbase}-{runnumber:{pRUNFMT}}-{seg:{pSEGFMT}}'
                    # check for one existing output file.
                    # These DO have a segment and a .root extension
                    first_output=f'{logbase}.root'
                    if first_output in existing_output:
                        CHATTY(f"Output file {first_output} already exists. Not submitting.")
                        continue
                    
                    dstfile=f'{outbase}-{runnumber:{pRUNFMT}}-{0:{pSEGFMT}}' # Does NOT have ".root" extension
                    if dstfile in existing_status:
                        WARN(f"Output file {dstfile} already has production status {existing_status[dstfile]}. Not submitting.")
                        continue                    

                    CHATTY(f"Creating {first_output} for run {runnumber} with {len(files_for_run[daqhost])} input files")
                    
                    files_for_run[daqhost].sort(key=lambda x: (x.segment)) # not needed but tidier
                    rule_matches[first_output] = [file.filename for file in files_for_run[daqhost]], outbase, logbase, runnumber, 0, daqhost, self.dsttype+'_'+leaf
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
        _,run,segend=runsegend.split('-')
        seg,end=segend.split('.')
    except ValueError as e:
        print(f"[parse_lfn] Caught error {e}")
        print(f"lfn = {lfn}")
        exit(-1)        
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
