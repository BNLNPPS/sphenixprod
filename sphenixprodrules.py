import yaml
import re
from typing import Dict, List, Tuple, Any, Optional
import itertools
import operator
from dataclasses import dataclass, asdict
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

""" This file contains the dataclasses for the rule configuration and matching.
    It encapsulates what is tedious but hopefully easily understood instantiation
    from a YAML file, with some values being changed or completed by command line arguments of the caller.
    RuleConfig  represents a single rule configuration.
    InputConfig represents the input configuration sub-block.
    JobConfig   represents the job configuration sub-block.
"""

# Striving to keep Dataclasses immutable (frozen=True)
# All modifications and should be done in the constructor

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
                                "combine_seg0_only",
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
