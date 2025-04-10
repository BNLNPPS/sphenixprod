import yaml
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import os
import pathlib
import pprint # noqa: F401

from sphenixdbutils import cnxn_string_map, dbQuery
from simpleLogger import WARN, DEBUG, ERROR, INFO, CRITICAL  # noqa: F401
from sphenixjobdicts import InputsFromOutput

""" This file contains the dataclasses for the rule configuration and matching.
    It encapsulates what is tedious but hopefully easily understood instantiation
    from a YAML file, with some values being changed or completed by command line arguments of the caller.
    RuleConfig  represents a single rule configuration.
    InputConfig represents the input configuration sub-block.
    JobConfig   represents the job configuration sub-block.

    The MatchConfig is the steering class for db queries to 
    find appropriate input files and name the output files.
    It is constructed from a RuleConfig object.

"""

# Striving to keep Dataclasses immutable (frozen=True)
# All modifications and substitutions should be done in the constructor

# ============================================================================
# shared format strings and default filesystem paths
RUNFMT = '%08i'
SEGFMT = '%05i'
VERFMT = '03d'

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
    Check that all required para parameters are present, and no unexpected ones.
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

# ============================================================================
@dataclass( frozen = True )
class InputConfig:
    """Represents the input configuration block in the YAML."""

    db: str
    # query: str
    direct_path: Optional[str] = None  # Make direct_path optional

# ============================================================================

@dataclass( frozen = True )
class JobConfig:
    """Represents the job configuration block in the YAML."""

    arguments: str
    output_destination: str ### needed? used?
    log: str
    accounting_group: str
    accounting_group_user: str
    priority: str
    request_xferslots: str
    batch_name: Optional[str] = None


# ============================================================================
@dataclass( frozen = True )
class RuleConfig:
    """Represents a single rule configuration in the YAML."""

    # Direct input (explictly provided by yaml file + command line arguments)
    rulestem: str       # DST_CALOFITTING
    period: str         # run3auau
    build: str          # ana.472
    dbtag: str          # 2025p000, nocdbtag 
    version: int        # 0, 1, 2, 3
    script: str         # run script on the worker node
    payload: str        # Working directory on the node; transferred by condor
    neventsper: int     # number of events per job
    comment: str        # arbitrary comment
    rsync: str          # additional files to rsync to the node
    mem: str            # "4000MB"; TODO: this belongs in JobConfig
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

    ## Query can dynamically use any field that's in params (via format(**params))
    # Powerful but dangerous, so enforce explicit (but optional) fields that users can use
    # Adding to them then becomes is a more conscious decision
    # These should logically be in InputConfig; we keep them here for backwards compatibility
    mnrun: Optional[int] = 0  # Extra mn < run < mx constraint
    mxrun: Optional[int] = -1
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
                    , required=["rulestem", "period",  "build", "dbtag", "version", "script",
                                "payload", "neventsper", "comment", "rsync", "mem"]
                    , optional=["mnrun", "mxrun", "outstub"])
        
        
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

        ### Add to transfer list
        params_data["rsync"]=params_data["rsync"] + rule_substitions["append2rsync"]
        
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
                    , required=["db"]
                    , optional=["direct_path"] )

        # Substitute rule_substitions into the query
        #input_query  = input_data["query"].format(**rule_substitions, **params_data)
        #input_query  = input_data["query"]

        # Rest of the input substitutions, like database name and direct path
        # DEBUG (f"Using database {rule.inputConfig.db}")
        input_direct_path = input_data.get("direct_path")
        if input_direct_path is not None and "mode" in rule_substitions:
            input_direct_path = input_direct_path.format(**rule_substitions)
            DEBUG (f"Using direct path {input_direct_path}")

        ### Extract and validate job
        job_data = rule_data.get("job", {})
        check_params(job_data
                    , required=[
                    "arguments",
                    "output_destination",
                    "log",
                    "accounting_group",
                    "accounting_group_user",
                    "priority",
                    "request_xferslots",
                    ]
                    , optional=["batch_name"] )

        # Substitute rule_substitions into the job data
        for field in job_data:
            subsval = job_data[field]
            if isinstance(subsval, str): # don't try changing None or dictionaries
                subsval = subsval.format( 
                          **rule_substitions
                        , outbase=outbase
                        , logbase=logbase
                        , **filesystem
                        , PWD=pathlib.Path(".").absolute()
                        , **params_data
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
            script=params_data["script"],
            payload=params_data["payload"],
            neventsper=params_data["neventsper"],
            comment=params_data["comment"],
            rsync=params_data["rsync"],
            build_string=build_string,
            version_string=version_string,
            logbase=logbase,
            outbase=outbase,
            mem=params_data["mem"],
            inputConfig=InputConfig(
                    db=input_data["db"],
                    #query=input_query,
                    direct_path=input_direct_path,
            ),
            filesystem=filesystem,
            jobConfig=JobConfig(
                batch_name=job_data.get("batch_name"),  # batch_name is optional
                arguments=job_data["arguments"],
                output_destination=job_data["output_destination"],
                log=job_data["log"],
                accounting_group=job_data["accounting_group"],
                accounting_group_user=job_data["accounting_group_user"],
                priority=job_data["priority"],
                request_xferslots=job_data["request_xferslots"],
            ),
            mnrun=params_data.get("mnrun"),  # mnrun and mxrun are optional
            mxrun=params_data.get("mxrun"),
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
    rulestem:   str = None         # Name of the matching rule (e.g. DST_CALO)
    script:     str = None         # run script on the worker node
    build:      str = None         # new or ana.472
    # tag:      str = None         # DB tag
    dbtag:      str = None         # DB tag
    payload:    str = None         # Working directory on the node; transferred by condor
    mem:        str = None         # Required memory. Required field, so defaulting to "4096MB" doesn't work
    version_string: str = None      # e.g. "v001"
    build_string:   str = None
    outbase:        str = None         # Name base of the output file (e.g. DST_STREAMING_EVENT_TPC20)

    # Created
    lfn:       str = None         # Logical filename that matches
    dst:       str = None         # Transformed output
    run:       str = None         # Run #
    seg:       str = None         # Seg #
    diskspace: str = "10GB"         # Required disk space

    db:        str = None # used to be filesdb and not in this class
    filequery: str = None # used to be files and not in this class

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
            script=rule_config.script,
            build=rule_config.build,
            build_string=rule_config.build_string,
            dbtag=rule_config.dbtag,
            payload=rule_config.payload,
            mem=rule_config.mem,
            version_string=rule_config.version_string,
            db = rule_config.inputConfig.db,
            #filequery = rule_config.inputConfig.query,
            outbase = rule_config.outbase,
        )

    # ------------------------------------------------
    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }

    # ------------------------------------------------
    def doyourthing (self, args) :
        # TODO: function will be named sensibly and potentially split up
        
        if self.filequery is None :
            ERROR( "No input query?" )
            exit(1)
            return [], None, []  # Early exit
         
        # TODO: add to sanity check
        #  payload should definitely be part of the rsync list but the yaml does that explicitly instead, e.g.
        #  payload :   ./ProdFlow/run3auau/streaming/
        #  rsync   : "./ProdFlow/run3auau/streaming/*"

        # TODO: Find the right class to store this
        # update    = kwargs.get('update',    True ) # update the DB
        updateDb= not args.submit
        
        ### Match parameters are set, now build up the list of inputs and construct corresponding output file names
        INFO("Building candidate inputs...")
        dbresult = dbQuery( cnxn_string_map[ self.db ], self.filequery )
        INFO(f"Matching DB entries: {len(dbresult)}")

        outputs=[]
        dstnames={}
        lfn_lists={}
        range_lists={}
        runMin=999999
        runMax=0
        for line in dbresult:
            ### DEBUG: For real db query result, use
            # run     = line.runnumber
            # segment = line.segment
            # streamname = getattr( line, 'streamname', None ) ## e.g. TPC12
            ### DEBUG: Hack for development:
            run     = line[1]
            segment = line[2]
            # DEBUG: Can be column 4 or 5
            if line[4] == 'NA' :
                streamname = None
            else:
                streamname = line[5]

            output = self.outbase + "-" + RUNFMT + "-" + SEGFMT + ".root"
            runsegkey = f"{run}-{segment}"  # used to index a dictionary
            if streamname:
                output = output.replace( '$(streamname)',streamname ) # TODO: hacky. "$(streamname)" is a poor choice (pretends to be a shell variable before substitution)
                runsegkey += f"-{streamname}"

            # Populate the output name
            # e.g. DST_TRKR_CLUSTER_run3auau_ana.472_2024p012-00057655-00003.root
            output = output % ( int(run), int(segment))
            outputs.append(output)

            # Each output file has a list of corresponding input files
            # TODO: Check key logic is correct. Used to be f"{self.name}_{self.build_string}_{self.dbtag}" or runsegkey. Trying outfile instead
            if dstnames.get( output ) is not None or lfn_lists.get( output ) is not None or range_lists.get( output ) is not None:
                ERROR( f"Duplicate key {output} in lfn,dst,range lists construction. Exiting." )
                exit(1)

            # Needed? For what? Can definitely be constructed from the output name itself, but maybe eliminated altogether
            dstnames[ output ] = (f'{self.name}',f'{self.build_string}_{self.dbtag}') # WRONG !! Missing streamname subst
            lfn_lists[ output ] = line[3].split()                       
            range_lists[ output ] = line[4].split() # list of ranges per file (often 'na')

            # For limiting the search space in DB queries to come
            runMax=max(runMax,run)
            runMin=min(runMin,run)
        
            # Verbatim from slurp:
            # # Drop the run and segment numbers and leading stuff and just pull the datasets.  Note.  When
            # # we switch up to versioning of the files, this will sweep up the version number as well.
            # # Do we want version to be part of the dataset, or a separate entity on its own?
            # #
            # # Additionally... we can no longer rely on just doing a split here UNLESS we are planning to
            # # have a complete break with backwards compatability... The dataset convention goes from
            # #
            # # anaIII_202JpKKK --> anaIII_202JpKKK_vMMM
            # #
            # # I can use a regex here instead.  But do we need to?  Do we want to?  I could see us making
            # # a complete break here... so that the old naming convention is just simply dropped dropped dropped
            # # and we reprocess.
            # #
            # # ... but we don't need to build this if we are using direct lookup
            # if rule.direct is None:
            #     for fn in f.files.split():
            #         base1 = fn.split('-')[0]
            #         rematch = regex_dset.match( base1 )
            #         dset = rematch.group(1)
            #         dtype = rematch.group(2)
            #         vnum = rematch.group(4)
            #         if vnum:
            #             dtype = dtype + '_' + vnum
            #         input_datasets[ ( dset, dtype ) ] = 1

            ### KK: Instead, see if we can't simplify and unify the construction of lfn2pfn later

        if len(lfn_lists)==0:
            DEBUG( "No input files found. Nothing to be done." )
            return [], None, []  # Early exit if nothing to be done

        #
        # Build dictionary of DSTs existing in the datasets table of the file catalog.  For every DST that is in this list,
        # we know that we do not have to produce it if it appears w/in the outputs list.
        
        print()
        # pprint.pprint(dstnames) ## filename -> (dsttype, build_ver)
        pprint.pprint(lfn_lists)
        # pprint.pprint(range_lists)
        # for key, tuple_ in dstnames.items():
        #     dt, ds = tuple_
        #     print(f"key: {key}")
        #     print(f"dt: {dt}")
        #     print(f"ds: {ds}")

        exit()

        # pprint.pprint(outputs[2])
        ### DEBUG: Just hand over the current objet(s) to the caller for development
        return outputs

       # # Do not submit if we fail sanity check on definition file
        # if not sanity_checks( params, input_ ):
        #     ERROR( "Sanity check failed. Exiting." )
        #     exit(1)

        
    # ------------------------------------------------
    def doanewthing (self, args) :
        # Replacement for the old logic
        # TODO: function will be named sensibly and potentially split up

        DEBUG(f'Checking for existing files satisfying {args.rulename}')
        
        ### Match parameters are set, now build up the list of inputs and construct corresponding output file names
        # Despite the "like" clause, this is a very fast query. Extra cuts or substitute cuts like
        # 'and runnumber>={self.runMin} and runnumber<={self.runMax}'
        # can be added if the need arises.
        # Note: If the file database is not up to date, this can be replaced by
        # a filesystem search in the output directory
        # Note: db in the yaml is for input, all output gets logged to the FileCatalog
        outTemplate = self.outbase.replace( 'STREAMNAME', '%' )
        query = f"""select filename from datasets where filename like '{outTemplate}%'"""
        DEBUG (f'Existing files query is {query}')
        if os.uname().sysname=='Darwin' :
            alreadyHave = [ c[3] for c in dbQuery( cnxn_string_map['fccro'], query ) ]
        else:
            alreadyHave = [ c.filename for c in dbQuery( cnxn_string_map['fccro'], query ) ]
        
        INFO(f"Already produced {len(alreadyHave)} output files like {outTemplate}*")

        InputStem = InputsFromOutput[self.rulestem]
        DEBUG( 'Input files are of the form:')
        DEBUG( f'\n{pprint.pformat(InputStem)}')

        exit(0)

# ============================================================================

# Example usage:
if __name__ == "__main__":
    try:
        # # Load all rules from the yaml file.
        # all_rules = RuleConfig.from_yaml_file("DST_STREAMING_run3auau_new_2024p012.yaml")

        # for rule_name, rule_config in all_rules.items():
        #     print(f"Successfully loaded rule configuration: {rule_name}")
        #     print(rule_config.dict())
        #     print("---------------------------------------")

        # Create a MatchConfig from the RuleConfig
        rule_config  = RuleConfig.from_yaml_file("NewDST_STREAMING_run3auau_new_2024p012.yaml",)
        match_config = MatchConfig.from_rule_config(rule_config)
        print(f"MatchConfig from RuleConfig {rule_name}:")
        print(match_config.dict())
        print("---------------------------------------")

    except (ValueError, FileNotFoundError) as e:
        print(f"Error: {e}")
