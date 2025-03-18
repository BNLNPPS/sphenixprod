import yaml
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional

from simpleLogger import ERROR, DEBUG

# import math

# Explicitly marking the dataclasses as mutable (frozen=False) to allow for
# modification of the attributes after instantiation. 
# After construction from a YAML file, the RuleConfig object may be modified
# to add additional attributes, change existing ones, replace placeholders.

# ============================================================================
@dataclass( frozen = False )
class InputConfig:
    """Represents the input configuration block in the YAML."""

    db: str
    query: str
    direct_path: Optional[str] = None  # Make direct_path optional


# ============================================================================
_default_filesystem = {
        'outdir'  :           "/sphenix/lustre01/sphnxpro/production/$(runname)/$(runtype)/$(build)_$(tag)_$(version)/{leafdir}/run_$(rungroup)/dst"
    ,   'logdir'  : "file:///sphenix/data/data02/sphnxpro/production/$(runname)/$(runtype)/$(build)_$(tag)_$(version)/{leafdir}/run_$(rungroup)/log"
    ,   'histdir' :        "/sphenix/data/data02/sphnxpro/production/$(runname)/$(runtype)/$(build)_$(tag)_$(version)/{leafdir}/run_$(rungroup)/hist"
    ,   'condor'  :                                 "/tmp/production/$(runname)/$(runtype)/$(build)_$(tag)_$(version)/{leafdir}/run_$(rungroup)/log"    
}

# ============================================================================

@dataclass( frozen = False )
class JobConfig:
    """Represents the job configuration block in the YAML."""

    arguments: str
    output_destination: str
    log: str
    accounting_group: str
    accounting_group_user: str
    priority: str
    request_xferslots: str
    batch_name: Optional[str] = None
    filesystem: Optional[dict] = None


# ============================================================================
@dataclass( frozen = False )
class RuleConfig:
    """Represents a single rule configuration in the YAML."""

    name: str
    build: str
    # build_name: str ## KK: deprecated --> deleted
    dbtag: str
    version: int
    logbase: str
    outbase: str
    script: str
    payload: str
    neventsper: int
    comment: str
    rsync: str
    mem: str
    input: InputConfig
    job: JobConfig
    mnrun: Optional[int] = None  # Adding mnrun and mxrun as optional
    mxrun: Optional[int] = None
    dstin: Optional[str] = None
    dataset: Optional[str] = None
    resubmit: bool = False

    def dict(self) -> Dict[str, Any]:
        """Convert to a dictionary, handling nested dataclasses."""
        data = asdict(self)
        data['input'] = asdict(self.input)
        data['job'] = asdict(self.job)
        return data

    @classmethod
    def from_yaml(cls, yaml_data: Dict[str, Any], rule_name: str) -> "RuleConfig":
        """
        Constructs a RuleConfig object from a YAML data dictionary.

        Args:
            yaml_data: The dictionary loaded from the YAML file.
            rule_name: The name of the rule to extract from the YAML.

        Returns:
            A RuleConfig object.
        """
        try:
            rule_data = yaml_data[rule_name]
        except KeyError:
            raise ValueError(f"Rule '{rule_name}' not found in YAML data.")

        # Extract and validate params
        params_data = rule_data.get("params", {})
        required_params_fields = ["name", "build", "dbtag", "version", "logbase", "outbase", "script",
                                  "payload", "neventsper", "comment", "rsync", "mem"]
        for f in required_params_fields:
            if f not in params_data:
                raise ValueError(f"Missing required field '{f}' in params for rule '{rule_name}'.")

        # Extract and validate input
        input_data = rule_data.get("input", {})
        required_input_fields = ["db", "query"]
        for f in required_input_fields:
            if f not in input_data:
                raise ValueError(f"Missing required field '{f}' in input for rule '{rule_name}'.")

        # Extract job
        job_data = rule_data.get("job", {})

        # Validate job
        required_job_fields = [
            "arguments",
            "output_destination",
            "log",
            "accounting_group",
            "accounting_group_user",
            "priority",
            "request_xferslots",
        ]
        for f in required_job_fields:
            if f not in job_data:
                raise ValueError(f"Missing required field '{f} in job for rule '{rule_name}'.")

        filesystem = job_data.get("filesystem")
        if filesystem is None:
            filesystem = _default_filesystem

        return cls(
            name=params_data["name"],
            build=params_data["build"],
            dbtag=params_data["dbtag"],
            version=params_data["version"],
            logbase=params_data["logbase"],
            outbase=params_data["outbase"],
            script=params_data["script"],
            payload=params_data["payload"],
            neventsper=params_data["neventsper"],
            comment=params_data["comment"],
            rsync=params_data["rsync"],
            mem=params_data["mem"],
            input=InputConfig(db=input_data["db"], query=input_data["query"], direct_path=input_data.get("direct_path")),
            job=JobConfig(
                batch_name=job_data.get("batch_name"),  # batch_name is optional
                arguments=job_data["arguments"],
                output_destination=job_data["output_destination"],
                log=job_data["log"],
                accounting_group=job_data["accounting_group"],
                accounting_group_user=job_data["accounting_group_user"],
                priority=job_data["priority"],
                request_xferslots=job_data["request_xferslots"],
                filesystem=filesystem
            ),
            mnrun=params_data.get("mnrun"),  # mnrun and mxrun are optional
            mxrun=params_data.get("mxrun"),
            dstin=params_data.get("dstin"),  # dstin and dataset are optional
            dataset=params_data.get("dataset"),
            resubmit=params_data.get("resubmit", False),  # Get resubmit from params, default to False
        )

    @classmethod
    def from_yaml_file(cls, yaml_file: str) -> Dict[str, "RuleConfig"]:
        """
        Constructs a dictionary of RuleConfig objects from a YAML file.

        Args:
            yaml_file: The path to the YAML file.

        Returns:
            A dictionary where keys are rule names and values are RuleConfig objects.
        """
        try:
            with open(yaml_file, "r") as stream:
                yaml_data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise ValueError(f"Error parsing YAML file: {exc}")
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML file not found: {yaml_file}")

        rules = {}
        for rule_name in yaml_data:
            rules[rule_name] = cls.from_yaml(yaml_data, rule_name)
        return rules

# ============================================================================

@dataclass( frozen = True )
class MatchConfig:
    # From RuleConfig
    name:     str = None         # Name of the matching rule
    script:   str = None         # The run script
    build:    str = None         # Build
    tag:      str = None         # DB tag
    payload:  str = None         # Payload directory (condor transfers inputs from)
    mem:      str = None         # Required memory
    version:  str = None

    # Inferred, in __post_init__
    buildarg: str = None

    lfn:      str = None         # Logical filename that matches
    dst:      str = None         # Transformed output
    run:      str = None         # Run #
    seg:      str = None         # Seg #
    disk:     str = None         # Required disk space

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

    # def __eq__( self, that ):
    #     return self.run==that.run and self.seg==that.seg

    def __post_init__(self):
        if self.buildarg is not None:
            ERROR("buildarg is internal, do not set it")
            exit(1)
        b = self.build.replace(".","")
        object.__setattr__(self, 'buildarg', b)
        DEBUG(f"buildarg: {self.buildarg}")


    #     run = int(self.run)
    #     object.__setattr__(self, 'rungroup', f'{100*math.floor(run/100):08d}_{100*math.ceil((run+1)/100):08d}')

    def dict(self):
        return { k: str(v) for k, v in asdict(self).items() if v is not None }

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
            name=rule_config.name,
            script=rule_config.script,
            build=rule_config.build,
            tag=rule_config.dbtag,
            payload=rule_config.payload,
            mem=rule_config.mem,
            version=rule_config.version,
            db = rule_config.input.db,
            filequery = rule_config.input.query,
        )

# ============================================================================


# Example usage:
if __name__ == "__main__":
    try:
        # Load all rules from the yaml file.
        all_rules = RuleConfig.from_yaml_file("DST_STREAMING_run3auau_new_2024p012.yaml")

        for rule_name, rule_config in all_rules.items():
            print(f"Successfully loaded rule configuration: {rule_name}")
            print(rule_config.dict())
            print("---------------------------------------")

            # Create a MatchConfig from the RuleConfig
            match_config = MatchConfig.from_rule_config(rule_config)
            print(f"MatchConfig from RuleConfig {rule_name}:")
            print(match_config.dict())
            print("---------------------------------------")

    except (ValueError, FileNotFoundError) as e:
        print(f"Error: {e}")
