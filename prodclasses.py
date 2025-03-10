## #!/usr/bin/env python

from dataclasses import dataclass, asdict, field
import yaml

__frozen__ = True
__rules__  = []

# Format strings for run and segment numbers.  n.b. that the "rungroup" which defines the logfile and output file directory structure
# hardcodes "08d" as the run format...  
#
RUNFMT = "%08i"
SEGFMT = "%05i"
DSTFMT = "%s_%s_%s-" + RUNFMT + "-" + SEGFMT + ".root"
DSTFMTv = "%s_%s_%s_%s-" + RUNFMT + "-" + SEGFMT + ".root"

@dataclass( frozen= __frozen__, eq=False )
class SPhnxRule:
    name:              str            # Name of the rule
    script:            str            # Production script
    build:             str  = None    # Build tag
    tag:               str  = None    # Database tag
    files:             str  = None    # Input files query
    filesdb:           str  = None    # Input files DB to query
    runlist:           str  = None    # Input run list query from daq
    direct:            str  = None    # Direct path to input files (supercedes filecatalog)
    lfn2pfn:           str  = "lfn2pfn"  # could be lfn2lfn
    #job:               SPhnxCondorJob = SPhnxCondorJob()
    resubmit:          bool = False   # Set true if job should overwrite existing job
    buildarg:          str  = ""      # The build tag passed as an argument (leaves the "." in place).
    payload:           str = "";      # Payload directory (condor transfers inputs from)
    limit:    int = 0                 # maximum number of matches to return 0=all
    runname:           str = None     # eg run2pp, extracted from name or ...
    version:           str = None     # eg v001 

#!/usr/bin/env python

from dataclasses import dataclass, asdict, field
import yaml
import pathlib

__frozen__ = True
__rules__  = []

# Format strings for run and segment numbers.  n.b. that the "rungroup" which defines the logfile and output file directory structure
# hardcodes "08d" as the run format...  
#
RUNFMT = "%08i"
SEGFMT = "%05i"
DSTFMT = "%s_%s_%s-" + RUNFMT + "-" + SEGFMT + ".root"
DSTFMTv = "%s_%s_%s_%s-" + RUNFMT + "-" + SEGFMT + ".root"

@dataclass( frozen=__frozen__, eq=False )
class SPhnxRule:
    name: str  # Name of the rule
    script: str  # Production script
    build: str = None  # Build tag
    tag: str = None  # Database tag
    files: str = None  # Input files query
    filesdb: str = None  # Input files DB to query
    runlist: str = None  # Input run list query from daq
    direct: str = None  # Direct path to input files (supercedes filecatalog)
    lfn2pfn: str = "lfn2pfn"  # could be lfn2lfn
    # job:               SPhnxCondorJob = SPhnxCondorJob()
    resubmit: bool = False  # Set true if job should overwrite existing job
    buildarg: str = ""  # The build tag passed as an argument (leaves the "." in place).
    payload: str = ""  # Payload directory (condor transfers inputs from)
    limit: int = 0  # maximum number of matches to return 0=all
    runname: str = None  # eg run2pp, extracted from name or ...
    version: str = None  # eg v001

    def __eq__(self, that):
        return self.name == that.name

    def __post_init__(self):
        # Verify the existence of the production script
        #    ... no guarentee that the script is actually at this default path ...
        #    ... it could be sitting in the intialdir of the job ...
        # path_ = ""
        # if self.job.initialdir:
        #    path_ = self.job.initialdir + "/"
        # assert( pathlib.Path( path_ + self.script ).exists() )

        object.__setattr__(self, 'buildarg', self.build)
        if self.build:
            b = self.build
            b = b.replace(".", "")
            object.__setattr__(self, 'build', b)

        if self.runname is None:
            object.__setattr__(self, 'runname', self.name.split('_')[-1])

        # Add to the global list of rules
        __rules__.append(self)

    def dict(self):
        return {k: str(v) for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_yaml(cls, yaml_file):
        """
        Constructs an SPhnxRule object from a YAML file.

        Args:
            yaml_file (str): The path to the YAML file.

        Returns:
            SPhnxRule: An instance of SPhnxRule.
        """
        config = {}
        try:
            with open(yaml_file, "r") as stream:
                config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            raise
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML file not found: {yaml_file}")

        # Assuming the yaml has the needed keys at the top level
        # We are creating a single rule from a file

        # Check for required fields
        required_fields = ["name", "script"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field '{field}' in YAML file.")

        return cls(
            name=config["name"],
            script=config["script"],
            build=config.get("build"),
            tag=config.get("tag"),
            files=config.get("files"),
            filesdb=config.get("filesdb"),
            runlist=config.get("runlist"),
            direct=config.get("direct"),
            lfn2pfn=config.get("lfn2pfn", "lfn2pfn"),
            resubmit=config.get("resubmit", False),
            buildarg=config.get("buildarg", ""),
            payload=config.get("payload", ""),
            limit=config.get("limit", 0),
            runname=config.get("runname"),
            version=config.get("version"),
        )


    # added by kk: read directly from the yaml file

    # kk: Why the replacement with a shallow "=="?
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
        

