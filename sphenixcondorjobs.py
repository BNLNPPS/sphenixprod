from dataclasses import dataclass
from typing import Optional, ClassVar, List
import math

import pprint # noqa F401

# ============================================================================

@dataclass( frozen = True )
class CondorJobConfig:
    """Represents the job configuration block in the YAML."""
    script:                str  # run script on the worker node
    arguments:             str
    output_destination:    str
    log:                   str
    neventsper:            int  # number of events per job
    payload:               str  # Working directory on the node; transferred by condor
    rsync:                 str  # additional files to rsync to the node
    mem:                   str  # "4000MB"
    comment:               str  # arbitrary comment
    priority:              str
    leafdir:               str = "TODO"
    rungroup:              str = "run_{a:08d}_{b:08d}"
    batch_name:            Optional[str] = None

# ============================================================================
@dataclass( frozen = True )
class CondorJob:
    """ This class is used for individual condor jobs. 
    Configured via JobConfig and RuleConfig.
    Individual jobs are created with 
    - an output file
    - a list of input files
    - a list of arguments, customized for each job 
    Key logic is to create dicts for htcondor and then either dump them to files or submit them directly
    Goal: Create chunks or batches of jobs that can be submitted in one go
    Idea: This package fills directories with job files, condor_submit is run as a separate daemon
    """

    # --- Class Variables (Shared across all instances) ---
    script:                ClassVar[str] # run script on the worker node
    neventsper:            ClassVar[int] # number of events per job
    # 04/25/2025: accounting_group and accounting_group_user should no longer be set, 
    # submit host will do this automatically.
    # accounting_group:      ClassVar[str] = 'group_sphenix.mdc2'
    # accounting_group_user: ClassVar[str] = 'sphnxpro'

    # --- Instance Variables (Specific to each job) ---
    arguments:             str
    output_destination:    str           
    log:                   str
    output_file:           str           # Output file for the job
    input_files:           List[str]     # List of input files for the job
    run:                   int
    seg:                   int
    # --- Most or all of these should also be ClassVar, but we're allowing some flexibility for now
    payload:               str           # Working directory on the node; transferred by condor.
    rsync:                 str           # additional files to rsync to the node.
    mem:                   str           # "4000MB".
    comment:               str           # arbitrary comment
    priority:              str
    batch_name:            Optional[str] = None

    # ------------------------------------------------
    @classmethod
    def from_config(cls, job_config: CondorJobConfig, output_file: str, input_files: List[str], run: int, seg: int):
        """
        Constructs a CondorJob instance from a CondorJobConfig object.
        """

        # leafdir=job_config.leafdir
        # print(f"leafdir: {leafdir}")
        # rungroup=job_config.rungroup.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
        # print(f"rungroup: {rungroup}")
        # output_file=output_file.format(leafdir=leafdir, rungroup=rungroup)
        # print(f"output_file: {output_file}")
        # input_files=",".join(input_files)
        # print(f"input_files: {input_files}")
        # input_files=input_files.format(leafdir=leafdir, rungroup=rungroup)
        

        rungroup=job_config.rungroup.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
        leafdir=job_config.leafdir
        job_config_arguments = job_config.arguments.format(rungroup=rungroup, leafdir=leafdir)
        print(f"job_config_arguments: {job_config_arguments}")
        print()
        exit()
        for key in job_config_arguments:
            job_config_arguments[key] = job_config_arguments[key].format(
                leafdir=job_config.leafdir,
                rungroup=job_config.rungroup.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
            )
        print(f"job_config_arguments: {job_config_arguments}")

        exit(0)
        arguments = job_config.arguments.format(
            neventsper=job_config.neventsper,
            input_files=input_files,
            output_file=output_file,
            leafdir=job_config.leafdir,
            rungroup=job_config.rungroup.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100)),
        )
        # print(f"{job_config.rungroup.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))}")
        # exit(0)
        return cls(
            arguments           = arguments,
            output_destination  = job_config.output_destination,
            log                 = job_config.log,
            payload             = job_config.payload,
            rsync               = job_config.rsync,
            mem                 = job_config.mem,
            comment             = job_config.comment,
            priority            = job_config.priority,
            batch_name          = job_config.batch_name, # Handles Optional[str] correctly
            output_file         = output_file,
            input_files         = input_files,
            run                 = run,
            seg                 = seg,
        )

    # ------------------------------------------------
    # @classmethod
    # def from_config(cls, job_config: CondorJobConfig, input_output_dict: Dict[str, str]):
    # ------------------------------------------------
    def dict(self):
        """
        Returns a dictionary representation suitable for htcondor.Submit,
        excluding None values.
        """
        # We need to manually construct the dict to include ClassVars if needed
        # by htcondor.Submit, or rely on htcondor.Submit handling them.
        # asdict() only includes instance variables.
        data = {
            # 'accounting_group':      self.accounting_group,
            # 'accounting_group_user': self.accounting_group_user,
            'script':                self.script,
            'arguments':             self.arguments,
            'output_destination':    self.output_destination,
            'log':                   self.log,
            'neventsper':            str(self.neventsper),  # Ensure it's a string if needed
            'payload':               self.payload,
            'rsync':                 self.rsync,
            'mem':                   self.mem,
            'comment':               self.comment,
            'priority':              self.priority,
        }
        if self.batch_name is not None:
            data['batch_name'] = self.batch_name

        # Filter out any potential None values if necessary, though current
        # fields seem mostly required or have defaults.
        return {k: str(v) for k, v in data.items() if v is not None}

# ============================================================================

# # Usage: 
# # Assuming 'my_job_config' is an instance of JobConfig
# condor_job_instance = CondorJob(my_job_config)
# job_dict_for_condor = condor_job_instance.dict()
