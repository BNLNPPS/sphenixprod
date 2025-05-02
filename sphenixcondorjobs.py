from dataclasses import dataclass
from typing import Optional, ClassVar, List
import math
from simpleLogger import ERROR, WARN, CHATTY, INFO, DEBUG  # noqa: F401

import pprint # noqa F401

# ============================================================================

@dataclass( frozen = True )
class CondorJobConfig:
    """Represents the job configuration block in the YAML.
    Members that are modified by individual jobs are tagged _tmpl
    """
    script:                str  # run script on the worker node
    payload:               str  # Working directory on the node; transferred by condor
    neventsper:            int  # number of events per job
    rsync:                 str  # additional files to rsync to the node
    mem:                   str  # "4000MB"
    request_disk:          str  # "10GB"
    comment:               str  # arbitrary comment
    batch_name:            Optional[str]
    priority:              str
    arguments_tmpl:             str
    output_destination_tmpl:    str
    log_tmpl:                   str
    rungroup_tmpl:              str = "run_{a:08d}_{b:08d}"

    # 04/25/2025: accounting_group and accounting_group_user should no longer be set, 
    # submit host will do this automatically.
    # accounting_group:      ClassVar[str] = 'group_sphenix.mdc2'
    # accounting_group_user: ClassVar[str] = 'sphnxpro'

    def condor_dict(self) -> dict:
        """
        Returns a dictionary representation suitable for base HTCondor job configuration,
        excluding None values and template fields.
        """
        data = {
            # 'should_transfer_files': 'YES',
            # 'when_to_transfer_output': 'ON_EXIT',
            # 'transfer_input_files':  ','.join(self.inputs),
            # 'transfer_output_files': self.output_file,
            # 'transfer_executable':   'NO',
            # 'executable':           self.script,
            # 'output':               self.output_destination,
            # 'error':                self.log,
            # 'log':                  self.log,
            # executable:              f"{`SLURPPATH}/jobwrapper.sh"
            # 'accounting_group':      self.accounting_group,
            # 'accounting_group_user': self.accounting_group_user,
            'universe':              'vanilla',
            'executable':            self.script,
            'request_memory':        self.mem,
            'request_disk':          self.request_disk,
            'priority':              self.priority,
            'comment':               self.comment,
        }
        # FIXME? Leads to MY.JobBatchName = $(name)_$(build)_$(tag)_$(version)-singlestreams
        # if self.batch_name is not None:
        #     data['+JobBatchName'] = self.batch_name # Use +JobBatchName for HTCondor

        return {k: str(v) for k, v in data.items() if v is not None}

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

    # --- Class Variable (Shared across all instances) ---
    job_config:            ClassVar[CondorJobConfig]

    # --- Instance Variables (Specific to each job) ---
    arguments:             str
    output_destination:    str           
    log:                   str
    output_file:           str           # Output file for the job --> not used directly except for bookkeeping
    inputs:                List[str]     # List of input files for the job
    outbase:               str           # Base name for the output file
    logbase:               str           # Base name for the log file
    run:                   int
    seg:                   int

    # ------------------------------------------------
    @classmethod
    def make_job(cls,
                output_file: str,
                inputs: List[str],
                outbase: str,
                logbase: str,
                leafdir: str,
                run: int, seg: int):
        """
        Constructs a CondorJob instance.
        """

        rungroup=cls.job_config.rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
        arguments = cls.job_config.arguments_tmpl.format(rungroup=rungroup,
                                                leafdir=leafdir,
                                                neventsper=cls.job_config.neventsper,
                                                outbase=outbase,
                                                logbase=logbase,
                                                run=run,
                                                seg=seg,
                                                inputs=','.join( inputs),
                                                )
        output_destination  = cls.job_config.output_destination_tmpl.format(rungroup=rungroup,
                                                                        leafdir=leafdir,
                                                                        )
        log                 = cls.job_config.log_tmpl.format(rungroup=rungroup,
                                                        leafdir=leafdir,
                                                        logbase=logbase,
                                                        )
        return cls(
            arguments           = arguments,
            output_destination  = output_destination,
            log                 = log,
            outbase             = outbase,
            logbase             = logbase,
            output_file         = output_file,
            inputs              = inputs,
            run                 = run,
            seg                 = seg,
        )

    # ------------------------------------------------
    def dict(self):
        """
        Returns a dictionary representation suitable for htcondor.Submit,
        excluding None values.
        """
        # data = self.job_config.condor_dict() # Repeat base config for each job
        data = {}
        # Add instance-specific fields
        data.update({
            'arguments':             self.arguments,
            'output_destination':    self.output_destination,
            'log':                   self.log,
        })

        # Filter out any potential None values if necessary, though current
        # fields seem mostly required or have defaults.
        return {k: str(v) for k, v in data.items() if v is not None}

# ============================================================================

# # Usage: 
# # Assuming 'my_job_config' is an instance of JobConfig
# condor_job_instance = CondorJob(my_job_config)
# job_dict_for_condor = condor_job_instance.dict()
