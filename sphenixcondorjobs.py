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
    universe:                   str = "vanilla"
    getenv:                     str = "False"
    environment:                str = None
    # executable:                 str = "./jobwrapper.sh"
    executable:                 str = "/bin/echo"
    comment:                    str = None # arbitrary comment
    user_job_wrapper:           str = None # TODO: use this instead of executable for jobwrapper.sh?
    # batch_name:                 Optional[str] = None 
    batch_name:                 str = "kolja.test"

    request_disk:               str = "10GB"
    request_cpus:               str = "1"
    request_memory:             str = "4000MB"
    priority:                   str = "3500" # higher is better.
    max_retries:                str = None # No default...
    request_xferslots:          str = None
    job_lease_duration:         str = "3600"
    requirements:               str = '(CPU_Type == "mdc2")'
    periodichold: 	            str = "(NumJobStarts>=1 && JobStatus == 1)"
    periodicremove:             str = None
    
    on_exit_hold:               str = None
    on_exit_remove:             str = None
    concurrency_limits:         str = None
    notification:               str = None
    notify_user:                str = None

    # 04/25/2025: accounting_group and accounting_group_user should no longer be set, 
    # submit host will do this automatically.
    # accounting_group:         str = 'group_sphenix.mdc2'
    # accounting_group_user:    str = 'sphnxpro'
    accounting_group:           str = None
    accounting_group_user:      str = None
    initialdir:                 str = None
    # should_transfer_files:      str = "YES" # TODO: check why this is needed
    # when_to_transfer_output:    str = "ON_EXIT"
    # transfer_output_files:      str = '""'
    transfer_output_remaps:     str = None
    transferout:                str = "false"
    transfererr:                str = "false"
    transfer_input_files:       str = None
    
    script:                     str = None # run script on the worker node
    payload:                    str = None # Working directory on the node; transferred by condor
    neventsper:                 int = "0" # number of events per job
    rsync:                      str = None # additional files to rsync to the node

    arguments_tmpl:             str = None
    output_destination_tmpl:    str = None
    log_tmpl:                   str = None
    rungroup_tmpl:              str = "run_{a:08d}_{b:08d}"

    def condor_dict(self) -> dict:
        """
        Returns a dictionary representation suitable for base HTCondor job configuration,
        excluding None values and template/internal fields.
        """
        # List of attributes that correspond directly to HTCondor parameters, in desired order
        condor_attributes = [
            'universe',
            'executable',
            'environment',
            'getenv',
            'initialdir',
            'requirements',
            'priority',
            'request_disk',
            'request_cpus',
            'request_memory',
            'request_xferslots',
            'job_lease_duration',
            'max_retries',
            'periodichold',
            'periodicremove',
            'on_exit_hold',
            'on_exit_remove',
            'concurrency_limits',
            'notification',
            'notify_user',
            'should_transfer_files',
            'when_to_transfer_output',
            'transfer_output_files',
            'transfer_output_remaps',
            'transfer_input_files',
            'accounting_group', # Keep last as they are often None/unused now
            'accounting_group_user',
            'comment', # Comment often comes last in submit files
        ]

        data = {attr_name: getattr(self, attr_name)
                for attr_name in condor_attributes
                if hasattr(self, attr_name)}

        # Handle special cases like batch_name
        if self.batch_name is not None:
            data['JobBatchName'] = self.batch_name # Use +JobBatchName for HTCondor

        # Filter out None values and convert remaining values to strings
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
    # output:                str = None 
    # error:                 str = None
    log:                   str
    output_file:           str           # Output file for the job --> not used directly except for bookkeeping
    inputs:                List[str]     # List of input files for the job
    outbase:               str           # Base name for the output file
    logbase:               str           # Base name for the log file
    run:                   int
    seg:                   int
    output:                str = '/sphenix/u/sphnxpro/kolja/sphenixprod/test/test.$(Process).out'
    error:                 str = '/sphenix/u/sphnxpro/kolja/sphenixprod/test/test.$(Process).err'


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
            'output':                self.output,
            'error':                 self.error,
        })

        # Filter out any potential None values if necessary, though current
        # fields seem mostly required or have defaults.
        return {k: str(v) for k, v in data.items() if v is not None}

# ============================================================================

# # Usage: 
# # Assuming 'my_job_config' is an instance of JobConfig
# condor_job_instance = CondorJob(my_job_config)
# job_dict_for_condor = condor_job_instance.dict()
