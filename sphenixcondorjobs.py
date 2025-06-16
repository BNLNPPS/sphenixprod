from dataclasses import dataclass
from typing import Optional, ClassVar, List
import math
from pathlib import Path
import pprint # noqa: F401

from simpleLogger import ERROR, WARN, CHATTY, INFO, DEBUG  # noqa: F401

# ============================================================================

@dataclass( frozen = True )
class CondorJobConfig:
    """Represents the job configuration block in the YAML.
    Members that are modified by individual jobs are tagged _tmpl
    """
    universe:                   str = "vanilla"
    getenv:                     str = "False"
    environment:                str = None
    executable:                 str = "/bin/echo"
    comment:                    str = None # arbitrary comment
    user_job_wrapper:           str = None # TODO: use this instead of executable for jobwrapper.sh?
    batch_name:                 Optional[str] = None

    request_disk:               str = "10GB"
    request_cpus:               str = "1"
    request_memory:             str = "4000MB"
    priority:                   str = "3500" # higher is better.
    max_retries:                str = None # No default...
    request_xferslots:          str = "1"
    job_lease_duration:         str = "3600"
    requirements:               str = None # '(CPU_Type == "mdc2")'
    periodichold: 	        str = "(NumJobStarts>=1 && JobStatus == 1)"
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

    ### Non-condor data members for steering.
    neventsper:                 int = "0" # number of events per job
    filesystem:                 dict = None  # base filesystem paths - placeholders to be filled in at job creation time (default: _default_filesystem)

    arguments_tmpl:             str = None
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
            'output_destination',
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
    arguments:              str
    outdir:                 str  # where the DST files are written to
    finaldir:               str  # where the DST files are eventually moved to by a spider - currently unused, the spider should know
    histdir:                str  # where histograms go
    output:                 str 
    error:                  str
    log:                    str
    output_file:            str           # Output file for the job --> not used directly except for bookkeeping
    inputs:                 List[str]     # List of input files for the job
    outbase:                str           # Base name for the output file
    logbase:                str           # Base name for the log file
    run:                    int
    seg:                    int
    daqhost:                str
        
    # ------------------------------------------------
    @classmethod
    def make_job(cls,
                output_file: str,
                inputs: List[str],
                outbase: str,
                logbase: str,
                leafdir: str,
                run: int, 
                seg: int,
                daqhost: str,
                ) -> 'CondorJob':
        """
        Constructs a CondorJob instance.
        """
        # Overwrite the input file list. We could hand it over but using the db on the nodes is preferred
        inputs = [ "UsingDbInput" ] # + ",".join(lipsum.generate_words(12000).split()), # for testing, fill up with lorem ipsum
        # Group blocks of 100 runnumbers together to control directory size
        rungroup=cls.job_config.rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100)) 
        arguments = cls.job_config.arguments_tmpl.format(rungroup=rungroup,
                                                leafdir=leafdir,
                                                neventsper=cls.job_config.neventsper,
                                                outbase=outbase,
                                                logbase=logbase,
                                                run=run,
                                                seg=seg,
                                                daqhost=daqhost,
                                                inputs=','.join(inputs),
                                                )
        outdir    = cls.job_config.filesystem['outdir'] .format(rungroup=rungroup, leafdir=leafdir)
        finaldir  = cls.job_config.filesystem['finaldir'].format(rungroup=rungroup, leafdir=leafdir)
        logdir    = cls.job_config.filesystem['logdir'] .format(rungroup=rungroup, leafdir=leafdir)
        histdir   = cls.job_config.filesystem['histdir'] .format(rungroup=rungroup, leafdir=leafdir)
        log       = cls.job_config.log_tmpl.format(rungroup=rungroup, leafdir=leafdir, logbase=logbase)
        output    = f'{logdir}/{logbase}.out'
        error     = f'{logdir}/{logbase}.err'
        # if Path('/.dockerenv').exists() :
        #     WARN("Running in docker")
        #     output:                str = '/Users/eickolja/sphenix/data02/sphnxpro/scratch/kolja/test2/test2.$(ClusterId).$(Process).out'
        #     error:                 str = '/Users/eickolja/sphenix/data02/sphnxpro/scratch/kolja/test2/test2.$(ClusterId).$(Process).err'

        return cls(
            arguments           = arguments,
            outdir              = outdir,
            finaldir            = finaldir,
            histdir             = histdir,
            log                 = log,
            output              = output,
            error               = error,
            outbase             = outbase,
            logbase             = logbase,
            output_file         = output_file,
            inputs              = inputs,
            run                 = run,
            seg                 = seg,
            daqhost             = daqhost,
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
            'arguments':    self.arguments,
            'outdir':       self.outdir,
            'finaldir':     self.finaldir,
            'log':          self.log,
            'output':       self.output,
            'error':        self.error,
        })

        # Filter out any potential None values if necessary, though current
        # fields seem mostly required or have defaults.
        return {k: str(v) for k, v in data.items() if v is not None}

        # ------------------------------------------------
    def condor_row(self):
        """
        Returns a one line string suitable for queue a,b,... from jobrows.in
        FIXME: None values?
        """
        # data = self.job_config.condor_dict() # Repeat base config for each job
        data = {}
        # Add instance-specific fields
        # arguments _must_ come last because it can contain spaces and errors
        # and condor's multi-queue from file mechanism only accepts that as the last, catchall, input 
        data.update({
            'log':                   self.log,
            'output':                self.output,
            'error':                 self.error,
            'arguments':             self.arguments,
        })

        # Filter out any potential None values if necessary, though current
        # fields seem mostly required or have defaults.
        # return ",".join([str(v) for v in data.values()])+"\n"
        return ",".join([str(v) for v in data.values()])

# ============================================================================

# # Usage: 
# # Assuming 'my_job_config' is an instance of JobConfig
# condor_job_instance = CondorJob(my_job_config)
# job_dict_for_condor = condor_job_instance.dict()
