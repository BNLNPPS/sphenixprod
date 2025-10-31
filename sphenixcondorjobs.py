from dataclasses import dataclass, make_dataclass, field, asdict
from typing import Optional, ClassVar, List, Any
import math
import pprint # noqa: F401

from simpleLogger import ERROR, WARN, CHATTY, INFO, DEBUG  # noqa: F401

# ============================================================================
# CondorJobConfig. Members that are modified by individual jobs are tagged _tmpl.
# Implemented via list and make_dataclass for simple and dynamic use of the available fields.
# The list of (name, type, Field) tuples for make_dataclass
#    echo "Usage: $0 <buildarg> <dataset> <intriplet> <indsttype> <run> <seg> <daqhost> <inputs>"
#    echo "       <nevents> <outdir> <histdir> <outbase> <neventsper> <dbtag>"
#    echo "       <logbase> <logdir> <condor_rsync> [dbid]"

glob_arguments_tmpl ="{buildarg} {dataset} {intriplet} {indsttype_str} {run} {seg} {daqhost} {inputs} "
glob_arguments_tmpl+="{nevents} {outdir} {histdir} {outbase} {neventsper} {dbtag} "
glob_arguments_tmpl+="{logbase} {logdir} {payload} "
CondorJobConfig_fields = [
    # --- Core Job Attributes ---
    ('universe',               str,            field(default="vanilla")),
    ('getenv',                 str,            field(default="False")),
    ('environment',            str,            field(default=None)),
    ('executable',             str,            field(default="/bin/echo")),
    ('comment',                str,            field(default=None)),
    ('user_job_wrapper',       str,            field(default=None)),
    ('batch_name',             Optional[str],  field(default=None)),

    # --- Resource Requests ---
    ('request_cpus',           str,            field(default="1")),
    ('request_xferslots',      str,            field(default=None)),
    ('request_memory',         str,            field(default="4000MB")),
    ('request_disk',           str,            field(default="10GB")),
    ('priority',               str,            field(default="3500")),
    ('job_lease_duration',     str,            field(default="3600")),
    ('max_retries',            str,            field(default=None)),

    # --- Job Lifecycle and Policy ---
    ('requirements',           str,            field(default=None)),
    ('periodichold',           str,            field(default="(NumJobStarts>=1 && JobStatus == 1)")),
    ('periodicremove',         str,            field(default=None)),
    ('on_exit_hold',           str,            field(default=None)),
    ('on_exit_remove',         str,            field(default=None)),
    ('concurrency_limits',     str,            field(default=None)),
    ('notification',           str,            field(default=None)),
    ('notify_user',            str,            field(default=None)),

    # --- Accounting and File Transfer ---
    ('accounting_group',       str,            field(default=None)),
    ('accounting_group_user',  str,            field(default=None)),
    ('initialdir',             str,            field(default=None)),
    ('transfer_output_remaps', str,            field(default=None)),
    ('transferout',            str,            field(default="false")),
    ('transfererr',            str,            field(default="false")),
    ('transfer_input_files',   str,            field(default=None)),

    # --- Non-Condor Data Members for Steering ---
    ('neventsper',             int,            field(default=None)),
    ('filesystem',             dict,           field(default=None)),
    ('arguments_tmpl',         str,            field(default=None)),
    ('log_tmpl',               str,            field(default=None)),
    ('rungroup_tmpl',          str,            field(default="run_{a:08d}_{b:08d}")),
]
CondorJobConfig_fieldnames= { f[0] for f in CondorJobConfig_fields }

# ----------------------------------------------------------------------------
def condor_dict(self):
    """
    Returns a dictionary representation suitable for base HTCondor job configuration,
    excluding None values and template/internal fields.
    - Passed as  namespace dictionary to make_dataclass
    - A dictionary keeps desired order (for readability) intact
    """
    all_fields = asdict(self)
    ignore_fields = { 'neventsper','filesystem','arguments_tmpl','log_tmpl','rungroup_tmpl' }

    return {key: value for key, value in all_fields.items() if key not in ignore_fields and value is not None}
# ----------------------------------------------------------------------------
CondorJobConfig = make_dataclass(
    'CondorJobConfig',
    CondorJobConfig_fields,
    namespace={'condor_dict': condor_dict}
)

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
    job_config:            ClassVar[Any]  # CondorJobConfig created dynamically via make_dataclass

    # --- Instance Variables (Specific to each job) ---
    arguments:              str
    outdir:                 str  # where the DST files are written to
    finaldir:               str  # where the DST files are eventually moved to by a spider - currently unused, the spider should know
    histdir:                str  # where histograms go
    output:                 str  # Stdout file for condor
    error:                  str  # Stderr file for condor
    log:                    str  # Log file for condor
    output_file:            str           # Output file for the job --> not used directly except for bookkeeping
    inputs:                 List[Any]     # Can be list of input files for the job; usually holds a steering string or flag though.
    outbase:                str           # Base name for the output file
    logbase:                str           # Base name for the log file
    run:                    int
    seg:                    int
    daqhost:                str

    # ------------------------------------------------
    @classmethod
    def make_job(cls,
                output_file: str,
                run: int,
                seg: int,
                daqhost: str,
                inputs: List[str],
                leafdir: str,
                outbase: str,
                logbase: str,
                ) -> 'CondorJob':
        """
        Constructs a CondorJob instance.
        """
        # Group blocks of 100 runnumbers together to control directory size
        rungroup=cls.job_config.rungroup_tmpl.format(a=100*math.floor(run/100), b=100*math.ceil((run+1)/100))
        arguments = cls.job_config.arguments_tmpl.format(
            outbase=outbase,
            logbase=logbase,
            run=run,
            seg=seg,
            daqhost=daqhost,
            leafdir=leafdir,
            rungroup=rungroup,
            neventsper=cls.job_config.neventsper,
            inputs=",".join(inputs),
        )
        outdir    = cls.job_config.filesystem['outdir'] .format(rungroup=rungroup, leafdir=leafdir)
        finaldir  = cls.job_config.filesystem['finaldir'].format(rungroup=rungroup, leafdir=leafdir)
        logdir    = cls.job_config.filesystem['logdir'] .format(rungroup=rungroup, leafdir=leafdir)
        histdir   = cls.job_config.filesystem['histdir'] .format(rungroup=rungroup, leafdir=leafdir)
        log       = cls.job_config.log_tmpl.format(rungroup=rungroup, leafdir=leafdir, logbase=logbase)

        output    = f'{logdir}/{logbase}.out'
        error     = f'{logdir}/{logbase}.err'

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
