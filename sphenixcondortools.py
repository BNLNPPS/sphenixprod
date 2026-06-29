#!/usr/bin/env python
import argparse
import subprocess
from pathlib import Path
import re
import pprint # noqa F401

from argparsing import monitor_args
from simpleLogger import slogger, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixprodrules import RuleConfig # New import
from sphenixmatching import MatchConfig
from sphenixmisc import setup_rot_handler, should_I_quit, shell_command # Modified import
import htcondor2 as htcondor  # type: ignore

_common_runscript_arg_count = None

def common_runscript_arg_count():
    global _common_runscript_arg_count
    if _common_runscript_arg_count is not None:
        return _common_runscript_arg_count

    prep_script = Path(__file__).with_name('common_runscript_prep.sh')
    try:
        for line in prep_script.read_text().splitlines():
            match = re.match(r'\s*ARG_COUNT\s*=\s*(\d+)\b', line)
            if match:
                _common_runscript_arg_count = int(match.group(1))
                return _common_runscript_arg_count
    except OSError as e:
        WARN(f"Could not read {prep_script}: {e}")
        return None

    WARN(f"Could not find ARG_COUNT in {prep_script}. Production DB ids will not be inferred from Condor Args.")
    return None

def production_dbid_from_job_ad(job_ad):
    condor_id = f"{job_ad.get('ClusterId')}.{job_ad.get('ProcId')}"
    batch_name = str(job_ad.get('JobBatchName', ''))
    if '.' not in batch_name:
        WARN(f"Job {condor_id} batch name '{batch_name}' has no dot. Proceeding without production DB updates.")
        return None

    expected_arg_count = common_runscript_arg_count()
    if expected_arg_count is None:
        return None
    expected_arg_count += 1  # common runscript args plus production dbid

    args = str(job_ad.get('Args', '')).split()
    if len(args) != expected_arg_count:
        WARN(
            f"Job {condor_id} has {len(args)} Args tokens, expected {expected_arg_count} "
            f"for a production job. Proceeding without production DB updates."
        )
        return None

    dbid = args[-1]
    if not dbid.isdigit():
        WARN(f"Job {condor_id} final Args token '{dbid}' is not a production dbid. Proceeding without DB updates.")
        return None
    return int(dbid)

condor_monitor_attrs = [
    'ClusterId', 'ProcId', # access via job_id = f"{ClusterId}.{ProcId}"
        # Interesting Statistics
    'JobStatus',  'QDate', 'CompletionDate',
    'ExitCode', 'HoldReason', 'RemoveReason',
    'RemoteHost', 'NumJobStarts',
    'ResidentSetSize', 'MemoryProvisioned', 'LastHoldReasonCode',
    'EnteredCurrentStatus',
        # Important for cloning
    'Owner', 'JobBatchName','Environment', 'JobPrio',
    'Cmd', 'Args', 'Iwd',
    'JobSubmitFile', 'Out', 'Err', 'UserLog',
    'RequestCpus', 'RequestDisk', 'RequestMemory', 'Requestxferslots'
]

def map_condor_ads(jobs) -> dict:
    ad_by_id = {}
    dbid_count = 0
    for ad in jobs:
        condor_id = f"{ad.get('ClusterId')}.{ad.get('ProcId')}"
        dbid = production_dbid_from_job_ad(ad)
        if dbid is None:
            ad_by_id[condor_id] = ad
            continue
        if dbid in ad_by_id:
            WARN(f"Duplicate production dbid {dbid} found in Condor queue. Keeping job {condor_id} under Condor id.")
            ad_by_id[condor_id] = ad
        else:
            ad_by_id[dbid] = ad
            dbid_count += 1
    INFO(f"Mapped {len(ad_by_id)} jobs, {dbid_count} by production dbid.")
    return ad_by_id

def condor_id_constraint(condor_ids) -> str:
    constraints = []
    for condor_id in condor_ids:
        try:
            cluster, proc = condor_id.split(".", 1)
            cluster = int(cluster)
            proc = int(proc)
        except ValueError:
            ERROR(f"Invalid Condor id '{condor_id}'. Expected ClusterId.ProcId, e.g. 194490.13.")
            exit(2)
        constraints.append(f"(ClusterId == {cluster} && ProcId == {proc})")
    return " || ".join(constraints)

def monitor_condor_jobs(batch_name: str, dryrun: bool=True) -> dict:
    """
    Check on the status of held jobs and process them using the htcondor2 bindings.
    """
    INFO("Polling for all condor jobs using htcondor2 python bindings...")
    
    try:
        schedd = htcondor.Schedd()

        # batch_pattern = f'.*\\.{batch_name}$' ## Assumes batch_name does NOT contain 'main.' prefix
        batch_pattern = f'.*{batch_name}$'      ## Assumes batch_name MAY contain any prefix

        # Query all jobs for the batch, we will filter by status locally
        constraint = f'regexp("{batch_pattern}", JobBatchName)'
        INFO(f"Querying condor with constraint: {constraint}")

        jobs = schedd.query(constraint=constraint, projection=condor_monitor_attrs)

        if not jobs:
            INFO("No jobs found for the specified batch name.")
            return {}

        INFO(f"Found {len(jobs)} jobs for batch_name {batch_name}.")
    except Exception as e:
        ERROR(f"An unexpected error occurred during condor query: {e}")
        exit(1)

    return map_condor_ads(jobs)

def monitor_condor_jobs_by_ids(condor_ids, dryrun: bool=True) -> dict:
    """
    Query specific Condor jobs by ClusterId.ProcId.
    """
    INFO("Polling for selected condor jobs using htcondor2 python bindings...")

    try:
        schedd = htcondor.Schedd()
        constraint = condor_id_constraint(condor_ids)
        INFO(f"Querying condor with constraint: {constraint}")
        jobs = schedd.query(constraint=constraint, projection=condor_monitor_attrs)

        if not jobs:
            INFO("No jobs found for the specified Condor ids.")
            return {}

        requested = set(condor_ids)
        found = {f"{ad.get('ClusterId')}.{ad.get('ProcId')}" for ad in jobs}
        missing = sorted(requested - found)
        if missing:
            WARN(f"{len(missing)} requested Condor id(s) were not found: {' '.join(missing)}")
        INFO(f"Found {len(jobs)} selected Condor jobs.")
    except Exception as e:
        ERROR(f"An unexpected error occurred during condor query: {e}")
        exit(1)

    return map_condor_ads(jobs)

# ============================================================================================
def get_queued_jobs(rule: RuleConfig):
    """
    Determines the number of jobs currently in the condor queue for a given rule.
    """
    # Determine what's already in "idle"
    # Note: For this, we cannot use runnumber cuts, too difficult (and expensive) to get from condor.
    # Bit of a clunky method. But it works and doesn't get called all that often.
    cq_query  =  'condor_q'
    cq_query += f" -constraint \'JobBatchName==\"{rule.job_config.batch_name}\"' "  # Select our batch
    cq_query +=  ' -format "%d." ClusterId -format "%d\\n" ProcId'                  # any kind of one-line-per-job output. e.g. 6398.10

    try:
        all_procs = shell_command(cq_query, raise_on_error=True)
    except subprocess.CalledProcessError as e:
        CRITICAL(f"condor_q failed (exit {e.returncode}) — condor infrastructure problem. Command: {cq_query.strip()}")
        return -1
    return len(all_procs)

# ============================================================================================



# ============================================================================================
def base_batchname_from_args(args: argparse.Namespace) -> str:
    if args.base_batchname is not None:
        return args.base_batchname

    # Prepare param_overrides for RuleConfig
    param_overrides = {}
    param_overrides["runs"] = args.runs
    param_overrides["runlist"] = args.runlist
    param_overrides["nevents"] = 0 # Not relevant, but needed for the RuleConfig ctor

    if args.physicsmode is not None:
        param_overrides["physicsmode"] = args.physicsmode

    # filesystem is the base for all output, allow for mangling here
    # "production" (in the default filesystem) is replaced
    param_overrides["prodmode"] = "production"
    if args.mangle_dirpath:
        param_overrides["prodmode"] = args.mangle_dirpath

    # Load specific rule from the given yaml file.
    try:
        rule = RuleConfig.from_yaml_file(
            yaml_file=args.config,
            rule_name=args.rulename,
            param_overrides=param_overrides
        )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error loading rule configuration: {e}")
        exit(1)

    # Create a match configuration from the rule
    INFO("Match configuration created.")

    # Call the main monitoring function
    batch_name=rule.job_config.batch_name # usually starts with "main." or so. Remove that
    batch_name=batch_name.split(".", 1)[-1]
    return batch_name

def main():
    args = monitor_args()

    # Set up submission logging before going any further
    sublogdir=setup_rot_handler(args)
    slogger.setLevel(args.loglevel)
    INFO(f"Logging to {sublogdir}, level {args.loglevel}")
    INFO("Running in production mode.")

    monitor_condor_jobs(batch_name=base_batchname_from_args(args), dryrun=args.dryrun)
    INFO(f"{Path(__file__).name} DONE.")

if __name__ == '__main__':
    main()
    exit(0)
