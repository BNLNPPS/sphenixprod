#!/usr/bin/env python

from pathlib import Path
import collections
import pickle
import json

import pprint # noqa F401

from argparsing import monitor_args
from sphenixdbutils import test_mode as dbutils_test_mode
from simpleLogger import slogger, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixmisc import setup_rot_handler
from sphenixcondortools import base_batchname_from_args, monitor_condor_jobs
import htcondor2 as htcondor # type: ignore

def main():
    args = monitor_args()
    #################### Test mode?
    test_mode = (
            dbutils_test_mode
            or args.test_mode
            # or ( hasattr(rule, 'test_mode') and rule.test_mode ) ## allow in the yaml file?
        )

    # Set up submission logging before going any further
    sublogdir=setup_rot_handler(args)
    slogger.setLevel(args.loglevel)
    INFO(f"Logging to {sublogdir}, level {args.loglevel}")

    if test_mode:
        INFO("Running in testbed mode.")
        args.mangle_dirpath = 'production-testbed'
    else:
        INFO("Running in production mode.")

    batch_name=base_batchname_from_args(args)
    jobs=monitor_condor_jobs(batch_name=batch_name, dryrun=args.dryrun)

    # Filter for held jobs (JobStatus == 5)
    held_jobs_ads = [ad for ad in jobs.values() if ad.get('JobStatus') == 5]

    if not held_jobs_ads:
        INFO(f"Found {len(jobs)} total jobs, but none are currently held.")
        return

    INFO(f"Found {len(jobs)} total jobs, {len(held_jobs_ads)} of which are held.")

    held_memory_usage = []
    held_request_memory = []
    kill_suggestion = []
    under_memory_hold_reasons = collections.Counter()
    for job_ad in held_jobs_ads:
        # MemoryUsage and RequestMemory are in MB
        mu = int(job_ad.get('ResidentSetSize', 0))/1024  # Convert from KB to MB
        rm = int(job_ad.get('MemoryProvisioned', 0))
        held_memory_usage.append(mu)
        held_request_memory.append(rm)
        # If memory usage is below request, it's interesting to see why it's held.
        if mu < rm:
            hold_reason = job_ad.get('HoldReason', 'Not Available')
            job_id = f"{job_ad.get('ClusterId')}.{job_ad.get('ProcId')}"
            DEBUG(f"Job {job_id} held with mu ({mu:.0f}MB) < rm ({rm}MB). Reason: {hold_reason}")
            reason_code = job_ad.get('LastHoldReasonCode', 0) # Default to 0 (None)
            if reason_code !=26 :
                WARN(f'Job {job_id} held with mu ({mu:.0f}MB) < rm ({rm}MB). Reason Code {reason_code}:\n\t"{hold_reason}"')
            under_memory_hold_reasons[reason_code] += 1

        # Now let's kill and resubmit this job
        # Fix difference between Submit object and ClassAd keys
        job_ad['output']=job_ad.pop('Out')
        job_ad['error']=job_ad.pop('Err')
        # adjust memory request
        new_submit_ad = htcondor.Submit(dict(job_ad))
        if args.memory:
            new_rm=int(args.memory)
        else:
            new_rm=int(rm)
            new_rm=int(new_rm * args.memory_scale_factor)

            if new_rm > args.max_memory:
                WARN(f"Calculated new memory request {new_rm}MB exceeds maximum of {args.max_memory}MB. Skipping.")
                #kill_suggestion.append(f"{job_ad['ClusterId']}.{job_ad['ProcId']}")
                kill_suggestion.append(job_ad)
                continue
        new_submit_ad['RequestMemory'] = str(new_rm)
        if args.resubmit:
            if not args.dryrun:
                schedd = htcondor.Schedd()
                try:
                    # The transaction context manager is deprecated. The following replacement operations are not atomic.
                    schedd.act(htcondor.JobAction.Remove, [f"{job_ad['ClusterId']}.{job_ad['ProcId']}"])
                    INFO(f"Removed held job {job_ad['ClusterId']}.{job_ad['ProcId']} from queue.")
                    submit_result = schedd.submit(new_submit_ad)
                    new_queue_id = submit_result.cluster()
                    INFO(f"Resubmitted job with increased memory request ({rm}MB -> {new_rm}MB) as {new_queue_id}.")                    
                except Exception as e:
                    ERROR(f"Failed to remove and resubmit job {job_ad['ClusterId']}.{job_ad['ProcId']}: {e}")
            else:
                INFO(f"(Dry Run) Would remove held job {job_ad['ClusterId']}.{job_ad['ProcId']} and resubmit with RequestMemory={new_rm}MB.")

    if kill_suggestion:
        kill_procs=[f"{job_ad['ClusterId']}.{job_ad['ProcId']}" for job_ad in kill_suggestion]
        INFO(f"There were {len(kill_suggestion)} jobs that could not be resubmitted due to exceeding max memory.")
        if args.kill:
            INFO(f"Killing them now as per --kill option.")
            if not args.dryrun:
                schedd = htcondor.Schedd()
                # with open(f"{batch_name}_killed_jobs.pkl", "wb") as f:
                #     pickle.dump(kill_suggestion, f)
                # with open(f"{batch_name}_killed_jobs.json", "w") as f:
                #     for job_ad in kill_suggestion:
                #         json.dump(dict(job_ad), f, indent=4)
                try:
                    schedd.act(htcondor.JobAction.Remove, kill_procs)
                    INFO(f"Killed {len(kill_suggestion)} jobs that exceeded max memory limit of {args.max_memory}MB.")
                except Exception as e:
                    ERROR(f"Failed to kill jobs: {e}")
        else:
            INFO(f"You may want to kill them manually: \n{' '.join(kill_procs)}")


    INFO(f"{Path(__file__).name} DONE.")

if __name__ == '__main__':
    main()
    exit(0)
