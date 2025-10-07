#!/usr/bin/env python

from pathlib import Path
from datetime import datetime, timedelta
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

    # for ad in jobs.values():
    #     print(datetime.fromtimestamp(ad.get('EnteredCurrentStatus')))
    #     exit()
    # exit()

    # Filter for any desired quality here
    filtered_jobs_ads = []
    cutoff = datetime.now() - timedelta(hours=48)
    for ad in jobs.values():
        t = datetime.fromtimestamp(ad.get('EnteredCurrentStatus'))
        if t > cutoff:
            filtered_jobs_ads.append(ad)

    if not filtered_jobs_ads:
        INFO(f"Found {len(jobs)} total jobs, but none qualify.")
        return
    INFO(f"Found {len(jobs)} total jobs; filtered {len(filtered_jobs_ads)} for further treatment")

    for job_ad in filtered_jobs_ads:
        # Now let's kill and resubmit this job
        # Fix difference between Submit object and ClassAd keys
        job_ad['output']=job_ad.pop('Out')
        job_ad['error']=job_ad.pop('Err')
        new_submit_ad = htcondor.Submit(dict(job_ad))

        # Change what you want changed. Eg, nCPU
        new_submit_ad['RequestCpus'] = '1'
        if args.resubmit:
            if not args.dryrun:
                schedd = htcondor.Schedd()
                try:
                    # The transaction context manager is deprecated. The following replacement operations are not atomic.
                    schedd.act(htcondor.JobAction.Remove, [f"{job_ad['ClusterId']}.{job_ad['ProcId']}"])
                    INFO(f"Removed held job {job_ad['ClusterId']}.{job_ad['ProcId']} from queue.")
                    submit_result = schedd.submit(new_submit_ad)
                    new_queue_id = submit_result.cluster()
                except Exception as e:
                    ERROR(f"Failed to remove and resubmit job {job_ad['ClusterId']}.{job_ad['ProcId']}: {e}")
            else:
                INFO(f"(Dry Run) Would remove held job {job_ad['ClusterId']}.{job_ad['ProcId']} and resubmit with RequestMemory={new_rm}MB.")

    INFO(f"{Path(__file__).name} DONE.")

if __name__ == '__main__':
    main()
    exit(0)
