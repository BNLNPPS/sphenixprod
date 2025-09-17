#!/usr/bin/env python

from pathlib import Path
import matplotlib as mpl # type: ignore
import matplotlib.pyplot as plt # type: ignore
from matplotlib.colors import LogNorm # type: ignore
import numpy as np # type: ignore
import collections

import pprint # noqa F401

from argparsing import monitor_args
from sphenixdbutils import test_mode as dbutils_test_mode
from simpleLogger import slogger, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixmisc import setup_rot_handler
from sphenixcondortools import base_batchname_from_args, monitor_condor_jobs
import htcondor2 as htcondor # type: ignore

def plot_memory_distribution(memory_usage, request_memory, output_file):
    """Generates and saves a histogram of memory usage vs. requested memory."""
    if not memory_usage and not request_memory:
        INFO("No memory data to plot for distribution.")
        return

    mpl.rcParams['axes.formatter.useoffset'] = False

    plt.style.use('seaborn-v0_8-deep')
    fig, ax = plt.subplots(figsize=(12, 7))

    max_val = max(np.max(memory_usage) if memory_usage else 0, np.max(request_memory) if request_memory else 0)
    bins = np.linspace(0, max_val, 50)

    if memory_usage:
        ax.hist(memory_usage, bins=bins, alpha=0.7, label=f'Memory Usage (Avg: {np.mean(memory_usage):.0f} MB)')
    if request_memory:
        ax.hist(request_memory, bins=bins, alpha=0.7, label=f'Requested Memory (Avg: {np.mean(request_memory):.0f} MB)')

    ax.set_title('Distribution of Memory Usage and Request for Held Jobs')
    ax.set_xlabel('Memory (MB)')
    ax.set_ylabel('Number of Jobs')
    ax.legend()
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)

    plt.tight_layout()
    plt.savefig(output_file)
    plt.close(fig)
    INFO(f"Saved memory distribution plot to {output_file}")

def plot_memory_boxplot(memory_usage, request_memory, output_file):
    """Generates and saves a boxplot of memory usage vs. requested memory."""
    if not memory_usage and not request_memory:
        INFO("No memory data to plot for boxplot.")
        return

    plt.style.use('seaborn-v0_8-deep')
    fig, ax = plt.subplots(figsize=(10, 7))

    data_to_plot = []
    labels = []
    if memory_usage:
        data_to_plot.append(memory_usage)
        labels.append('Memory Usage')
    if request_memory:
        data_to_plot.append(request_memory)
        labels.append('Requested Memory')

    ax.boxplot(data_to_plot, patch_artist=True)

    ax.set_title('Box Plot of Memory Usage and Request for Held Jobs')
    ax.set_ylabel('Memory (MB)')
    ax.set_xticklabels(labels)
    ax.yaxis.grid(True, linestyle='--', which='major', color='grey', alpha=0.5)

    plt.tight_layout()
    plt.savefig(output_file)
    plt.close(fig)
    INFO(f"Saved memory box plot to {output_file}")

def plot_memory_scatterplot(memory_usage, request_memory, output_file):
    """Generates and saves a 2D histogram of memory usage vs. requested memory."""
    if not memory_usage or not request_memory:
        INFO("Not enough data to plot 2D histogram.")
        return

    plt.style.use('seaborn-v0_8-deep')
    fig, ax = plt.subplots(figsize=(11, 10))

    x_data = np.array(request_memory)
    y_data = np.array(memory_usage)

    # Create the 2D histogram. A logarithmic color scale is used to handle skewed distributions.
    counts, xedges, yedges, im = ax.hist2d(x_data, y_data, bins=50, cmap='viridis', norm=LogNorm())

    # Add a color bar to show the number of jobs in each bin
    fig.colorbar(im, ax=ax, label='Number of Jobs')

    # Add a y=x reference line for easy comparison
    max_val = max(np.max(x_data) if len(x_data) > 0 else 0, np.max(y_data) if len(y_data) > 0 else 0)
    ax.plot([0, max_val], [0, max_val], 'r--', label='Requested = Used (y=x)')

    ax.set_title('Memory Usage vs. Requested Memory for Held Jobs (2D Histogram)')
    ax.set_xlabel('Requested Memory (MB)')
    ax.set_ylabel('Actual Memory Usage (MB)')
    ax.legend()
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax.set_aspect('equal', adjustable='box')

    plt.tight_layout()
    plt.savefig(output_file)
    plt.close(fig)
    INFO(f"Saved memory 2D histogram to {output_file}")

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
        new_rm=int(rm * 1.5)  # Increase request by 50%
        if new_rm > args.max_memory:
            WARN(f"Calculated new memory request {new_rm}MB exceeds maximum of {args.max_memory}MB. Skipping.")
            kill_suggestion.append(f"{job_ad['ClusterId']}.{job_ad['ProcId']}")
            continue
        new_submit_ad['RequestMemory'] = str(new_rm)
        if not args.dryrun:
            schedd = htcondor.Schedd()
            try:
                # The transaction context manager is deprecated. The following replacement operations are not atomic.
                schedd.act(htcondor.JobAction.Remove, [f"{job_ad['ClusterId']}.{job_ad['ProcId']}"])
                INFO(f"Removed held job {job_ad['ClusterId']}.{job_ad['ProcId']} from queue.")
                submit_result = schedd.submit(new_submit_ad)
                new_queue_id = submit_result.cluster()
                INFO(f"Resubmitted job with increased memory request ({rm}MB -> {new_rm}MB) as {new_queue_id}.")
                
                exit()
            except Exception as e:
                ERROR(f"Failed to remove and resubmit job {job_ad['ClusterId']}.{job_ad['ProcId']}: {e}")
        else:
            INFO(f"(Dry Run) Would remove held job {job_ad['ClusterId']}.{job_ad['ProcId']} and resubmit with RequestMemory={new_rm}MB.")

    if held_memory_usage or held_request_memory:
        dist_plot_file = f"{batch_name}_memory_distribution.png"
        box_plot_file  = f"{batch_name}_memory_boxplot.png"
        scatter_plot_file = f"{batch_name}_memory_scatterplot.png"
        plot_memory_distribution(held_memory_usage, held_request_memory, dist_plot_file)
        plot_memory_boxplot(held_memory_usage, held_request_memory, box_plot_file)
        plot_memory_scatterplot(held_memory_usage, held_request_memory, scatter_plot_file)

    if under_memory_hold_reasons:
        INFO("Frequency of hold reason codes for jobs held while under memory request:")
        pprint.pprint(dict(under_memory_hold_reasons))

    if kill_suggestion:
        INFO(f"There were {len(kill_suggestion)} jobs that could not be resubmitted due to exceeding max memory.")
        INFO(f"You may want to kill them manually: \n{', '.join(kill_suggestion)}")

    INFO(f"{Path(__file__).name} DONE.")

if __name__ == '__main__':
    main()
    exit(0)