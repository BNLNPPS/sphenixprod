#!/usr/bin/env python

import os
import sys
from datetime import datetime
import yaml

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages

from argparsing import submission_args
from sphenixdbutils import dbQuery, cnxn_string_map, list_to_condition
from sphenixprodrules import RuleConfig
from sphenixmisc import setup_rot_handler
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401


def get_time_diffs(run_condition, dsttype, tag, dataset):
    query = f"""
    SELECT started, finished
    FROM production_jobs
    WHERE {run_condition}
      AND tag = '{tag}'
      AND dataset = '{dataset}'
      AND status = 'finished'
      AND dsttype LIKE '{dsttype}%'
      AND started IS NOT NULL
      AND finished IS NOT NULL
    """
    
    DEBUG(f"Executing query:\n{query}")

    cursor = dbQuery(cnxn_string_map['statr'], query)
    if not cursor:
        ERROR("Failed to query production database.")
        return None

    results = cursor.fetchall()
    if not results:
        return []

    time_diffs_seconds = []
    for started, finished in results:
        if isinstance(started, str):
            started = datetime.fromisoformat(started)
        if isinstance(finished, str):
            finished = datetime.fromisoformat(finished)

        time_diffs_seconds.append((finished - started).total_seconds())
    
    return time_diffs_seconds

def pick_unit(time_diffs_seconds):
    median = np.median(time_diffs_seconds)
    if median > 1800:   # bulk of jobs take > 30 min → hours is readable
        return 'hours'
    if median > 30:     # bulk takes > 30 s → minutes
        return 'minutes'
    return 'seconds'

def plot_histogram(ax, time_diffs_seconds, title, time_unit='hours'):
    """
    Plots a histogram of time differences with an overflow bin.
    """
    
    max_time_hours = np.max(time_diffs_seconds) / 3600 if time_diffs_seconds else 0

    # Unit-specific configurations
    config = {
        'hours': {'conv': 3600, 'label': 'hours', 'max_val': 10, 'bin_w': 0.1, 'tick_step': 2},
        'minutes': {'conv': 60, 'label': 'minutes', 'max_val': 600, 'bin_w': 10, 'tick_step': 60},
        'seconds': {'conv': 1, 'label': 'seconds', 'max_val': 1200, 'bin_w': 20, 'tick_step': 120},
    }

    if time_unit not in config:
        raise ValueError("Invalid time_unit. Must be 'hours', 'minutes', or 'seconds'.")

    cfg = config[time_unit]
    time_diffs = [t / cfg['conv'] for t in time_diffs_seconds]
    avg_time = np.mean(time_diffs)
    
    max_val = cfg['max_val']
    bin_width = cfg['bin_w']
    tick_step = cfg['tick_step']
    
    # Special condition for jobs finishing in less than 10 hours
    if time_unit == 'hours' and max_time_hours < 10:
        max_val = 10
        bin_width = 10 / 60  # 10-minute bins
        tick_step = 1

    # Prepare data for plotting
    plot_data = [min(diff, max_val) for diff in time_diffs]
    bins = np.arange(0, max_val + bin_width, bin_width)
    
    # Plotting
    ax.hist(plot_data, bins=bins, alpha=0.7, label=f'Time (Avg: {avg_time:.2f} {cfg["label"]})')

    # Axes and labels
    ax.set_title(title)
    ax.set_xlabel(f'Wall time (start to finish) ({cfg["label"]})')
    ax.set_ylabel('Number of Jobs')
    ax.legend()
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)

    # X-axis ticks and overflow label
    ax.set_xlim(0, max_val)
    xticks = np.arange(0, max_val + bin_width, tick_step)
    
    # Ensure the last tick is at max_val
    if max_val not in xticks:
        xticks = np.append(xticks, max_val)

    xticklabels = [f'{t:g}' for t in xticks]
    xticklabels[-1] = f'{int(max_val)}+'

    ax.set_xticks(xticks)
    ax.set_xticklabels(xticklabels)

def main():
    """
    Main function to plot job time distribution.
    """
    args = submission_args()
    
    plt.rcParams.update({'font.size': 16})

    sublogdir = setup_rot_handler(args)
    slogger.setLevel(args.loglevel)
    INFO(f"Logging to {sublogdir}, level {args.loglevel}")

    param_overrides = {}
    param_overrides["runs"] = args.runs
    param_overrides["runlist"] = args.runlist
    param_overrides["nevents"] = 0 

    if args.physicsmode is not None:
        param_overrides["physicsmode"] = args.physicsmode

    param_overrides["prodmode"] = "production"
    if args.mangle_dirpath:
        param_overrides["prodmode"] = args.mangle_dirpath

    try:
        rule = RuleConfig.from_yaml_file(
            yaml_file=args.config,
            rule_name=args.rulename,
            param_overrides=param_overrides
        )
        INFO(f"Successfully loaded rule configuration: {args.rulename}")
    except (ValueError, FileNotFoundError) as e:
        ERROR(f"Error: {e}")
        sys.exit(1)

    # If --runs is specified and there are multiple runs, create a multi-page PDF.
    if args.runs and 1 < len(rule.runlist_int) <= 5:
        output_pdf_path = f'job_time_distribution_{args.rulename}.pdf'
        with PdfPages(output_pdf_path) as pdf:
            for run in rule.runlist_int:
                INFO(f"Processing run: {run}")
                run_condition = list_to_condition([run], name="runnumber")
                time_diffs_seconds = get_time_diffs(run_condition, rule.dsttype, rule.outtriplet, rule.dataset)

                if not time_diffs_seconds:
                    INFO(f"No finished jobs found for run {run}.")
                    continue

                fig, ax = plt.subplots(figsize=(12, 7))
                plt.style.use('seaborn-v0_8-deep')
                
                title = f'Job Time Distribution for {args.rulename} (Run: {run})'
                plot_histogram(ax, time_diffs_seconds, title, time_unit=pick_unit(time_diffs_seconds))
                
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)

            INFO(f"Saved multi-page PDF to {output_pdf_path}")

    else: # Original behavior: single plot for all runs, now for each time unit
        run_condition = list_to_condition(rule.runlist_int, name="runnumber")
        time_diffs_seconds = get_time_diffs(run_condition, rule.dsttype, rule.outtriplet, rule.dataset)
        if time_diffs_seconds is None:
            sys.exit(1) # Error occurred in get_time_diffs
        if not time_diffs_seconds:
            INFO("No finished jobs found for the specified runs.")
            sys.exit(0)

        run_str = f"Run(s): {rule.runlist_int}"
        # if len(rule.runlist_int) > 2:
        #     run_str = f"Runs $\in$ [{min(rule.runlist_int)},...,{max(rule.runlist_int)}]"
        if rule.runlist is not None:
            #run_str = f"Runs from file: {rule.runlist}"
            run_str = f"Runs from file: {os.path.basename(rule.runlist)}"

        base_title = f'Job Time Distribution for {args.rulename}\n{run_str}'
        
        unit = pick_unit(time_diffs_seconds)
        fig, ax = plt.subplots(figsize=(12, 7))
        plt.style.use('seaborn-v0_8-deep')

        plot_histogram(ax, time_diffs_seconds, base_title, time_unit=unit)

        plt.tight_layout()
        output_file = f'job_time_distribution_{args.rulename}.png'
        plt.savefig(output_file)
        INFO(f"Saved plot to {output_file}")
        plt.close(fig)
    

if __name__ == '__main__':
    main()
