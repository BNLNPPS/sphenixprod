#!/usr/bin/env python

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


def get_time_diffs(run_condition, dsttype):
    query = f"""
    SELECT submitting, ended
    FROM production_status
    WHERE {run_condition}
      AND dsttype = '{dsttype}'
      AND status = 'finished'
      AND submitting IS NOT NULL
      AND ended IS NOT NULL
    """
    
    DEBUG(f"Executing query:\n{query}")

    cursor = dbQuery(cnxn_string_map['statr'], query)
    if not cursor:
        ERROR("Failed to query production database.")
        return None

    results = cursor.fetchall()
    if not results:
        return []

    time_diffs_hours = []
    for submitting, ended in results:
        if isinstance(submitting, str):
            submitting = datetime.fromisoformat(submitting)
        if isinstance(ended, str):
            ended = datetime.fromisoformat(ended)
            
        time_diff = (ended - submitting).total_seconds() / 3600
        time_diffs_hours.append(time_diff)
    
    return time_diffs_hours

def plot_histogram(ax, time_diffs_hours, title):
    """
    Plots a histogram of time differences with an overflow bin.
    """
    # Original data average
    avg_hours = np.mean(time_diffs_hours)
    
    # Cap values at 60 for the overflow bin
    plot_data = [min(diff, 60) for diff in time_diffs_hours]
    
    # Define bins from 0 to 60
    bins = np.arange(61) # 0-1, 1-2, ..., 59-60
    
    ax.hist(plot_data, bins=bins, alpha=0.7, label=f'Time Distribution (Avg: {avg_hours:.2f} hours)')
    
    ax.set_title(title)
    ax.set_xlabel('Time from Submission to Finish (hours)')
    ax.set_ylabel('Number of Jobs')
    ax.legend()
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    # Set x-axis limit and ticks for overflow
    ax.set_xlim(0, 60)
    xticks = np.arange(0, 61, 10)
    xticklabels = [str(t) for t in xticks]
    xticklabels[-1] = '60+'
    ax.set_xticks(xticks)
    ax.set_xticklabels(xticklabels)

def main():
    """
    Main function to plot job time distribution.
    """
    args = submission_args()
    
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

    # If --runs is specified, create a multi-page PDF with one plot per run
    if args.runs and len(rule.runlist_int) > 1:
        output_pdf_path = f'job_time_distribution_{args.rulename}.pdf'
        with PdfPages(output_pdf_path) as pdf:
            for run in rule.runlist_int:
                INFO(f"Processing run: {run}")
                run_condition = list_to_condition([run], name="run")
                time_diffs_hours = get_time_diffs(run_condition, rule.dsttype)

                if not time_diffs_hours:
                    INFO(f"No finished jobs found for run {run}.")
                    continue

                fig, ax = plt.subplots(figsize=(12, 7))
                plt.style.use('seaborn-v0_8-deep')
                
                title = f'Job Time Distribution for {args.rulename} (Run: {run})'
                plot_histogram(ax, time_diffs_hours, title)
                
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)

            INFO(f"Saved multi-page PDF to {output_pdf_path}")

    else: # Original behavior: single plot for all runs
        run_condition = list_to_condition(rule.runlist_int, name="run")
        time_diffs_hours = get_time_diffs(run_condition, rule.dsttype)

        if time_diffs_hours is None:
            sys.exit(1) # Error occurred in get_time_diffs
        if not time_diffs_hours:
            INFO("No finished jobs found for the specified runs.")
            sys.exit(0)

        fig, ax = plt.subplots(figsize=(12, 7))
        plt.style.use('seaborn-v0_8-deep')
        
        run_str = f"Run(s): {rule.runlist_int}"
        title = f'Job Time Distribution for {args.rulename}\n{run_str}'
        plot_histogram(ax, time_diffs_hours, title)

        plt.tight_layout()
        output_file = f'job_time_distribution_{args.rulename}.png'
        plt.savefig(output_file)
        INFO(f"Saved plot to {output_file}")
        plt.close(fig)

if __name__ == '__main__':
    main()
