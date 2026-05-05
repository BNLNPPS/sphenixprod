#!/usr/bin/env python

import sys
from datetime import datetime, timezone, timedelta

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

from argparsing import submission_args
from sphenixdbutils import dbQuery, cnxn_string_map
from sphenixprodrules import RuleConfig
from sphenixmisc import setup_rot_handler
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401


START_DATE    = datetime(2026, 4, 21, tzinfo=timezone.utc)
BIN_HOURS     = 2
ROLLING_BINS  = 12   # rolling average window: 12 bins = 24 hours


def get_start_times(dsttype, tag, dataset, since):
    query = f"""
    SELECT started
    FROM production_jobs
    WHERE tag = '{tag}'
      AND dataset = '{dataset}'
      AND status = 'finished'
      AND started >= '{since.isoformat()}'
      AND dsttype LIKE '{dsttype}%'
      AND started IS NOT NULL
    """
    DEBUG(f"Executing query:\n{query}")

    cursor = dbQuery(cnxn_string_map['statr'], query)
    if not cursor:
        ERROR("Failed to query production database.")
        return None

    results = cursor.fetchall()
    if not results:
        return []

    times = []
    for (started,) in results:
        if isinstance(started, str):
            started = datetime.fromisoformat(started)
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        times.append(started)

    return times


def main():
    args = submission_args()

    plt.rcParams.update({'font.size': 16})

    sublogdir = setup_rot_handler(args)
    slogger.setLevel(args.loglevel)
    INFO(f"Logging to {sublogdir}, level {args.loglevel}")

    param_overrides = {}
    param_overrides["runs"]     = args.runs
    param_overrides["runlist"]  = args.runlist
    param_overrides["nevents"]  = 0

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

    start_times = get_start_times(rule.dsttype, rule.outtriplet, rule.dataset, START_DATE)
    if start_times is None:
        sys.exit(1)
    if not start_times:
        INFO(f"No finished jobs found since {START_DATE.date()}.")
        sys.exit(0)

    INFO(f"Found {len(start_times)} finished jobs.")

    now       = datetime.now(timezone.utc)
    bin_edges = mdates.drange(START_DATE, now + timedelta(hours=BIN_HOURS), timedelta(hours=BIN_HOURS))
    start_nums = [mdates.date2num(t) for t in start_times]

    counts, _ = np.histogram(start_nums, bins=bin_edges)
    bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])

    kernel  = np.ones(ROLLING_BINS) / ROLLING_BINS
    rolling = np.convolve(counts, kernel, mode='same')

    fig, ax = plt.subplots(figsize=(16, 7))
    plt.style.use('seaborn-v0_8-deep')

    ax.bar(bin_edges[:-1], counts, width=(bin_edges[1] - bin_edges[0]), alpha=0.5, align='edge', label=f'{BIN_HOURS}h bins')
    ax.plot(bin_centers, rolling, color='red', linewidth=2, label=f'{ROLLING_BINS * BIN_HOURS}h rolling avg')

    ax.xaxis_date()
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=6))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    plt.xticks(rotation=45, ha='right')

    ax.set_xlim(bin_edges[0], bin_edges[-1])
    ax.set_title(f'Finished jobs by start time — {args.rulename}\n(since {START_DATE.date()}, {BIN_HOURS}h bins)')
    ax.set_xlabel('Job start time')
    ax.set_ylabel('Number of jobs finished')
    ax.legend()
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)

    plt.tight_layout()
    output_file = f'job_throughput_{args.rulename}.png'
    plt.savefig(output_file)
    INFO(f"Saved plot to {output_file}")
    plt.close(fig)


if __name__ == '__main__':
    main()
