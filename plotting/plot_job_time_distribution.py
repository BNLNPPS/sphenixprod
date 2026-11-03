#!/usr/bin/env python

import os
import sys
from datetime import datetime
import yaml

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.offsetbox import AnchoredOffsetbox, HPacker, TextArea, VPacker

from argparsing import submission_args, sql_constraint_clause
from sphenixdbutils import dbQuery, cnxn_string_map, list_to_condition
from sphenixprodrules import RuleConfig
from sphenixmisc import setup_rot_handler
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401


def get_time_diffs_by_cpus(run_condition, dsttype, tag, dataset, constraint_clause):
    query = f"""
    SELECT runnumber, started, finished, request_cpus
    FROM production_jobs
    WHERE {run_condition}
      AND tag = '{tag}'
      AND dataset = '{dataset}'
      AND status = 'finished'
      AND dsttype LIKE '{dsttype}%'
      {constraint_clause}
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
        return {}, set()

    time_diffs_by_cpus = {}
    timed_runs = set()
    for runnumber, started, finished, request_cpus in results:
        if isinstance(started, str):
            started = datetime.fromisoformat(started)
        if isinstance(finished, str):
            finished = datetime.fromisoformat(finished)

        cpu_key = int(request_cpus) if request_cpus is not None else None
        time_diffs_by_cpus.setdefault(cpu_key, []).append((finished - started).total_seconds())
        timed_runs.add(int(runnumber))

    return time_diffs_by_cpus, timed_runs


def flatten_time_diffs(time_diffs_by_cpus):
    return [diff for diffs in time_diffs_by_cpus.values() for diff in diffs]


def cpu_sort_key(cpu_count):
    return (cpu_count is None, cpu_count if cpu_count is not None else 0)


def cpu_label(cpu_count):
    if cpu_count is None:
        return "unknown CPUs"
    if cpu_count == 1:
        return "1 CPU"
    return f"{cpu_count} CPUs"


def get_status_counts(run_condition, dsttype, tag, dataset, constraint_clause):
    query = f"""
    SELECT
      SUM(CASE WHEN status = 'finished' THEN 1 ELSE 0 END) AS finished,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
    FROM production_jobs
    WHERE {run_condition}
      AND tag = '{tag}'
      AND dataset = '{dataset}'
      AND status IN ('finished', 'failed')
      AND dsttype LIKE '{dsttype}%'
      {constraint_clause}
    """

    DEBUG(f"Executing query:\n{query}")

    cursor = dbQuery(cnxn_string_map['statr'], query)
    if not cursor:
        ERROR("Failed to query production database.")
        return None

    row = cursor.fetchone()
    finished = int(row.finished or 0)
    failed = int(row.failed or 0)
    return {
        "finished": finished,
        "failed": failed,
        "total": finished + failed,
    }


def pick_unit(time_diffs_seconds):
    median = np.median(time_diffs_seconds)
    if median > 1800:
        return 'hours'
    if median > 30:
        return 'minutes'
    return 'seconds'


def time_summary_stats(time_diffs, unit):
    conversions = {"hours": 3600, "minutes": 60, "seconds": 1}
    values = np.asarray(time_diffs, dtype=float) / conversions[unit]
    count = len(values)
    sigma = np.std(values, ddof=1) if count > 1 else 0.0
    return {
        "count": count,
        "mean": np.mean(values),
        "min": np.min(values),
        "max": np.max(values),
        "sigma": sigma,
        "total": np.sum(values),
        "total_uncertainty": sigma * np.sqrt(count),
    }


def print_time_summary(time_diffs_by_cpus, label):
    all_time_diffs = flatten_time_diffs(time_diffs_by_cpus)
    if not all_time_diffs:
        return

    unit = pick_unit(all_time_diffs)

    def summary_line(group_label, time_diffs):
        stats = time_summary_stats(time_diffs, unit)
        INFO(
            f"  {group_label}: n={stats['count']:,}, "
            f"mean={stats['mean']:.3f} {unit}, min={stats['min']:.3f}, "
            f"max={stats['max']:.3f}, sigma={stats['sigma']:.3f}, "
            f"total={stats['total']:.3f} +/- {stats['total_uncertainty']:.3f} {unit}"
        )

    INFO(f"Timing summary for {label} ({unit}):")
    summary_line("all finished jobs", all_time_diffs)
    for cpu_count in sorted(time_diffs_by_cpus, key=cpu_sort_key):
        summary_line(cpu_label(cpu_count), time_diffs_by_cpus[cpu_count])


def plot_histogram(ax, time_diffs_by_cpus, title, time_unit='hours'):
    """
    Plots wall-time histograms grouped by request_cpus with an overflow bin.
    """
    all_time_diffs_seconds = flatten_time_diffs(time_diffs_by_cpus)
    max_time_hours = np.max(all_time_diffs_seconds) / 3600 if all_time_diffs_seconds else 0

    config = {
        'hours': {'conv': 3600, 'label': 'hours', 'max_val': 30, 'bin_w': 0.25, 'tick_step': 2},
        'minutes': {'conv': 60, 'label': 'minutes', 'max_val': 600, 'bin_w': 10, 'tick_step': 60},
        'seconds': {'conv': 1, 'label': 'seconds', 'max_val': 1200, 'bin_w': 20, 'tick_step': 120},
    }

    if time_unit not in config:
        raise ValueError("Invalid time_unit. Must be 'hours', 'minutes', or 'seconds'.")

    cfg = config[time_unit]
    max_val = cfg['max_val']
    bin_width = cfg['bin_w']
    tick_step = cfg['tick_step']

    if time_unit == 'hours' and max_time_hours < 10:
        max_val = 10
        bin_width = 10 / 60
        tick_step = 1

    bins = np.arange(0, max_val + bin_width, bin_width)

    for cpu_count in sorted(time_diffs_by_cpus, key=cpu_sort_key):
        time_diffs = [t / cfg['conv'] for t in time_diffs_by_cpus[cpu_count]]
        plot_data = [min(diff, max_val) for diff in time_diffs]
        avg_time = np.mean(time_diffs)
        ax.hist(
            plot_data,
            bins=bins,
            histtype='step',
            linewidth=2,
            label=f'{cpu_label(cpu_count)} (n={len(time_diffs):,})',
        )

    ax.set_title(title)
    ax.set_xlabel(f'Wall time (start to finish) ({cfg["label"]})')
    ax.set_ylabel('Number of Jobs')
    ax.legend(loc='upper left')
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)

    ax.set_xlim(0, max_val)
    xticks = np.arange(0, max_val + bin_width, tick_step)

    if max_val not in xticks:
        xticks = np.append(xticks, max_val)

    xticklabels = [f'{t:g}' for t in xticks]
    xticklabels[-1] = f'{int(max_val)}+'

    ax.set_xticks(xticks)
    ax.set_xticklabels(xticklabels)


def add_timing_summary_box(ax, time_diffs_by_cpus, status_counts, run_count=1):
    all_time_diffs = flatten_time_diffs(time_diffs_by_cpus)
    if not all_time_diffs:
        return

    unit = pick_unit(all_time_diffs)
    stats = time_summary_stats(all_time_diffs, unit)
    rows = [
        ("finished:", f"{status_counts['finished']:,}" if status_counts else "-", "black"),
        ("failed:", f"{status_counts['failed']:,}" if status_counts else "-", "red"),
        ("timed jobs:", f"{stats['count']:,}", "black"),
        ("mean:", f"{stats['mean']:.3f} {unit}", "black"),
        ("sigma:", f"{stats['sigma']:.3f} {unit}", "black"),
        ("min / max:", f"{stats['min']:.3f} / {stats['max']:.3f} {unit}", "black"),
        ("total wall:", f"{stats['total']:,.1f} +/- {stats['total_uncertainty']:,.1f} {unit}", "black"),
        ("wall / selected run:", f"{stats['total'] / max(run_count, 1):,.1f} {unit}", "black"),
    ]

    label_column = VPacker(
        children=[
            TextArea(label, textprops={"color": color, "ha": "left"})
            for label, _, color in rows
        ],
        align="left",
        pad=0,
        sep=2,
    )
    value_column = VPacker(
        children=[
            TextArea(value, textprops={"color": color, "ha": "right"})
            for _, value, color in rows
        ],
        align="right",
        pad=0,
        sep=2,
    )
    summary_box = HPacker(
        children=[label_column, value_column],
        align="baseline",
        pad=0,
        sep=12,
    )

    anchored_box = AnchoredOffsetbox(
        loc="upper right",
        child=summary_box,
        bbox_to_anchor=(0.98, 0.95),
        bbox_transform=ax.transAxes,
        frameon=True,
        borderpad=0,
        pad=0.35,
    )
    anchored_box.patch.set_boxstyle("round,pad=0.35")
    anchored_box.patch.set_facecolor("white")
    anchored_box.patch.set_edgecolor("0.5")
    anchored_box.patch.set_alpha(0.85)
    ax.add_artist(anchored_box)


def main():
    """
    Main function to plot job time distribution.
    """
    args = submission_args(allow_sql_constraint=True)

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

    constraint_clause = sql_constraint_clause(args.sql_constraint)

    if args.runs and 1 < len(rule.runlist_int) <= 5:
        output_pdf_path = f'job_time_distribution_{args.rulename}.pdf'
        with PdfPages(output_pdf_path) as pdf:
            for run in rule.runlist_int:
                INFO(f"Processing run: {run}")
                run_condition = list_to_condition([run], name="runnumber")
                time_data = get_time_diffs_by_cpus(run_condition, rule.dsttype, rule.outtriplet, rule.dataset, constraint_clause)
                status_counts = get_status_counts(run_condition, rule.dsttype, rule.outtriplet, rule.dataset, constraint_clause)

                if time_data is None:
                    sys.exit(1)
                time_diffs_by_cpus, timed_runs = time_data
                all_time_diffs = flatten_time_diffs(time_diffs_by_cpus)
                if not all_time_diffs:
                    INFO(f"No finished jobs found for run {run}.")
                    continue

                fig, ax = plt.subplots(figsize=(12, 7))
                plt.style.use('seaborn-v0_8-deep')

                title = f'Job Time Distribution for {args.rulename} (Run: {run})'
                plot_histogram(ax, time_diffs_by_cpus, title, time_unit=pick_unit(all_time_diffs))
                add_timing_summary_box(ax, time_diffs_by_cpus, status_counts, run_count=len(timed_runs))

                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)
                print_time_summary(time_diffs_by_cpus, f"{args.rulename} (run {run})")

            INFO(f"Saved multi-page PDF to {output_pdf_path}")

    else:
        run_condition = list_to_condition(rule.runlist_int, name="runnumber")
        time_data = get_time_diffs_by_cpus(run_condition, rule.dsttype, rule.outtriplet, rule.dataset, constraint_clause)
        status_counts = get_status_counts(run_condition, rule.dsttype, rule.outtriplet, rule.dataset, constraint_clause)
        if time_data is None:
            sys.exit(1)
        time_diffs_by_cpus, timed_runs = time_data
        all_time_diffs = flatten_time_diffs(time_diffs_by_cpus)
        if not all_time_diffs:
            INFO("No finished jobs found for the specified runs.")
            sys.exit(0)

        run_str = f"Run(s): {rule.runlist_int}"
        if rule.runlist is not None:
            run_str = f"Runs from file: {os.path.basename(rule.runlist)}"

        base_title = f'Job Time Distribution for {args.rulename}\n{run_str}'

        unit = pick_unit(all_time_diffs)
        fig, ax = plt.subplots(figsize=(12, 7))
        plt.style.use('seaborn-v0_8-deep')

        plot_histogram(ax, time_diffs_by_cpus, base_title, time_unit=unit)
        add_timing_summary_box(
            ax, time_diffs_by_cpus, status_counts, run_count=len(timed_runs)
        )

        plt.tight_layout()
        output_file = f'job_time_distribution_{args.rulename}.png'
        plt.savefig(output_file)
        INFO(f"Saved plot to {output_file}")
        plt.close(fig)
        print_time_summary(time_diffs_by_cpus, args.rulename)


if __name__ == '__main__':
    main()
