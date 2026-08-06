#!/usr/bin/env python

import os
import sys
import yaml

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.offsetbox import AnchoredOffsetbox, HPacker, TextArea, VPacker

from argparsing import submission_args
from sphenixdbutils import dbQuery, cnxn_string_map, list_to_condition
from sphenixprodrules import RuleConfig
from sphenixmisc import setup_rot_handler
from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401


def get_memory_values(run_condition, dsttype, tag, dataset):
    query = f"""
    SELECT MemoryProvisioned, MemoryUsage
    FROM production_jobs
    WHERE {run_condition}
      AND tag = '{tag}'
      AND dataset = '{dataset}'
      AND status IN ('finished', 'failed')
      AND dsttype LIKE '{dsttype}%'
      AND MemoryProvisioned IS NOT NULL
      AND MemoryUsage IS NOT NULL
      AND MemoryProvisioned > 0
      AND MemoryUsage > 0
    """

    DEBUG(f"Executing query:\n{query}")

    cursor = dbQuery(cnxn_string_map['statr'], query)
    if not cursor:
        ERROR("Failed to query production database.")
        return None

    results = cursor.fetchall()
    if not results:
        return []

    memory_values = []
    for provisioned_mb, usage_mb in results:
        memory_values.append((float(provisioned_mb) / 1024.0, float(usage_mb) / 1024.0))

    return memory_values


def get_status_counts(run_condition, dsttype, tag, dataset):
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


def _axis_limits(*arrays):
    values = np.concatenate([np.asarray(a, dtype=float) for a in arrays if len(a)])
    if values.size == 0:
        return 0, 15

    low = max(0, float(np.min(values)))
    high = float(np.max(values))
    span = high - low
    pad = max(0.5, span * 0.08)

    axis_min = min(max(0, low - pad), 14.5)
    return axis_min, 15


def plot_memory(ax_hist, ax_scatter, memory_values, title):
    provisioned_gb = np.array([v[0] for v in memory_values])
    usage_gb = np.array([v[1] for v in memory_values])

    min_axis, max_axis = _axis_limits(provisioned_gb, usage_gb)
    bin_width = 0.5
    bins = np.arange(0, max_axis + bin_width, bin_width)
    if len(bins) < 2:
        bins = np.array([0, max_axis + bin_width])

    ax_hist.hist(
        provisioned_gb,
        bins=bins,
        alpha=0.65,
        label=f'Provisioned (avg: {np.mean(provisioned_gb):.1f} GB)',
    )
    ax_hist.hist(
        usage_gb,
        bins=bins,
        alpha=0.65,
        label=f'Actual usage (avg: {np.mean(usage_gb):.1f} GB)',
    )
    ax_hist.set_title('Memory distribution')
    ax_hist.set_xlabel('Memory (GB)')
    ax_hist.set_ylabel('Number of jobs')
    ax_hist.set_xlim(min_axis, max_axis)
    ax_hist.legend(loc='upper right')
    ax_hist.grid(True, which='both', linestyle='--', linewidth=0.5)

    ax_scatter.scatter(provisioned_gb, usage_gb, alpha=0.45, s=18, edgecolors='none')
    ax_scatter.plot([min_axis, max_axis], [min_axis, max_axis], 'r--', linewidth=1.5, label='usage = provisioned')
    ax_scatter.set_title('Actual vs provisioned memory')
    ax_scatter.set_xlabel('Memory provisioned (GB)')
    ax_scatter.set_ylabel('Actual memory usage (GB)')
    ax_scatter.set_xlim(min_axis, max_axis)
    ax_scatter.set_ylim(min_axis, max_axis)
    ax_scatter.set_aspect('equal', adjustable='box')
    ax_scatter.legend(loc='upper left')
    ax_scatter.grid(True, which='both', linestyle='--', linewidth=0.5)

    ax_hist.figure.suptitle(title)


def add_status_box(ax, status_counts, plotted_count):
    if status_counts is None:
        return

    rows = [
        ("plotted:", plotted_count, "black"),
        ("finished:", status_counts["finished"], "black"),
        ("failed:", status_counts["failed"], "red"),
        ("total:", status_counts["total"], "black"),
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
            TextArea(f"{value}", textprops={"color": color, "ha": "right"})
            for _, value, color in rows
        ],
        align="right",
        pad=0,
        sep=2,
    )
    status_box = HPacker(
        children=[label_column, value_column],
        align="baseline",
        pad=0,
        sep=12,
    )

    anchored_box = AnchoredOffsetbox(
        loc="upper right",
        child=status_box,
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


def run_label(rule):
    run_str = f"Run(s): {rule.runlist_int}"
    if rule.runlist is not None:
        run_str = f"Runs from file: {os.path.basename(rule.runlist)}"
    return run_str


def make_plot(memory_values, status_counts, title, output_target):
    fig, (ax_hist, ax_scatter) = plt.subplots(1, 2, figsize=(18, 8))
    plt.style.use('seaborn-v0_8-deep')

    plot_memory(ax_hist, ax_scatter, memory_values, title)
    add_status_box(ax_hist, status_counts, len(memory_values))

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    output_target(fig)
    plt.close(fig)


def main():
    """
    Main function to plot job memory distribution.
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

    if args.runs and 1 < len(rule.runlist_int) <= 5:
        output_pdf_path = f'job_memory_distribution_{args.rulename}.pdf'
        with PdfPages(output_pdf_path) as pdf:
            for run in rule.runlist_int:
                INFO(f"Processing run: {run}")
                run_condition = list_to_condition([run], name="runnumber")
                memory_values = get_memory_values(run_condition, rule.dsttype, rule.outtriplet, rule.dataset)
                status_counts = get_status_counts(run_condition, rule.dsttype, rule.outtriplet, rule.dataset)

                if memory_values is None:
                    sys.exit(1)
                if not memory_values:
                    INFO(f"No jobs with memory values found for run {run}.")
                    continue

                title = f'Job Memory Distribution for {args.rulename}\nRun: {run}'
                make_plot(memory_values, status_counts, title, pdf.savefig)

            INFO(f"Saved multi-page PDF to {output_pdf_path}")

    else:
        run_condition = list_to_condition(rule.runlist_int, name="runnumber")
        memory_values = get_memory_values(run_condition, rule.dsttype, rule.outtriplet, rule.dataset)
        status_counts = get_status_counts(run_condition, rule.dsttype, rule.outtriplet, rule.dataset)
        if memory_values is None:
            sys.exit(1)
        if not memory_values:
            INFO("No jobs with memory values found for the specified runs.")
            sys.exit(0)

        title = f'Job Memory Distribution for {args.rulename}\n{run_label(rule)}'
        output_file = f'job_memory_distribution_{args.rulename}.png'
        make_plot(memory_values, status_counts, title, lambda fig: fig.savefig(output_file))
        INFO(f"Saved plot to {output_file}")


if __name__ == '__main__':
    main()
