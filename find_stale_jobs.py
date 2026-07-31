#!/usr/bin/env python
"""
find_stale_jobs.py — report production jobs that look stuck.

A job is considered a candidate when:
  - status matches --status  (default: 'running')
  - memoryprovisioned > --min-memory  (default: 11000 MB)
  - the 'running' timestamp is older than --running-hours  (default: 72 h)

For each candidate the 'out' log file mtime is checked:
  - if the file is older than --out-hours  (default: 24 h) it is flagged STALE
  - if the file is newer it is flagged ACTIVE  (job may still be writing)
  - if the file cannot be stat'd it is flagged MISSING

Does NOT depend on any other module in this project.
"""

import argparse
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


try:
    import pyodbc
except ImportError:
    pyodbc = None  # allow --dryrun without pyodbc installed

# ============================================================================
# Inline logger
# ============================================================================
CHATTY_LEVEL_NUM = 5
logging.addLevelName(CHATTY_LEVEL_NUM, "CHATTY")

def _chatty(self, message, *args, **kws):
    if self.isEnabledFor(CHATTY_LEVEL_NUM):
        self._log(CHATTY_LEVEL_NUM, message, args, stacklevel=2, **kws)
logging.Logger.chatty = _chatty


class _Fmt(logging.Formatter):
    show_datetime = True
    grey     = "\x1b[38;20m"
    yellow   = "\x1b[33;20m"
    green    = "\x1b[32;20m"
    blue     = "\x1b[36;20m"
    red      = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset    = "\x1b[0m"
    _datetime_fmt = "%(asctime)s [%(levelname)s] - %(message)s"
    _plain_fmt    = "[%(levelname)s] - %(message)s"

    def _base_format(self):
        return self._datetime_fmt if self.show_datetime else self._plain_fmt

    def format(self, record):
        base = self._base_format()
        formats = {
            CHATTY_LEVEL_NUM: self.yellow   + base + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.DEBUG:    self.grey     + base + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.INFO:     self.green    + base + self.reset,
            logging.WARNING:  self.blue     + base + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.ERROR:    self.red      + base + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.CRITICAL: self.bold_red + base + " (%(filename)s:%(lineno)d) " + self.reset,
        }
        formatter = logging.Formatter(formats.get(record.levelno, base))
        return formatter.format(record)


_log = logging.getLogger('find_stale_jobs')
if not _log.hasHandlers():
    _ch = logging.StreamHandler()
    _ch.setFormatter(_Fmt())
    _log.addHandler(_ch)

CHATTY = _log.chatty
DEBUG  = _log.debug
INFO   = _log.info
WARN   = _log.warning
ERROR  = _log.error

# ============================================================================
# DB connection strings
# ============================================================================
if os.uname().sysname == 'Darwin':
    _STATR = 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DATABASE=productiondb;READONLY=True;UID=eickolja'
else:
    _STATR = 'DSN=Production_read;READONLY=True;UID=argouser'

_RETRYABLE = {'40001', '53300', '57P03', '08006', '08001'}

def _db_query(query: str, ntries: int = 5):
    CHATTY(f'[sql]\n{query}')
    if pyodbc is None:
        ERROR("pyodbc not available; cannot query DB.")
        sys.exit(1)
    for itry in range(ntries):
        try:
            conn = pyodbc.connect(_STATR)
            curs = conn.cursor()
            curs.execute(query)
            return curs
        except pyodbc.Error as exc:
            state = exc.args[0]
            ERROR(f"Attempt {itry + 1}/{ntries}: {exc}")
            if state in _RETRYABLE:
                delay = min(60, (2 ** itry) * (0.5 + random.random()))
                WARN(f"Retrying in {delay:.1f}s …")
                time.sleep(delay)
            else:
                ERROR("Non-retryable DB error. Stop.")
                sys.exit(41)
        except Exception as exc:
            ERROR(f"Unexpected error: {exc}")
            sys.exit(41)
    ERROR("Exhausted all DB attempts. Stop.")
    sys.exit(41)

# ============================================================================
# Helpers
# ============================================================================
def _age_hours(ts) -> float:
    """Return how many hours ago `ts` (datetime or epoch float) was."""
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    elif ts.tzinfo is None:
        dt = ts.replace(tzinfo=timezone.utc)
    else:
        dt = ts
    return (datetime.now(tz=timezone.utc) - dt).total_seconds() / 3600.0


def _out_status(out_path: str, out_hours: float) -> tuple[str, str]:
    """
    Return (label, detail) for the given out file path.
    label: 'STALE' | 'ACTIVE' | 'MISSING'
    """
    try:
        mtime = Path(out_path).stat().st_mtime
    except OSError:
        return 'MISSING', 'cannot stat'
    age_h = _age_hours(mtime)
    age_str = f"{age_h:.1f} h ago"
    if age_h > out_hours:
        return 'STALE', age_str
    return 'ACTIVE', age_str

# ============================================================================
# Main logic
# ============================================================================
def _build_query(args) -> str:
    conditions = [
        f"status = '{args.status}'",
        f"memoryprovisioned > {args.min_memory}",
        f"started < NOW() - INTERVAL '{args.running_hours} hours'",
    ]
    if args.dataset:
        conditions.append(f"dataset = '{args.dataset}'")
    if args.dsttype:
        op = 'LIKE' if '%' in args.dsttype else '='
        conditions.append(f"dsttype {op} '{args.dsttype}'")
    if args.tag:
        conditions.append(f"tag = '{args.tag}'")

    where = '\n  AND '.join(conditions)
    return (
        "SELECT ClusterId, ProcId, submission_host, started, out, MemoryProvisioned, dataset, dsttype"
        " FROM production_jobs"
        f"\nWHERE {where}"
        "\nORDER BY started ASC;"
    )


def main():
    args = _parse_args()
    if args.running_hours is None:
        args.running_hours = args.out_hours
    _set_loglevel(args)
    _Fmt.show_datetime = False

    query = _build_query(args)
    INFO(f"Query:\n{query}")

    if args.dryrun:
        INFO("[dryrun] not executing query.")
        return

    curs = _db_query(query)
    rows = curs.fetchall()
    conn = getattr(curs, 'connection', None)
    curs.close()
    if conn:
        conn.close()

    if not rows:
        INFO("No matching jobs found.")
        return

    INFO(f"{len(rows)} candidate job(s) found. Checking out files …\n")

    counts = {'STALE': 0, 'ACTIVE': 0, 'MISSING': 0}
    stale_lines = []
    for cluster_id, proc_id, submission_host, started, out, mem_prov, dataset, dsttype in rows:
        condor_id = f"{cluster_id}.{proc_id}" if cluster_id is not None else "?.?"
        submit_host = submission_host or '?'
        running_age = _age_hours(started)
        label, detail = _out_status(out, args.out_hours)
        counts[label] += 1
        line = f"[{label:7s}]  running {running_age:6.1f} h ago  {detail:30s}  {out}"
        if label == 'STALE':
            ERROR(line)
            stale_lines.append((condor_id, submit_host, running_age, detail, mem_prov, dataset, dsttype, out))
        elif label == 'MISSING':
            WARN(line)
        else:
            INFO(line)

    INFO(f"\nSummary: {counts['STALE']} STALE  {counts['ACTIVE']} ACTIVE  {counts['MISSING']} MISSING  (of {len(rows)} total)")

    if stale_lines:
        cid_w  = max(len(r[0]) for r in stale_lines)
        host_w = max(len(r[1]) for r in stale_lines)
        ds_w   = max(len(r[5]) for r in stale_lines)
        dst_w  = max(len(r[6]) for r in stale_lines)
        mtime_w = max(len(r[3]) for r in stale_lines)
        header = (f"{'CONDOR_ID':<{cid_w}}  {'SUBMIT_HOST':<{host_w}}"
                  f"  {'STARTED_AGO':>11}  {'OUT_MTIME':<{mtime_w}}"
                  f"  {'MEM_PROV':>8}"
                  f"  {'DATASET':<{ds_w}}  {'DSTTYPE':<{dst_w}}")
        if args.show_out:
            header += "  OUT"
        print(f"\nStale jobs:\n{header}")
        for condor_id, submit_host, running_age, detail, mem_prov, dataset, dsttype, out in stale_lines:
            prov_s = f"{mem_prov:>8}" if mem_prov is not None else f"{'?':>8}"
            row = (f"{condor_id:<{cid_w}}  {submit_host:<{host_w}}"
                   f"  {running_age:>10.1f}h  {detail:<{mtime_w}}"
                   f"  {prov_s}"
                   f"  {dataset:<{ds_w}}  {dsttype:<{dst_w}}")
            if args.show_out:
                row += f"  {out}"
            print(row)

# ============================================================================
# Argument parsing
# ============================================================================
def _add_verbosity(parser):
    vgroup = parser.add_mutually_exclusive_group()
    vgroup.add_argument('-v', '--verbose', action='count', default=0,
                        help='Increase verbosity (-v INFO, -vv DEBUG, -vvv CHATTY).')
    vgroup.add_argument('-d', '--debug',  action='store_true', help='Alias for -vv (DEBUG).')
    vgroup.add_argument('--chatty',       action='store_true', help='Alias for -vvv (CHATTY).')


def _set_loglevel(args):
    if args.chatty or args.verbose >= 3:
        _log.setLevel(CHATTY_LEVEL_NUM)
    elif args.debug or args.verbose == 2:
        _log.setLevel(logging.DEBUG)
    else:
        _log.setLevel(logging.INFO)


def _parse_args():
    parser = argparse.ArgumentParser(
        description='Find production jobs that appear stuck.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: status=running, memoryprovisioned>11000, started >72 h ago
  find_stale_jobs.py

  # Narrow to a dataset and check out files older than 12 h
  find_stale_jobs.py --dataset run3oo --out-hours 12

  # Only show query, do not hit DB
  find_stale_jobs.py --dryrun
""",
    )

    parser.add_argument('--status',        default='running',
                        help='Job status to match (default: running).')
    parser.add_argument('--min-memory',    dest='min_memory', type=int, default=11000,
                        help='Minimum memoryprovisioned in MB (default: 11000).')
    parser.add_argument('--running-hours', dest='running_hours', type=float, default=None,
                        help='Select jobs running for at least this many hours (default: same as --out-hours).')
    parser.add_argument('--out-hours',     dest='out_hours',     type=float, default=24.0,
                        help='Flag out file as STALE when its mtime is older than this many hours (default: 24).')
    parser.add_argument('--dataset',  default=None, help='Filter by dataset name.')
    parser.add_argument('--dsttype',  default=None, help='Filter by dsttype (%% triggers LIKE).')
    parser.add_argument('--tag',      default=None, help='Filter by production tag.')
    parser.add_argument('--show-out', dest='show_out', action='store_true', default=False,
                        help='Include the OUT file path in the final stale-jobs table.')
    parser.add_argument('-n', '--dryrun', action='store_true', default=False,
                        help='Print the SQL query without executing it.')
    _add_verbosity(parser)

    return parser.parse_args()


if __name__ == '__main__':
    main()
