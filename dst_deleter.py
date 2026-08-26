#!/usr/bin/env python
"""
dst_deleter.py — standalone DST file deletion helper.

Two subcommands:

  generate  Query the FileCatalog and write a TSV work list (lfn<TAB>path).
            Inspect the list before proceeding.

  execute   Read the work list in batches and, for each batch:
              1. Delete the corresponding rows from `files` and `datasets`.
              2. Unlink the physical files.
            DB deletion intentionally happens first: once a batch starts, the
            catalog should no longer advertise files that are being removed.
            Completed batches are removed from the work list so interrupted
            runs can be restarted.

Does NOT depend on any other module in this project.
"""

import argparse
import glob
import logging
import os
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path


import psutil
import pyodbc

_PROC = psutil.Process()

def _rss_mb() -> int:
    return _PROC.memory_info().rss // (1024 * 1024)

# ============================================================================
# Inline logger  (mirrors simpleLogger.py)
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
    _plain_fmt = "[%(levelname)s] - %(message)s"

    def _base_format(self):
        return self._datetime_fmt if self.show_datetime else self._plain_fmt

    def format(self, record):
        base_format = self._base_format()
        formats = {
            CHATTY_LEVEL_NUM: self.yellow + base_format + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.DEBUG:    self.grey + base_format + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.INFO:     self.green + base_format + self.reset,
            logging.WARNING:  self.blue + base_format + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.ERROR:    self.red + base_format + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.CRITICAL: self.bold_red + base_format + " (%(filename)s:%(lineno)d) " + self.reset,
        }
        formatter = logging.Formatter(formats.get(record.levelno, base_format))
        return formatter.format(record)


def _set_log_timestamps_enabled(enabled: bool):
    _Fmt.show_datetime = enabled


_log = logging.getLogger('dst_deleter')
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
# DB connection strings  (mirrors sphenixdbutils.py)
# ============================================================================
if os.uname().sysname == 'Darwin':
    _FCW = 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DATABASE=filecatalogdb;UID=eickolja'
    _FCR = 'DRIVER=PostgreSQL Unicode;SERVER=localhost;DATABASE=filecatalogdb;READONLY=True;UID=eickolja'
else:
    _FCW = 'DSN=FileCatalog;UID=phnxrc'
    _FCR = 'DSN=FileCatalog;READONLY=True;UID=phnxrc'

# ============================================================================
# DB query helper  (mirrors sphenixdbutils.dbQuery)
# ============================================================================
_RETRYABLE = {'40001', '53300', '57P03', '08006', '08001'}

def _db_query(cnxn_string: str, query: str, ntries: int = 5, dryrun: bool = False):
    CHATTY(f'[sql]\n{query}')
    if dryrun:
        INFO(f'[dryrun] would execute:\n{query}')
        return None
    for itry in range(ntries):
        try:
            INFO(f"DB connect starting | RSS {_rss_mb()} MB")
            conn = pyodbc.connect(cnxn_string)
            INFO(f"DB connect complete | RSS {_rss_mb()} MB")
            curs = conn.cursor()
            INFO(f"DB execute starting | RSS {_rss_mb()} MB")
            curs.execute(query)
            INFO(f"DB execute complete | RSS {_rss_mb()} MB")
            return curs  # pyodbc cursor holds a ref to conn; connection stays alive
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

def _close_cursor(curs) -> None:
    conn = getattr(curs, 'connection', None)
    curs.close()
    if conn is not None:
        conn.close()


# ============================================================================
# Run-number condition builder  (mirrors sphenixdbutils.list_to_condition)
# ============================================================================
def _run_condition(runs: list, table: str = '', pair_is_range: bool = True) -> str:
    col = f"{table}.runnumber" if table else "runnumber"
    runs = sorted(runs)

    n = len(runs)
    if n == 0:
        ERROR("No run numbers supplied.")
        sys.exit(2)
    if n == 1:
        return f"{col} = {runs[0]}"
    if n == 2 and pair_is_range:
        return f"{col} >= {runs[0]} and {col} <= {runs[1]}"
    return f"{col} in ({','.join(str(r) for r in runs)})"

# ============================================================================
# Subcommand: generate
# ============================================================================
def _sql_literal(val: str) -> str:
    return "'" + val.replace("'", "''") + "'"

def _sql_cond(col: str, val: str) -> str:
    op = 'like' if '%' in val else '='
    return f"{col} {op} {_sql_literal(val)}"


def _warn_about_home_outfile(outfile: str) -> None:
    home = Path.home().resolve()
    outpath = Path(outfile).expanduser().resolve()
    if home == outpath or home in outpath.parents:
        WARN(f"Output file {str(outpath)!r} is under your home directory. Do not generate large deletion work lists there; use /tmp or a scratch area.")
    else:
        WARN("Do not generate large deletion work lists in your home directory; use /tmp or a scratch area.")


def _format_bytes(num_bytes: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{num_bytes} B"
        value /= 1024


def _disk_free_for_outfile(outfile: str):
    outpath = Path(outfile).expanduser()
    parent = outpath if outpath.exists() and outpath.is_dir() else outpath.parent
    while not parent.exists():
        if parent.parent == parent:
            return None, None
        parent = parent.parent
    usage = shutil.disk_usage(str(parent))
    return parent.resolve(), usage.free


def cmd_generate(args):
    runs = _resolve_runs(args)
    INFO(f"Run selection: {len(runs)} run(s), first={runs[0]}, last={runs[-1]}")

    if args.fetch_size <= 0:
        ERROR("--fetch-size must be positive.")
        sys.exit(2)

    run_cond = _run_condition(runs, table='d', pair_is_range=args.runs is not None)
    where_clause = f"""
WHERE  {run_cond}
  AND  {_sql_cond('d.dataset', args.dataset)}
  AND  {_sql_cond('d.dsttype', args.dsttype)}
  AND  {_sql_cond('d.tag',     args.tag)}
"""
    estimate_query = f"""
SELECT COUNT(*) AS entries,
       COALESCE(SUM(LENGTH(COALESCE(f.lfn::text, '')) + 1 + LENGTH(COALESCE(f.full_file_path::text, '')) + 1), 0) AS tsv_bytes
FROM   files f
LEFT JOIN datasets d ON f.lfn = d.filename
{where_clause}
;
"""
    def page_query(last_lfn: str = None) -> str:
        page_where = where_clause
        if last_lfn is not None:
            page_where += f"  AND  f.lfn > {_sql_literal(last_lfn)}\n"
        return f"""
SELECT f.lfn, f.full_file_path
FROM   files f
LEFT JOIN datasets d ON f.lfn = d.filename
{page_where}
ORDER BY f.lfn ASC
LIMIT {args.fetch_size}
;
"""
    INFO(f"Querying FileCatalog: dataset={args.dataset!r} dsttype={args.dsttype!r} tag={args.tag!r}")
    _warn_about_home_outfile(args.outfile)
    INFO(f"Generate RSS at start: {_rss_mb()} MB")

    if args.dryrun:
        INFO(f'[dryrun] would estimate line count with:\n{estimate_query}')
        INFO(f'[dryrun] would execute first page:\n{page_query()}')
        INFO(f'[dryrun] would write TSV to {args.outfile!r}')
        return

    estimate_curs = _db_query(_FCR, estimate_query)
    estimate_row = estimate_curs.fetchone()
    estimated_entries = int(estimate_row[0])
    estimated_tsv_bytes = int(estimate_row[1])
    _close_cursor(estimate_curs)
    header_bytes = len(f"# dataset: {args.dataset}\n# dsttype: {args.dsttype}\n# tag:     {args.tag}\n".encode("utf-8"))
    estimated_output_bytes = estimated_tsv_bytes + header_bytes
    estimated_lines = estimated_entries + 3
    disk_path, disk_free = _disk_free_for_outfile(args.outfile)
    disk_note = "disk free unavailable"
    if disk_free is not None:
        disk_note = f"{_format_bytes(disk_free)} free on {disk_path}"
        if estimated_output_bytes > disk_free:
            disk_note += " [estimated output exceeds free space]"
    INFO(f"Estimated output size: {estimated_entries:,} entries, about {estimated_lines:,} TSV lines / {_format_bytes(estimated_output_bytes)} including header; {disk_note}. RSS {_rss_mb()} MB.")

    count = 0
    last_lfn = None
    with open(args.outfile, 'w') as fh:
        fh.write(f"# dataset: {args.dataset}\n")
        fh.write(f"# dsttype: {args.dsttype}\n")
        fh.write(f"# tag:     {args.tag}\n")
        while True:
            INFO(f"Page query starting at {count:,} entries | RSS {_rss_mb()} MB")
            curs = _db_query(_FCR, page_query(last_lfn))
            INFO(f"Page query opened | RSS {_rss_mb()} MB")
            INFO(f"Fetch starting at {count:,} entries | RSS {_rss_mb()} MB")
            batch = curs.fetchmany(args.fetch_size)
            fetch_rss = _rss_mb()
            _close_cursor(curs)
            if not batch:
                INFO(f"Fetch returned no rows | RSS {fetch_rss} MB")
                break
            batch_count = len(batch)
            INFO(f"Fetch returned {batch_count:,} rows | RSS {fetch_rss} MB")
            for lfn, path in batch:
                fh.write(f"{lfn}\t{path}\n")
            last_lfn = batch[-1][0]
            count += batch_count
            del batch
            INFO(f"  ... {count:,} entries written | RSS {_rss_mb()} MB")

    INFO(f"Done. {count} entries written to {args.outfile!r}. Final RSS {_rss_mb()} MB.")
    INFO(f"Inspect the list, then run:  dst_deleter.py execute --infile {args.outfile}")

# ============================================================================
# Subcommand: execute
# ============================================================================
def _copy_tail(fh, src: int, dst: int, chunk: int = 1024 * 1024) -> None:
    """
    Copy bytes [src, EOF) to [dst, ...) within an open r+b file handle, then truncate.
    src > dst always (we're removing a section from the front of the data).
    Copies in chunks so memory use is O(chunk), not O(file size).
    """
    read_pos, write_pos = src, dst
    while True:
        fh.seek(read_pos)
        data = fh.read(chunk)
        if not data:
            break
        fh.seek(write_pos)
        fh.write(data)
        read_pos  += len(data)
        write_pos += len(data)
    fh.truncate(write_pos)


_MUNLINK = shutil.which('munlink')


_DRYRUN_SHOW_MAX = 3
# Conservative ARG_MAX budget: 2 MB total - ~512 KB for environment = ~1.5 MB for argv.
# At ~175 chars per Lustre path that allows ~8000 paths, so 2000 gives comfortable headroom.
_MUNLINK_CHUNK = 2000


def _delete_files(paths: list, dryrun: bool, shown: list) -> int:
    """
    Unlink files. Returns number of files processed.
    shown is a one-element list [n] tracking how many dryrun paths have been
    printed so far across all batches; capped at _DRYRUN_SHOW_MAX.

    Uses munlink(1) when available — it unlinks in bulk without per-file
    permission/attribute checks, which is significantly faster on Lustre.
    Falls back to Path.unlink() otherwise.
    """
    if _MUNLINK:
        return _delete_files_munlink(paths, dryrun, shown)
    return _delete_files_python(paths, dryrun, shown)


def _delete_files_munlink(paths: list, dryrun: bool, shown: list) -> int:
    if dryrun:
        for p in paths:
            if shown[0] < _DRYRUN_SHOW_MAX:
                DEBUG(f"[dryrun] would unlink {p}")
                shown[0] += 1
        return len(paths)
    for i in range(0, len(paths), _MUNLINK_CHUNK):
        chunk = paths[i : i + _MUNLINK_CHUNK]
        result = subprocess.run([_MUNLINK] + chunk, capture_output=True, text=True)
        if result.returncode != 0:
            WARN(f"munlink returned {result.returncode}: {result.stderr.strip()}")
    return len(paths)


def _delete_files_python(paths: list, dryrun: bool, shown: list) -> int:
    count = 0
    for p in paths:
        if dryrun:
            if shown[0] < _DRYRUN_SHOW_MAX:
                DEBUG(f"[dryrun] would unlink {p}")
                shown[0] += 1
            count += 1
            continue
        try:
            Path(p).unlink()
            count += 1
        except FileNotFoundError:
            WARN(f"Already gone: {p}")
        except OSError as exc:
            ERROR(f"Failed to unlink {p}: {exc}")
            sys.exit(1)
    return count


def _delete_db_batch(lfns: list, dryrun: bool) -> None:
    """Delete one batch from `files` then `datasets` by lfn."""
    quoted = "','".join(lfns)
    in_clause = f"('{quoted}')"

    files_sql    = f"DELETE FROM files    WHERE lfn      IN {in_clause}"
    datasets_sql = f"DELETE FROM datasets WHERE filename IN {in_clause}"

    if dryrun:
        sample = "', '".join(lfns[:3])
        ellipsis = f", … ({len(lfns) - 3} more)" if len(lfns) > 3 else ""
        INFO(f"[dryrun] would DELETE FROM files/datasets WHERE lfn IN ('{sample}'{ellipsis})")
        return

    curs = _db_query(_FCW, files_sql)
    if curs is not None:
        DEBUG(f"  files:    {curs.rowcount} rows deleted")
        curs.commit()

    curs = _db_query(_FCW, datasets_sql)
    if curs is not None:
        DEBUG(f"  datasets: {curs.rowcount} rows deleted")
        curs.commit()


_KNOWN_PREFIXES = [
    '/sphenix/lustre01/sphnxpro/production/',
    '/sphenix/data/data02/sphnxpro/production/',
    '/sphenix/data/data03/sphnxpro/production/',
]

def _cleanup_base_path(file_path: str, dsttype: str = None) -> str | None:
    """
    Derive the per-dsttype base directory from a known storage path.
    Path structure: {prefix}{dataset}/{physicsmode}/{tag}/{dsttype}/...
    Returns the base path up to and including dsttype, or None if unrecognised.
    If dsttype is given (from the TSV header), it overrides what is in the path
    and any SQL wildcard % is replaced with shell glob *.
    """
    prefix = next((p for p in _KNOWN_PREFIXES if file_path.startswith(p)), None)
    if prefix is None:
        return None
    parts = file_path[len(prefix):].split('/')
    if len(parts) < 4:
        return None
    dataset, physicsmode, tag, path_dsttype = parts[:4]
    effective_dsttype = dsttype.replace('%', '*') if dsttype else path_dsttype
    return f"{prefix}{dataset}/{physicsmode}/{tag}/{effective_dsttype}/"


def cmd_execute(args):
    infile = args.infile
    if not Path(infile).exists():
        ERROR(f"Work list not found: {infile!r}")
        sys.exit(2)

    if _MUNLINK:
        INFO(f"munlink found at {_MUNLINK}; will use it for bulk unlinking.")
    else:
        INFO("munlink not found; falling back to Python Path.unlink().")

    total_files = total_batches = 0
    shown = [0]
    header = {}
    first_path = None

    with open(infile, 'r+b') as fh:
        # Read header, record its end position and line count.
        header_lines = 0
        while True:
            pos = fh.tell()
            raw = fh.readline()
            if not raw:
                break
            line = raw.decode().rstrip('\n')
            if line.startswith('#'):
                header_lines += 1
                if ':' in line:
                    key, _, val = line[1:].partition(':')
                    header[key.strip()] = val.strip()
            elif line:
                parts = line.split('\t', 1)
                first_line_path = parts[1] if len(parts) == 2 else None
                fh.seek(pos)   # put the first data line back
                break
        header_end = fh.tell()

        wc = subprocess.run(['wc', '-l', infile], capture_output=True, text=True)
        grand_total = int(wc.stdout.split()[0]) - header_lines
        fh.seek(header_end)

        if not args.dryrun:
            print(f"This will delete {grand_total} files, starting with:\n  {first_line_path}")
            answer = input("Are you sure? [y/N] ").strip().lower()
            if answer not in ('y', 'yes'):
                INFO("Aborted.")
                return

        t_start = time.monotonic()

        while True:
            # Read one batch of data lines.
            lfns, paths = [], []
            while len(lfns) < args.batch_size:
                raw = fh.readline()
                if not raw:
                    break
                line = raw.decode().rstrip('\n')
                if not line or line.startswith('#'):
                    continue
                parts = line.split('\t', 1)
                if len(parts) == 2:
                    if first_path is None:
                        first_path = parts[1]
                    lfns.append(parts[0])
                    paths.append(parts[1])
                else:
                    WARN(f"Skipping malformed line: {line!r}")

            if not lfns:
                break

            batch_end = fh.tell()
            total_batches += 1
            total_files += len(lfns)
            pct = 100.0 * total_files / grand_total if grand_total else 0.0
            elapsed = time.monotonic() - t_start
            remaining = grand_total - total_files
            eta_s = int(elapsed * remaining / total_files) if total_files else 0
            eta = f"{eta_s // 3600}h{eta_s % 3600 // 60}m{eta_s % 60}s"
            INFO(f"Batch {total_batches}: {total_files} | "
                 f"{grand_total} total ({pct:.1f}%) | ETA {eta} | RSS {_rss_mb()} MB")

            # DB-first is intentional: avoid leaving catalog rows pointing at
            # files that this process has already started removing.
            _delete_db_batch(lfns, dryrun=args.dryrun)
            _delete_files(paths, dryrun=args.dryrun, shown=shown)

            if not args.dryrun:
                _copy_tail(fh, src=batch_end, dst=header_end)
                fh.seek(header_end)
                fh.flush()

    if args.dryrun and total_files > _DRYRUN_SHOW_MAX:
        INFO(f"[dryrun] … and {total_files - _DRYRUN_SHOW_MAX} more files.")

    INFO(f"Done. {total_batches} batch(es), {total_files} files processed.")

    if first_path:
        base = _cleanup_base_path(first_path, dsttype=header.get('dsttype'))
        if base:
            escaped = base.replace('*', r'\*')
            INFO(f"To remove empty directories, run:\n  dst_deleter.py cleanup --path {escaped}")


# ============================================================================
# Subcommand: cleanup
# ============================================================================
def cmd_cleanup(args):
    bases = glob.glob(args.path)
    if not bases:
        ERROR(f"No paths matched: {args.path!r}")
        sys.exit(2)

    removed = 0
    for base in sorted(bases):
        base = Path(base)
        INFO(f"Cleaning up {base}")
        for dirpath, _dirnames, _filenames in os.walk(base, topdown=False):
            p = Path(dirpath)
            if p == base:
                continue
            try:
                if args.dryrun:
                    if not any(p.iterdir()):
                        DEBUG(f"[dryrun] would rmdir {p}")
                        removed += 1
                else:
                    p.rmdir()   # raises OSError if not empty
                    removed += 1
            except OSError:
                pass

    INFO(f"{'Would remove' if args.dryrun else 'Removed'} {removed} empty directories.")

# ============================================================================
# Shared helpers
# ============================================================================
def _resolve_runs(args) -> list:
    if args.runs is not None:
        return args.runs
    p = Path(args.runlist)
    if not p.exists():
        ERROR(f"Run list file not found: {args.runlist}")
        sys.exit(2)
    with open(p) as fh:
        tokens = fh.read().split()
    runs = []
    for tok in tokens:
        try:
            runs.append(int(tok))
        except ValueError:
            WARN(f"Skipping non-integer token in runlist: {tok!r}")
    if not runs:
        ERROR("Run list file contained no valid run numbers.")
        sys.exit(2)
    return runs


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

# ============================================================================
# Argument parsing
# ============================================================================
def _parse_args():
    parser = argparse.ArgumentParser(
        description='Generate a DST file deletion list and execute batched file + DB cleanup.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Step 1 — build the work list:
  dst_deleter.py generate \\
      --runs 82300 82400 \\
      --dataset run3oo --dsttype 'DST_TRIGGERED_%' --tag pro001_pcdb001_v001 \\
      --outfile /tmp/to_delete.tsv

  # Inspect the list, then step 2 — delete files and DB entries in batches:
  dst_deleter.py execute --infile /tmp/to_delete.tsv

  # Dry-run either step:
  dst_deleter.py generate ... --dryrun
  dst_deleter.py execute  --infile /tmp/to_delete.tsv --dryrun

  # Step 3 — remove empty directories (path printed by execute):
  dst_deleter.py cleanup --path /sphenix/lustre01/sphnxpro/production/run3oo/physics/pro001_pcdb001_v001/DST_TRIGGERED_EVENT/
""",
    )

    sub = parser.add_subparsers(dest='command', required=True)

    # -- generate -------------------------------------------------------------
    gen = sub.add_parser('generate', help='Query FileCatalog and write TSV work list.')
    gen.add_argument('--fetch-size', dest='fetch_size', type=int, default=50_000,
                     help='Rows fetched per paged DB query (default: 50000).')

    rgroup = gen.add_mutually_exclusive_group(required=True)
    rgroup.add_argument('--runs', nargs='+', type=int, metavar='RUN',
                        help='One run, two for an inclusive range, or more for an explicit list.')
    rgroup.add_argument('--runlist', metavar='FILE',
                        help='Plain-text file with one run number per line.')

    gen.add_argument('--dataset', required=True, help='Dataset name, e.g. run3oo')
    gen.add_argument('--dsttype', required=True, help="DST type, e.g. 'DST_TRIGGERED_%' (% triggers LIKE)")
    gen.add_argument('--tag',     required=True, help='Production tag, e.g. pro001_pcdb001_v001')
    gen.add_argument('-o', '--outfile', required=True, help='Output TSV file (lfn<TAB>full_file_path).')
    gen.add_argument('-n', '--dryrun', action='store_true', default=False,
                     help='Print SQL without querying or writing.')
    _add_verbosity(gen)
    gen.set_defaults(func=cmd_generate)

    # ── execute ───────────────────────────────────────────────────────────────
    exe = sub.add_parser('execute', help='Delete files and DB entries batch by batch.')

    exe.add_argument('-i', '--infile', required=True, help='TSV work list produced by generate.')
    exe.add_argument('--batch-size', dest='batch_size', type=int, default=10_000,
                     help='DB rows deleted and infile lines removed per batch (default: 10000). '
                          'munlink is called in internal chunks of 2000 regardless of this value.')
    exe.add_argument('-n', '--dryrun', action='store_true', default=False,
                     help='Print what would happen without deleting anything.')
    _add_verbosity(exe)
    exe.set_defaults(func=cmd_execute)

    # ── cleanup ───────────────────────────────────────────────────────────────
    cln = sub.add_parser('cleanup', help='Remove empty directories under a base path.')

    cln.add_argument('--path', required=True,
                     help='Base directory to clean up (printed by execute for lustre paths).')
    cln.add_argument('-n', '--dryrun', action='store_true', default=False,
                     help='Print directories that would be removed without removing them.')
    _add_verbosity(cln)
    cln.set_defaults(func=cmd_cleanup)

    return parser.parse_args()


def main():
    args = _parse_args()
    _set_log_timestamps_enabled(False)
    _set_loglevel(args)

    args.func(args)


if __name__ == '__main__':
    main()
