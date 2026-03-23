# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`sphenixprod` is the production toolchain for the sPHENIX experiment at BNL. It manages large-scale physics data processing by submitting HTCondor jobs, querying PostgreSQL databases for input file matching, and tracking job state across two databases (Production DB and FileCatalog).

## Environment Setup

```bash
source sphenixprod/this_sphenixprod.sh
```

This sources `/opt/sphenix/core/bin/sphenix_setup.sh`, adds scripts to `PATH`, sets `PYTHONPATH`, and sets `ODBCINI=./.odbc.ini`. The `.odbc.ini` file must exist in the working directory for database connections to work.

## Key Commands

**Dry-run submission (no jobs submitted, no DB writes):**
```bash
create_submission.py --config ProdFlow/short/run3auau/v001_combining_run3_new_nocdbtag.yaml \
  --rule DST_TRIGGERED_EVENT_run3physics --runs 69600 72000 -n -vv
```

**Real submission:**
```bash
create_submission.py --config <config.yaml> --rule <RULENAME> --runs <START> <END> --andgo -vv
```

**Chunked submission (faster feedback for large run lists):**
```bash
create_submission.py --config <config.yaml> --rule <RULENAME> --runs <START> <END> \
  --chunk-size 100 --andgo -vv
```

**Autopilot (cron-driven, per-host dispatch):**
```bash
production_control.py --steerfile <steer.yaml>
```

**Install dependencies:**
```bash
pip install -r requirements.txt
```

## Architecture

### Pipeline flow

`create_submission.py` is the main entry point. It:
1. Parses a YAML config file into a `RuleConfig` (via `sphenixprodrules.py`)
2. Builds a `MatchConfig` (via `sphenixmatching.py`) which queries the databases to find input files for the given run range
3. Creates `CondorJob` objects (via `sphenixcondorjobs.py`) and writes HTCondor submit files
4. Optionally submits via `execute_condorsubmission.py` (with `--andgo`)

### Autopilot flow

`production_control.py` is intended for cron jobs. It reads a steer YAML keyed by hostname, and for each rule calls `create_submission.py`, `dstspider.py`, `histspider.py`, and `monitor_finish.py` as configured. `dispatch_productions.py` runs multiple `production_control.py` instances from a steer-list file with configurable stagger.

### Core library modules

| Module | Role |
|---|---|
| `sphenixprodrules.py` | `RuleConfig` dataclass: parses YAML rule/job/input config; filesystem path templates |
| `sphenixmatching.py` | `MatchConfig`: DB queries to match input files for a run range |
| `sphenixcondorjobs.py` | `CondorJobConfig` / `CondorJob`: maps to HTCondor submit parameters |
| `sphenixdbutils.py` | Interface to Production DB and FileCatalog via pyodbc; determines test vs. prod mode |
| `sphenixjobdicts.py` | Dictionaries mapping DST output types to their required input types |
| `sphenixmisc.py` | Utilities: `shell_command`, lock/unlock files, rotating log setup |
| `simpleLogger.py` | Custom logger with levels: CHATTY < DEBUG < INFO < WARN < ERROR < CRITICAL |
| `argparsing.py` | Shared argument parsing (`--runs`, `--runlist`, `--dryrun`, verbosity flags, etc.) |

### Configuration (YAML)

Production rules live in `ProdFlow/` subdirectories. Each YAML file defines one or more named rules. A rule specifies input DST types, output DST type, build/dbtag/version triplet, resource requests, and filesystem overrides. The steer files for `production_control.py` are also YAML, keyed by hostname, with `submit`/`dstspider`/`histspider`/`finishmon` flags per rule.

### Test vs. production mode

`sphenixdbutils.py` activates **test mode** if:
- The current directory path contains `testbed`
- A `.testbed` file exists
- A `SPHNX_TESTBED_MODE` file exists

**Production mode** is activated by a `SPHNX_PRODUCTION_MODE` file (changes DB DSN).

### Databases

- **Production DB**: `sphnxproddbmaster.sdcc.bnl.gov`, database `Production`, table `production_status`
- **FileCatalog**: `sphnxdbmaster.sdcc.bnl.gov`, database `FileCatalog`, tables `files` and `datasets`

Access requires pyodbc and a valid `.odbc.ini` in the working directory.

### Filesystem layout

Output files follow the template defined in `sphenixprodrules.py`:
```
/sphenix/lustre01/sphnxpro/{prodmode}/{period}/{physicsmode}/{outtriplet}/{leafdir}/{rungroup}/
```
Logs go to `/sphenix/data/data02/sphnxpro/...` and submission logs to `/tmp/sphenixprod/sphenixprod/`.

### Post-processing scripts

- `dstspider.py` — crawls output dirs, registers finished DST files in FileCatalog
- `histspider.py` — same for histogram files
- `monitor_finish.py` — monitors job completion status
- `eradicate_runs.py` — removes runs from DBs and optionally filesystem
- `resubmit_to_condor.py` — resubmits failed jobs

### Verbosity flags

All scripts share `-v` (INFO), `-vv` / `-d` (DEBUG), `-vvv` / `-c` (CHATTY), or `--loglevel LEVEL`. Use `-n` / `--dryrun` to suppress all DB writes and job submissions.
