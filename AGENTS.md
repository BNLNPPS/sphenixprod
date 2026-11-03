@../prodmacros/AGENTS.md

# sphenixprod agent orientation

Read `../prodmacros/AGENTS.md` before working here; Claude Code imports it through the line above. If the sibling checkout is unavailable, locate it in the [prodmacros repository](https://github.com/sPHENIX-Collaboration/prodmacros). This file gives the `sphenixprod` code map; `README.md` has command examples and detailed usage.

## Runtime and entry points

- From this repository, source `./this_sphenixprod.sh` before running its commands. It sets `ODBCINI=./.odbc.ini`, so the file must be in the current working directory for database access.
- `create_submission.py` parses a rule YAML through `sphenixprodrules.py`, matches inputs through `sphenixmatching.py`, creates `sphenixcondorjobs.py` jobs, and optionally submits them via `execute_condorsubmission.py` when `--andgo` is set. The argument parser uses `--rulename` for a direct rule selection.
- `master_production_control.sh` invokes `dispatch_productions.py` for each active pilot. `production_control.py` selects the pilot's hostname entry and launches submission and spiders according to its flags. Check the current parser and pilot before using a placeholder or option.
- `dstspider.py` and `histspider.py` register finished outputs in FileCatalog. `resubmit_to_condor.py`, `custom_resubmit.py`, `find_stale_jobs.py`, `check_downstream.py`, and `dst_deleter.py` handle later job inspection or maintenance. Consult each command's help and source before using a write or delete action.

## Code and data

- `sphenixdbutils.py` maps the Production read and write DSNs and the FileCatalog DSN; job tracking uses `production_jobs`. `sphenixjobdicts.py` maps DST output types to required inputs. `argparsing.py` owns common command arguments.
- On the shared production host, there is no testbed database switch. A command capable of writing can affect production unless its documented dry-run path is used. Check the code for each command; some inspection tools are read only.
- Default output and log locations come from `sphenixprodrules.py`, and rule YAMLs may override them. Rule templates live in `prodmacros/run*/`; instances keep their own rules and pilots under `thisProd/`.

## Legacy

- `production_status` and the pilot's `finishmon` / `monitor_finish.py` path remain for legacy use. Current job tracking uses `production_jobs`; do not treat those legacy paths as part of a normal new production.
