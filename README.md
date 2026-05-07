# sphenixprod
Production Toolchain for the sPHENIX experiment

Originally based on https://github.com/klendathu2k/slurp with the goal of streamlining and scaling to keep O(100k) farm nodes occupied.

## Installation
All (at least hopefully) dependencies are in `requirements.txt`. 
```sh
pip install -r requirements.txt
```

## Job Exit Codes

All job scripts report a final exit code via `common_runscript_finish.sh`, which records it in the production database. Codes are designed to identify the failure stage at a glance:

| Code | Stage | Meaning |
|------|-------|---------|
| 0 | — | Success |
| 2 | Setup | Bad arguments or configuration error |
| 3 | Setup | Unsupported OS / environment setup failed |
| 10 | Input | No input files found (DB query returned empty) |
| 11 | Input | Remote file health check failed (missing or wrong size) |
| 20 | Stage-in | Input file copy failed (dd retries exhausted or source missing) |
| 21 | Stage-in | Input file md5 mismatch after copy |
| 30 | Stage-out | Output file not found (macro produced no output) |
| 31 | Stage-out | Output file copy failed (dd retries exhausted) |
| 111 | Input | Streaming: wrong number of GL1 or detector list files |
| other | Macro | Propagated directly from `root.exe` exit code |

## Chunking Support

For large run lists, the submission process can be time-consuming when processing all runs at once. The `--chunk-size` parameter allows you to process runs in smaller chunks, enabling faster feedback and more incremental progress.

### Usage

```bash
# Process all runs at once (default behavior)
create_submission.py --config config.yaml --rulename RULE --runs 1000 2000

# Process runs in chunks of 50
create_submission.py --config config.yaml --rulename RULE --runs 1000 2000 --chunk-size 50

# Process runs from a runlist file in chunks of 100
create_submission.py --config config.yaml --rulename RULE --runlist runs.txt --chunk-size 100
```

### How It Works

- Each chunk goes through the complete pipeline: matching → file creation → DB updates → optional submission
- Runs are processed newest-first within each chunk
- With `--andgo`, jobs are submitted after each chunk completes
- Default: `--chunk-size 0` processes all runs at once (backward compatible)

### Benefits

1. **Faster Time to First Submission**: Start submitting jobs sooner rather than waiting for all runs to be processed
2. **Better Resource Management**: Spread processing over time to avoid overwhelming resources
3. **Incremental Progress**: See results from earlier chunks while later chunks are still processing
4. **Easier Debugging**: Smaller chunks make it easier to identify and fix issues


