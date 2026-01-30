# sphenixprod
Production Toolchain for the sPHENIX experiment

Originally based on https://github.com/klendathu2k/slurp with the goal of streamlining and scaling to keep O(100k) farm nodes occupied.

## Installation
All (at least hopefully) dependencies are in `requirements.txt`. 
```sh
pip install -r requirements.txt
```

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


