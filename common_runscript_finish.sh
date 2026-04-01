#!/usr/bin/env bash
## Final bookkeeping for sPHENIX production job scripts.
## Source this after stageout calls, with status_f4a, user_cpu, sys_cpu,
## max_rss_kb set by common_runscript_exec.sh.

ls -la

python ${SPHENIXPROD_SCRIPT_PATH}/sphenixdbutils.py jobended \
    --exit-code   ${status_f4a:-1} \
    ${user_cpu:+  --user-cpu   ${user_cpu}} \
    ${sys_cpu:+   --sys-cpu    ${sys_cpu}} \
    ${max_rss_kb:+--memory-kb  ${max_rss_kb}}

echo All done
