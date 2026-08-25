#!/usr/bin/env bash
## Final bookkeeping for sPHENIX production job scripts.
## Source this after stageout calls, with status_f4a, user_cpu, sys_cpu,
## exec_wall_sec, max_rss_kb, exec_cpu_percent set by common_runscript_exec.sh.
## stagein_wall_sec and stageout_wall_sec are accumulated by stagein.sh and stageout.sh.

status_f4a=${status_f4a:-1}

ls -la

python ${SPHENIXPROD_SCRIPT_PATH}/sphenixdbutils.py jobended \
    --exit-code   ${status_f4a} \
    ${user_cpu:+  --user-cpu   ${user_cpu}} \
    ${sys_cpu:+   --sys-cpu    ${sys_cpu}} \
    ${exec_wall_sec:+--exec-wall-sec ${exec_wall_sec}} \
    ${exec_cpu_percent:+--exec-cpu-percent ${exec_cpu_percent}} \
    ${stagein_wall_sec:+--stagein-wall-sec ${stagein_wall_sec}} \
    ${stageout_wall_sec:+--stageout-wall-sec ${stageout_wall_sec}} \
    ${max_rss_kb:+--memory-kb  ${max_rss_kb}}

if [[ ${status_f4a} -eq 0 ]]; then
    echo "All done. Job completed successfully."
else
    echo "Job failed with exit code ${status_f4a}."
fi
exit ${status_f4a}
