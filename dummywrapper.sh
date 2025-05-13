#!/usr/bin/env bash

## Logging details
echo Hostname: `hostname`
echo Script: $0
echo Arguments: $@
echo


# This script expects arguments in the order defined by the Condor submission:
# nevents, outbase, logbase, run, seg, outdir, buildarg, tag, inputs, 
# ranges, neventsper, logdir, comment, histdir, PWD_from_condor, rsync_from_condor

EXPECTED_ARG_COUNT=16

if [ "$#" -ne "$EXPECTED_ARG_COUNT" ]; then
    echo "Error: Incorrect number of arguments."
    echo "Expected $EXPECTED_ARG_COUNT, but received $#."
    echo "Arguments received: $@"
    echo "Usage: $0 <nevents> <outbase> <logbase> <run> <seg> <outdir> <buildarg> <tag> <inputs> <ranges_str> <neventsper> <log_dir> <comment_str> <hist_dir> <condor_pwd_val> <condor_rsync_val>"
    exit 1
fi

# Parse arguments using shift
nevents="$1"; shift
outbase="$1"; shift
logbase="$1"; shift
run_number="$1"; shift         # Corresponds to {run}
segment="$1"; shift            # Corresponds to {seg}
output_directory="$1"; shift   # Corresponds to {outdir}
build_argument="$1"; shift     # Corresponds to {buildarg}
tag_value="$1"; shift          # Corresponds to {tag}
input_files="$1"; shift        # Corresponds to {inputs}
ranges_string="$1"; shift      # Corresponds to $(ranges)
nevents_per_job="$1"; shift    # Corresponds to {neventsper}
log_directory="$1"; shift      # Corresponds to {logdir}
job_comment="$1"; shift        # Corresponds to {comment}
histogram_directory="$1"; shift # Corresponds to {histdir}
condor_pwd="$1"; shift         # Corresponds to {PWD}
# condor_rsync="$1"; shift       # Corresponds to {rsync}
condor_rsync=`echo $1|sed 's/,/ /g'`; shift       # Corresponds to {rsync}, change from comma separation

# Now we can use these variables in your script
echo "Processing job with the following parameters:"
echo "---------------------------------------------"
echo "Number of events to process (nevents): $nevents"
echo "Output base name (outbase):            $outbase"
echo "Log base name (logbase):               $logbase"
echo "Run number (run):                      $run_number"
echo "Segment number (seg):                  $segment"
echo "Output directory (outdir):             $output_directory"
echo "Build argument (buildarg):             $build_argument"
echo "Tag (tag):                             $tag_value"
echo "Ranges string (ranges):                $ranges_string"
echo "Events per sub-job (neventsper):       $nevents_per_job"
echo "Log directory (logdir):                $log_directory"
echo "Job comment (comment):                 $job_comment"
echo "Histogram directory (histdir):         $histogram_directory"
echo "Condor PWD (PWD):                      $condor_pwd"
echo "Condor Rsync Paths (rsync):            $condor_rsync" 
echo "---------------------------------------------"
echo "Input file(s) (inputs):                $input_files"
echo "---------------------------------------------"

## Make sure logfiles are kept even when receiving a signal
sighandler()
{
mv ${logbase}.out ${logdir#file:/}
mv ${logbase}.err ${logdir#file:/}
}
trap sighandler SIGTERM 
trap sighandler SIGSTOP 
trap sighandler SIGINT 
## trap sighandler SIGKILL # kill -0 can't be trapped

# Note: Original jobwrapper.sh has more intricate trapping that
# catches more signals and updates the prod db.

OS=$( hostnamectl | awk '/Operating System/{ print $3" "$4 }' )
# OS=$( grep ^NAME /etc/os-release | sed 's/"/ /g'|awk '{print $2}' ) # alternative, works on more linux systems
echo "Setting up Production software for for ${OS}"

# Set up environment
source /opt/sphenix/core/bin/sphenix_setup.sh -n $build_argument
export PATH=${PATH}:${HOME}/bin
# echo env: `env`

#TODO: Need cupsid - assigned at job registration in the prod db for efficient updates
# echo cupsid:     ${cupsid}

# stage in the payload files
cd $_CONDOR_SCRATCH
echo Copying payload data to `pwd`
for f in ${condor_rsync}; do
    echo $f
    cp --verbose -r $f . 
done


exit 0

