#!/usr/bin/env bash

MAX_ARG_COUNT=3
if [ "$#" -gt "$MAX_ARG_COUNT" ]; then
    echo "Unsupported call:"
    echo $@
    echo Abort.
    exit 0
fi

filename=${1} #filename=`basename ${1}`   # must be a local file
destination=${2}
dbid=${3}

echo stageout ${filename} ${destination} dbid=${dbid} start `date`

if [ ! -f ${filename} ]; then
    echo "${filename} not found!"
    echo ls -lahtr
    ls -lahtr
    exit 0
fi

# Could test the destination like this, but we want to minimize lustre probing in the worker jobs
# if [ -d "$destination" ]; then   echo "$DIRECTORY does exist." ; else exit 1; fi

# Number of events, -1 if empty or 0:
nevents=$( root.exe -q -b GetEntries.C\(\"${filename}\"\) | awk '/Number of Entries/{ print $4; }' )
nevents=${nevents:--1}

# md5sum:
md5=`/usr/bin/env md5sum ${filename} | cut -d ' ' -f 1`

# dbid, for faster access to the production db
if [ -z "$dbid" ]; then
    dbid="nodbid"
fi

#change the destination filename
destname=`basename ${filename}`
destname="${destname}:nvents:${nevents}"
destname="${destname}:md5:${md5}"
destname="${destname}:dbid:${dbid}"

action="cp -v ${filename} ${destination}/${destname} && rm -v ${filename}"
echo ${action}
echo
eval ${action}

exit 0 # Fom Jason: stageout should never propagate a failed error code...

# # spider can pick this back up with
# gotname=`echo ${destname} | cut -d ':' -f 1`
# gotnev=`echo ${destname} | cut -d ':' -f 3`
# gotmd5=`echo ${destname} | cut -d ':' -f 5`
# gotdbid=`echo ${destname} | cut -d ':' -f 7`
# echo "destname        : ${destname}"
# echo "gotname         : ${gotname}"
# echo "destination dir : $destination"
# echo "gotnev          : $gotnev"
# echo "gotmd           : $gotmd5"
# echo "gotdbid         : $gotdbid"



# nevents_=$( root.exe -q -b GetEntries.C\(\"${filename}\"\) | awk '/Number of Entries/{ print $4; }' )
# nevents=${nevents_:--1}

# # prodtype is required... specifies whether the production status entry manages a single output file (only) or many output files (many).
# echo ./cups.py -r ${runnumber} -s ${segment} -d ${dstname}  stageout ${filename} ${destination} --dsttype ${dsttype} --dataset ${build}_${dbtag} --nevents ${nevents} --inc --prodtype many
#      ./cups.py -r ${runnumber} -s ${segment} -d ${dstname}  stageout ${filename} ${destination} --dsttype ${dsttype} --dataset ${build}_${dbtag} --nevents ${nevents} --inc --prodtype many

# echo stageout ${filename} ${destination} finish `date`




