#!/usr/bin/bash

filename=`basename ${1}`   # must be a local file
destination=${2}

echo stageout ${filename} ${destination} start `date`

# An option version number is optionally added to the filenaming convention.  It is made part of the dataset name.
# We add the optional _vxxx tag to the dbtag portion of this regex... so note well that 'dbtag' is overloaded in
# this context.  It now indicates both the conditions database tag and the file version number.

regex_dsttype_run="([A-Z]+_[A-Z0-9_]+[a-z0-9]+)_([a-z0-9]+)_(202[345]p[0-9][0-9][0-9][_v0-9]*|nocdbtag[_v0-9]*)-([0-9]+)-([0-9]+)"
regex_dsttype_range="([A-Z]+_[A-Z_]+[a-z0-9]+)_([a-z0-9]+)_(202[3456789]p[0-9][0-9][0-9][_v0-9]*|nocdbtag[_v0-9]*)-([0-9]+)-([0-9]+)-([0-9]+)"

# decode filename
base=${filename/.root/}
dstname=${base%%-*}

# Filename matches a dsttype with a single run
if [[ $base =~ $regex_dsttype_run ]]; then
   dsttype=${BASH_REMATCH[1]}
 echo $dsttype ...
   build=${BASH_REMATCH[2]}
 echo $build
   dbtag=${BASH_REMATCH[3]}
 echo $dbtag
   runnumber=${BASH_REMATCH[4]}
 echo $runnumber
   segment=${BASH_REMATCH[5]}
 echo $segment
fi
# Filename matches a dst "run range" type
if [[ $base =~ $regex_dsttype_range ]]; then
   dsttype=${BASH_REMATCH[1]}
 echo $dsttype ...
   build=${BASH_REMATCH[2]}
 echo $build
   dbtag=${BASH_REMATCH[3]}
 echo $dbtag
   runnumber=${BASH_REMATCH[4]}
 echo $runnumber
   runnumber2=${BASH_REMATCH[5]}
 echo $runnumber
   segment=${BASH_REMATCH[6]}
 echo $segment
fi

nevents_=$( root.exe -q -b GetEntries.C\(\"${filename}\"\) | awk '/Number of Entries/{ print $4; }' )
nevents=${nevents_:--1}

# prodtype is required... specifies whether the production status entry manages a single output file (only) or many output files (many).
echo ./cups.py -r ${runnumber} -s ${segment} -d ${dstname}  stageout ${filename} ${destination} --dsttype ${dsttype} --dataset ${build}_${dbtag} --nevents ${nevents} --inc --prodtype many
     ./cups.py -r ${runnumber} -s ${segment} -d ${dstname}  stageout ${filename} ${destination} --dsttype ${dsttype} --dataset ${build}_${dbtag} --nevents ${nevents} --inc --prodtype many

echo stageout ${filename} ${destination} finish `date`

exit 0 # stageout should never propagate a failed error code...



