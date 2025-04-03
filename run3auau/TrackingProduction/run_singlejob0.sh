#!/usr/bin/bash

#     arguments             : "$(nevents) {outbase} {logbase} $(run) $(seg) $(outdir) $(buildarg) $(tag) $(inputs) $(ranges) {neventsper} {logdir} {comment} {histdir} {PWD} {rsync}"
nevents=${1}
outbase=${2}
logbase=${3}
runnumber=${4}
segment=${5}
outdir=${6}
build=${7/./}
dbtag=${8}
inputs=(`echo ${9} | tr "," " "`)  # array of input files 
ranges=(`echo ${10} | tr "," " "`)  # array of input files with ranges appended
neventsper=${11}
logdir=${12:-.}
comment=${13}
histdir=${14:-.}
subdir=${15}
payload=(`echo ${16} | tr ","  " "`) # array of files to be rsynced
# ---
{
    
export cupsid=${@: -1}
echo CUPSID=${cupsid}

export USER="$(id -u -n)"
export LOGNAME=${USER}
export HOME=/sphenix/u/${USER}
hostname

source /opt/sphenix/core/bin/sphenix_setup.sh -n ${7}

echo OFFLINE_MAIN: $OFFLINE_MAIN

# user has supplied an odbc.ini file.  use it.
if [ -e odbc.ini ]; then
echo "Setting user provided odbc.ini file"
export ODBCINI=./odbc.ini
fi

echo ..............................................................................................
echo $@
echo .............................................................................................. 
echo nevents: $nevents
echo outbase: $outbase
echo logbase: $logbase
echo runnumb: $runnumber
echo segment: $segment
echo outdir:  $outdir
echo build:   $build
echo dbtag:   $dbtag
echo inputs:  ${inputs[@]}
echo subdir:  ${subdir}
echo payload: ${payload[@]}

echo .............................................................................................. 

for i in ${payload[@]}; do
    cp --verbose ${subdir}/${i} .
done

#______________________________________________________________________________________ started __
#
./cups.py -r ${runnumber} -s ${segment} -d ${outbase} started
#_________________________________________________________________________________________________


if [[ "${9}" == *"dbinput"* ]]; then
    echo GRABBING DBINPUT $runnumber $segment $outbase
    echo ./cups.py -r ${runnumber} -s ${segment} -d ${outbase} getinputs
    ./cups.py -r ${runnumber} -s ${segment} -d ${outbase} getinputs
    
    for i in $(./cups.py -r ${runnumber} -s ${segment} -d ${outbase} getinputs); do
       cp -v ${i} .
       echo $( basename $i ) >> inlist   
    done
else
    echo WFT???
  for i in ${inputs[@]}; do
     cp -v ${i} .
     echo $( basename $i ) >> inlist   
  done
fi

echo "Here is the input list"
cat inlist

./cups.py -r ${runnumber} -s ${segment} -d ${outbase} running

dstname=${logbase%%-*}
echo "in dir"
pwd
echo root.exe -q -b Fun4All_SingleJob0.C\(${nevents},${runnumber},\"${logbase}.root\",\"${dbtag}\",\"inlist\"\)
     root.exe -q -b Fun4All_SingleJob0.C\(${nevents},${runnumber},\"${logbase}.root\",\"${dbtag}\",\"inlist\"\);  status_f4a=$?

ls -la

echo ./stageout.sh ${logbase}.root ${outdir}
     ./stageout.sh ${logbase}.root ${outdir}

for hfile in `ls HIST_*.root`; do
    echo Stageout ${hfile} to ${histdir}
    ./stageout.sh ${hfile} ${histdir}
done

ls -la

# Flag run as finished. 
echo ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} finished -e ${status_f4a} --nevents ${nevents}  
     ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} finished -e ${status_f4a} --nevents ${nevents}

echo "bdee bdee bdee, That's All Folks!"

} 
#>${logdir#file:/}/${logbase}.out  2>${logdir#file:/}/${logbase}.err


exit $status_f4a
