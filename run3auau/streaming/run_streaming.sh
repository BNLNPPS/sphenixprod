#!/usr/bin/bash

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
neventsper=${11:-1000}
logdir=${12:-.}
comment=${13}
histdir=${14:-.}
subdir=${15}
payload=(`echo ${16} | tr ","  " "`) # array of files to be rsynced
#-----
export cupsid=${@: -1}
echo cupsid = $cupsid

sighandler()
{
mv ${logbase}.out ${logdir#file:/}
mv ${logbase}.err ${logdir#file:/}
}
trap sighandler SIGTERM 
trap sighandler SIGSTOP 
trap sighandler SIGINT 
trap sighandler SIGKILL


{

export USER="$(id -u -n)"
export LOGNAME=${USER}
export HOME=/sphenix/u/${USER}

source /opt/sphenix/core/bin/sphenix_setup.sh -n ${7}

echo OFFLINE_MAIN: $OFFLINE_MAIN

echo "PAYLOAD"
for i in ${payload[@]}; do
    cp --verbose ${subdir}/${i} .
done

if [ -e odbc.ini ]; then
echo export ODBCINI=./odbc.ini
     export ODBCINI=./odbc.ini
else
     echo No odbc.ini file detected.  Using system odbc.ini
fi

echo "CUPS configuration"
echo ./cups.py -r ${runnumber} -s ${segment} -d ${outbase} info
     ./cups.py -r ${runnumber} -s ${segment} -d ${outbase} info


echo ./cups.py -r ${runnumber} -s ${segment} -d ${outbase} started
     ./cups.py -r ${runnumber} -s ${segment} -d ${outbase} started     

echo "INPUTS" 
if [[ "${9}" == *"dbinput"* ]]; then
   echo   ./cups.py -r ${runnumber} -s ${segment} -d ${outbase} getinputs
          ./cups.py -r ${runnumber} -s ${segment} -d ${outbase} getinputs >> inputfiles.list
else
   for i in ${inputs[@]}; do
      echo $i >> inputfiles.list
   done
fi

#while read -r fname; do
#   echo $fname
#done < inputfiles.list

#______________________________________________________________________________________________
# Map TPC input files into filelists
# TPC_ebdc23_cosmics-00030117-0009.evt test%%_cosmics*
inputlist=""
#for f in "${inputs[@]}"; do
cat inputfiles.list | while read -r f; do
    b=$( basename $f )
    # TPC files
    if [[ $b =~ "TPC_ebdc" ]]; then
       l=${b%%_cosmics*}  # handle either cosmic events or calibrations or beam...
       l=${l%%_calib*}
       l=${l%%_beam*}
       l=${l%%_physics*}
       echo ${f} >> ${l/TPC_ebdc/tpc}.list
       echo Add ${f} to ${l/TPC_ebdc/tpc}.list
       inputlist="${f} ${inputlist}"
    fi
    # TPOT files
    if [[ $b =~ "TPOT_ebdc" ]]; then
       echo ${f} >> tpot.list 
       echo Add ${f} to tpot.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "GL1_cosmics" ]]; then
       echo ${f} >> gl1.list
       echo Add ${f} to gl1.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "cosmics_intt" ]]; then
       l=${b#*cosmics_}
       l=${l%%-*}
       echo ${f} >> ${l}.list
       echo Add ${f} to ${l}.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "cosmics_mvtx" ]]; then
       l=${b#*cosmics_}
       l=${l%%-*}
       echo ${f} >> ${l}.list
       echo Add ${f} to ${l}.list
       inputlist="${f} ${inputlist}"
    fi

    if [[ $b =~ "GL1_beam" ]]; then
       echo ${f} >> gl1.list
       echo Add ${f} to gl1.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "beam_intt" ]]; then
       l=${b#*beam_}
       l=${l%%-*}
       echo ${f} >> ${l}.list
       echo Add ${f} to ${l}.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "beam_mvtx" ]]; then
       l=${b#*beam_}
       l=${l%%-*}
       echo ${f} >> ${l}.list
       echo Add ${f} to ${l}.list
       inputlist="${f} ${inputlist}"
    fi

    if [[ $b =~ "GL1_calib" ]]; then
       echo ${f} >> gl1.list
       echo Add ${f} to gl1.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "calib_intt" ]]; then
       l=${b#*calib_}
       l=${l%%-*}
       echo ${f} >> ${l}.list
       echo Add ${f} to ${l}.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "calib_mvtx" ]]; then
       l=${b#*calib_}
       l=${l%%-*}
       echo ${f} >> ${l}.list
       echo Add ${f} to ${l}.list
       inputlist="${f} ${inputlist}"
    fi

    if [[ $b =~ "GL1_physics" ]]; then
       echo ${f} >> gl1.list
       echo Add ${f} to gl1.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "physics_intt" ]]; then
       l=${b#*physics_}
       l=${l%%-*}
       echo ${f} >> ${l}.list
       echo Add ${f} to ${l}.list
       inputlist="${f} ${inputlist}"
    fi
    if [[ $b =~ "physics_mvtx" ]]; then
       l=${b#*physics_}
       l=${l%%-*}
       echo ${f} >> ${l}.list
       echo Add ${f} to ${l}.list
       inputlist="${f} ${inputlist}"
    fi
    
done

#______________________________________________________________________________________________

touch gl1.list
touch intt0.list
touch intt1.list
touch intt2.list
touch intt3.list
touch intt4.list
touch intt5.list
touch intt6.list
touch intt7.list
touch mvtx0.list
touch mvtx1.list
touch mvtx2.list
touch mvtx3.list
touch mvtx4.list
touch mvtx5.list
touch tpot.list

ls -la *.list

# If no input files are in the file lists exit with code 111 to indicate a failure
if [ $(cat *.list|wc -l) -eq 0 ]; then
     ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} finished -e 111 --nevents 0 --inc 
     exit 111
fi

# Flag job as running in production status
./cups.py -r ${runnumber} -s ${segment} -d ${outbase} running

# Flag the creation of a new dataset in dataset_status
dstname=${logbase%%-*}

echo root.exe -q -b Fun4All_Stream_Combiner.C\(${nevents},${runnumber},\"${outbase}\",\"${outdir}\",${neventsper}\);
     root.exe -q -b Fun4All_Stream_Combiner.C\(${nevents},${runnumber},\"${outbase}\",\"${outdir}\",${neventsper}\); status_f4a=$?

# There should be no output files hanging around  (TODO add number of root files to exit code)
ls -la 

# Flag run as finished.  Increment nevents by zero
echo ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} finished -e ${status_f4a} --nevents 0 --inc 
     ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} finished -e ${status_f4a} --nevents 0 --inc 


echo $outbase
echo $logbase

for hfile in `ls HIST_*.root`; do
    echo Stageout ${hfile} to ${histdir}
    ./stageout.sh ${hfile} ${histdir}
done

# Cleanup any stray root and/or list files leftover from stageout
rm *.root *.list

ls -la

logsize=$( du -s ${logdir#file:/}/${logbase}.out | awk '//{ print $1 }' )
if [ "$logsize" -gt 10240 ];
then
   ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} message "Normal termination with large log file" --flag 10 --logsize ${logsize}
else
   ./cups.py -v -r ${runnumber} -s ${segment} -d ${outbase} message "Normal termination" --flag 0 --logsize ${logsize}
fi

echo "script done"
} | tee ${logdir#file:/}/${logbase}.out 

echo "Job termination with logsize= " ${logsize} "kB"


#mv ${logbase}.out ${logdir#file:/}
#mv ${logbase}.err ${logdir#file:/}

if [ -e cups.stat ]; then
    cp cups.stat ${logdir#file:/}/${logbase}.dbstat
fi

exit $status_f4a


