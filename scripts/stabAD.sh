#!/bin/bash

MEM=$1
SDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts"
WDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims/${MEM}"
case=$(<case.txt)

python "${SDIR}/spinup_stability.py" AD.yml
status=$?
echo "status: ${status}"

if [[ "${status}" == "11" ]]; then
    echo "AD needs more spinup"
    cd $case
    ./xmlchange CONTINUE_RUN=True
    ./xmlchange STOP_N=20
    ./xmlchange JOB_WALLCLOCK_TIME="1:00:00" --subgroup case.run
    ./xmlchange JOB_WALLCLOCK_TIME="0:20:00" --subgroup case.st_archive
    cd $WDIR
    echo $case>case.txt
    echo "${SDIR}/stabAD.sh ${MEM}">commands.txt
elif [[ "$status" == "0" ]]; then
    echo "AD spinup appears sufficient"
    ${SDIR}/setupPAD.sh $MEM
else
    echo "something looks wrong, halting tether"
    rm commands.txt
fi
