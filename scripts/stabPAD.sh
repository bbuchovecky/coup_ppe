#!/bin/bash

MEM=$1
SDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts"
WDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims/${MEM}"
case=$(<case.txt)

python "${SDIR}/spinup_stability.py" PAD.yml
status=$?
echo "status: ${status}"

if [[ "${status}" == "11" ]]; then
    echo "pAD needs more spinup"
    cd $case
    ./xmlchange CONTINUE_RUN=True
    ./xmlchange STOP_N=60
    ./xmlchange JOB_WALLCLOCK_TIME="2:45:00" --subgroup case.run
    cd $WDIR
    echo $case>case.txt
    echo "${SDIR}/stabPAD.sh ${MEM}">commands.txt
elif [[ "$status" == "0" ]]; then
    echo "PAD spinup appears sufficient"
    ${SDIR}/setupIHIST.sh $MEM
else
    echo "something looks wrong, halting tether"
    rm commands.txt
fi
