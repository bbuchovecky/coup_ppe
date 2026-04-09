#!/bin/bash

SDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts"
CASEROOTDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims"
# MEMBERS=(000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)
MEMBERS=(001)

for m in "${MEMBERS[@]}"; do

    mem="coupPPE.${m}"
    wdir="${CASEROOTDIR}/${mem}"

    echo "MEMBER = ${mem}"
    echo "CASEROOT = ${wdir}"

    cd $SDIR

    cp run.job "runOFFLHIST.${m}.job"
    echo -e "\n./runOFFLHIST.sh ${mem}" >> "runOFFLHIST.${m}.job"
    sed -i "s/name/runOFFLHIST.${m}/g" "runOFFLHIST.${m}.job"

    # RUNJOB=$(qsub "runOFFLHIST.${m}.job")
    # echo $RUNJOB
    # qsub -W depend=afterok:$RUNJOB -v MEM=${mem} checkOFFLHIST.job


done
