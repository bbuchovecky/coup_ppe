#!/bin/bash

SDIR="/glade/u/home/bbuchovecky/projects/cpl_ppe_co2/scripts"
MEMBERS=(003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)

for m in "${MEMBERS[@]}"; do

    mem="coupPPE.${m}"
    echo "MEMBER = ${mem}"

    cd $SDIR
    cp runFHIST.job "runFHIST.${m}.job"

    echo -e "\n./runFHIST.sh ${mem}" >> "runFHIST.${m}.job"
    sed -i "s/name/runFHIST.${m}/g" "runFHIST.${m}.job"

    qsub "runFHIST.${m}.job"

done
