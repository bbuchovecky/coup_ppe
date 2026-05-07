#!/bin/bash

SDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts"
MEMBERS=(000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)

for m in "${MEMBERS[@]}"; do

    mem="coupPPE.${m}"
    echo "MEMBER = ${mem}"

    cd $SDIR

    cp run.job "runOFFLSSP370.${m}.job"
    echo -e "\n./runOFFLSSP370.sh ${mem}" >> "runOFFLSSP370.${m}.job"
    sed -i "s/name/runOFFLSSP370.${m}/g" "runOFFLSSP370.${m}.job"

    qsub "runOFFLSSP370.${m}.job"

done
