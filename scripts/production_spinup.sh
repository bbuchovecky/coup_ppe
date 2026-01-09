#!/bin/bash

SDIR="/glade/u/home/bbuchovecky/projects/cpl_ppe_co2/scripts"
CASEROOTDIR="/glade/u/home/bbuchovecky/projects/cpl_ppe_co2/sims"
MEMBERS=(003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)

for m in "${MEMBERS[@]}"; do

    mem="coupPPE.${m}"
    echo "MEMBER = ${mem}"

    wdir="${CASEROOTDIR}/${mem}"
    mkdir -p "$wdir"
    
    cp "${SDIR}/tether_things/"* "${wdir}/"

    sed -i "s/THIS_CASE/${mem}/g" "${wdir}/AD.yml"
    sed -i "s/THIS_CASE/${mem}/g" "${wdir}/PAD.yml"

    echo "${SDIR}/setupAD.sh ${mem}" > "${wdir}/commands.txt"

    cd "$wdir" || exit 1
    qsub -v MEM="$mem" segment001.job

done
