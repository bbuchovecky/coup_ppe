#!/bin/bash

# MEMBERS=(000)
MEMBERS=(000 001 002 003 004 005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)

DO="FHIST"

if [ "$DO" == "FHIST" ]; then
    echo "Transferring FHIST"

    for MEM in "${MEMBERS[@]}"; do

        mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}

        rsync -a --info=progress2 \
            --relative \
            --exclude='*/atm/hist/***' \
            --exclude='*/lnd/hist/***' \
            /glade/derecho/scratch/bbuchovecky/archive/./f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.${MEM}/ \
            /glade/campaign/univ/uwas0155/ppe/historical/coupled_simulations
    done


elif [ "$DO" == "IHIST" ]; then
    echo "Transferring IHIST"

    for MEM in "${MEMBERS[@]}"; do

        mkdir -p /glade/campaign/univ/uwas0155/ppe/historical/IHistClm50Bgc.CPLHIST.historical.coupPPE.${MEM}

        rsync -a --info=progress2 \
            --relative \
            /glade/derecho/scratch/bbuchovecky/archive/./IHistClm50Bgc.CPLHIST.historical.coupPPE.${MEM}.IHIST/ \
            /glade/campaign/univ/uwas0155/ppe/historical/spinup_simulations/
    done


else
    echo "Unknown option: $DO"


fi

