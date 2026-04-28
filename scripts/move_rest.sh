#!/bin/bash

YEARS=(1950 1955 1960 1965 1970 1975 1980 1985 1990 1995 2000)
MEMBERS=(007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)

for m in "${MEMBERS[@]}"; do

    echo $m

    SRCDIR=$(ls -d /glade/derecho/scratch/bbuchovecky/i.e21.CPLHIST_BGC.f19_f19_mg17.historical.coupPPE.${m}/run/rest_pre*)
    DESTDIR="/glade/derecho/scratch/bbuchovecky/archive/i.e21.CPLHIST_BGC.f19_f19_mg17.historical.coupPPE.${m}/rest"

    for yr in "${YEARS[@]}"; do

        echo $DESTDIR/$yr-01-01-00000
        ls $SRCDIR/*$yr*
        mkdir $DESTDIR/$yr-01-01-00000
        mv $SRCDIR/*$yr* $DESTDIR/$yr-01-01-00000

    done

    # rmdir $SRCDIR

done