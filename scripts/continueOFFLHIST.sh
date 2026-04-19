#!/bin/bash

SIMDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims"
ARCHDIR="/glade/derecho/scratch/bbuchovecky/archive"
CASE="i.e21.CPLHIST_BGC.f19_f19_mg17.historical"
MEMBERS=(005 006 007 008 009 010 011 012 013 014 015 016 017 018 019 020 021 022 023 024 025 026 027 028)
# MEMBERS=(004)

for m in "${MEMBERS[@]}"; do

    mem="coupPPqR = ${mem}"

    rundir="/glade/derecho/scratch/bbuchovecky/${CASE}.${mem}/run"
    mkdir $rundir/rest_pre2000
    mv $rundir/*.r*.* $rundir/rest_pre2000/
    mv $rundir/rest_pre2000/*.r*.2000* $rundir/

    cd $SIMDIR/$mem/$CASE.$mem
    pwd

    ./xmlchange STOP_OPTION="nyears"
    ./xmlchange STOP_N=15
    ./xmlchange REST_OPTION="nyears"
    ./xmlchange REST_N=5
    ./xmlchange CONTINUE_RUN=TRUE

    ./xmlchange JOB_WALLCLOCK_TIME="01:45:00" --subgroup case.run

    ./case.submit

done
