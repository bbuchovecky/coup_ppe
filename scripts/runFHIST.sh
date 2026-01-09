#!/bin/bash

MEM=$1
CASENAME="f.e21.FHIST_BGC.f19_f19_mg17.historical.${MEM}"
PROJECT=UWAS0155

WDIR="/glade/u/home/bbuchovecky/projects/cpl_ppe_co2/sims/${MEM}"
NAMELISTS="/glade/u/home/bbuchovecky/projects/cpl_ppe_co2/scripts/namelists"
PARAMFILES="/glade/u/home/bbuchovecky/projects/cpl_ppe_co2/pert/paramfiles"
SOURCEMODS="/glade/u/home/bbuchovecky/projects/cpl_ppe_co2/pert/srcmods/perturbed"

COMPSET=HIST_CAM60_CLM50%BGC-CROP_CICE%PRES_DOCN%DOM_MOSART_CISM2%NOEVOLVE_SWAV
GRID=f19_f19_mg17
CESMROOT="/glade/u/home/bbuchovecky/cesm_source/cesm2.1.5"

REFCASE="f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.cplhist"
REFDIR="/glade/derecho/scratch/bbuchovecky/archive/${REFCASE}/rest/1950-01-01-00000"
REFDATE="1950-01-01"

LINITCASE="IHistClm50Bgc.CPLHIST.historical.${MEM}.IHIST"
HDIR="/glade/derecho/scratch/bbuchovecky/archive/${LINITCASE}/rest/1950-01-01-00000"
LINIT="${LINITCASE}.clm2.r.1950-01-01-00000.nc"


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


# redirect output from this script to a log file
FILENAME="$(pwd)/$(basename "${0%.*}")"
exec > >(tee -a "${FILENAME}.${MEM}.log") 2>&1
echo $FILENAME
echo $USER
date +%y-%m-%dT%H:%M:%S


CASEROOTBASE=$WDIR
caseroot="${CASEROOTBASE}/${CASENAME}"
echo "caseroot: ${caseroot}"


cd "${CESMROOT}/cime/scripts"
./create_newcase --case $caseroot --compset $COMPSET --res $GRID --project $PROJECT --mach derecho --run-unsupported
cd $caseroot
./case.setup


# apply sourcemods for history fields
cp "${SOURCEMODS}/all/clm/"* ./SourceMods/src.clm/
cp "${SOURCEMODS}/all/cam/"* ./SourceMods/src.cam/


# apply sourcemods for parameter change
cp "${SOURCEMODS}/${MEM}/"* ./SourceMods/src.clm/


# apply namelist mods for history fields
cp "${NAMELISTS}/FHIST/"* ./


# apply namelist mods for parameter change
cat "${NAMELISTMODS}/${MEM}.txt" >> user_nl_clm


# apply parameter file for parameter change
echo -e "\nparamfile = \"${PARAMFILES}/${MEM}.nc\"" >> user_nl_clm


# apply land initial conditions from spinup
finidat="${HDIR}/${LINIT}"
echo -e "\nfinidat='${finidat}'" >> user_nl_clm


./xmlchange RUN_TYPE=hybrid
./xmlchange PROJECT=$PROJECT
./xmlchange JOB_PRIORITY="regular"
./xmlchange RUN_STARTDATE=$REFDATE
./xmlchange RUN_REFCASE=$REFCASE
./xmlchange RUN_REFDIR=$REFDIR
./xmlchange RUN_REFDATE=$REFDATE
./xmlchange GET_REFCASE="True"

./xmlchange STOP_OPTION="nyears"
./xmlchange STOP_N=5
./xmlchange REST_OPTION="nyears"
./xmlchange REST_N=5
./xmlchange RESUBMIT=12
./xmlchange JOB_WALLCLOCK_TIME=08:00:00 --subgroup case.run


./case.build
./case.submit


mv "${FILENAME}.${MEM}.log" .
