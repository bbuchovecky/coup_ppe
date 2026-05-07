#!/bin/bash

set -u  # exit if undefined variable is used

MEM=$1
CASENAME="f.e21.FSSP370_BGC.f19_f19_mg17.ssp370.${MEM}"
PROJECT=UWAS0155

WDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sims/${MEM}"
NAMELISTS="/glade/u/home/bbuchovecky/projects/coup_ppe/scripts/namelists"
NAMELISTMODS="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/nlmods"
PARAMFILES="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/paramfiles"
SOURCEMODS="/glade/u/home/bbuchovecky/projects/coup_ppe/pert/srcmods/perturbed"

COMPSET=SSP370_CAM60_CLM50%BGC-CROP_CICE%PRES_DOCN%DOM_MOSART_CISM2%NOEVOLVE_SWAV
GRID=f19_f19_mg17
CESMROOT="/glade/u/home/bbuchovecky/cesm_source/cesm2.1.5"

SSTICE="/glade/campaign/univ/uwas0155/ppe/docn_sst/sstice_bc_diddle_1yr_clim_relax.f19_g17.HadOIBl_c241003.LE2_1231_005_ts.nc"

REFCASE="f.e21.FHIST_BGC.f19_f19_mg17.historical.${MEM}"
REFDIR="/glade/derecho/scratch/bbuchovecky/archive/${REFCASE}/rest/2015-01-01-00000"
LINIT="${REFCASE}.clm2.r.2015-01-01-00000.nc"
REFDATE="2015-01-01"


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
finidat="${REFDIR}/${LINIT}"
echo -e "\nfinidat='${finidat}'" >> user_nl_clm


# point to custom prescribed SST file
./xmlchange DOCN_MODE=prescribed
./xmlchange SSTICE_DATA_FILENAME=$SSTICE
./xmlchange SSTICE_YEAR_START=2015
./xmlchange SSTICE_YEAR_END=2100
./xmlchange SSTICE_YEAR_ALIGN=2015


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
./xmlchange RESUBMIT=14
./xmlchange JOB_WALLCLOCK_TIME=08:00:00 --subgroup case.run


./case.build
./case.submit


mv "${FILENAME}.${MEM}.log" .
