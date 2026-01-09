#!/bin/tcsh
#PBS -N proc_cplhist_ystr
#PBS -q develop
#PBS -l walltime=5:00:00
#PBS -A UWAS0155
#PBS -j oe
#PBS -k eod
#PBS -l select=1:ncpus=1
###################################################################################################
# This script takes the raw coupler output and formats it so that the coupler output can be used to
# force an offline land-only simulation.

module load nco

set CASENAME=f.e21.FHIST_BGC.f19_f19_mg17.historical.coupPPE.cplhist
set FILEPATH=/glade/derecho/scratch/bbuchovecky/archive/$CASENAME

set cpl_prefixes=( ha2x1hi ha2x3h ha2x1h ha2x1d )
set months=( 01 02 03 04 05 06 07 08 09 10 11 12 )
set years=(ystr)

set FILEPATTERN=$FILEPATH/cpl/hist/$CASENAME.cpl
set FILEPATTERN_PROC=$FILEPATH/cpl/proc/$CASENAME.cpl

echo $FILEPATTERN

foreach val ($cpl_prefixes)
   foreach year ($years)
      echo $year
      foreach month ($months)
         echo $FILEPATTERN.$val.$year-$month.nc
         ncrcat $FILEPATTERN.$val.$year-$month-* $FILEPATTERN_PROC.$val.$year-$month.nc
         
         # Add doma_lat and doma_lon etc variables... probably just to month 1, but give it to all of them to play it safe and see if that fixes the problem
         ncks -A -v doma_lat,doma_lon,doma_area,doma_area,doma_aream,doma_mask,doma_frac $FILEPATTERN.ha2x1h.1850-01-01.nc $FILEPATTERN_PROC.$val.$year-$month.nc
      end
   end
end
