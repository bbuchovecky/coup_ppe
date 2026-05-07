#!/bin/bash


SCRIPT_DIR="/glade/u/home/bbuchovecky/projects/coup_ppe/sst"
DATA_DIR="/glade/campaign/univ/uwas0155/ppe/docn_sst"
TOOL_DIR="/glade/u/home/bbuchovecky/cesm_source/cesm2.1.5/components/cam/tools/icesst"
GRID_PATH="/glade/campaign/collections/gdex/data/d651077/cesmdata/inputdata/atm/cam/coords/fv_1.9x2.5.nc"

ICE_FILENAME="ice_prediddle_1yr_clim_relax.f09_g17.HadOIBl_c241003.LE2_1231_005.nc"
SST_FILENAME="sst_prediddle_1yr_clim_relax.f09_g17.HadOIBl_c241003.LE2_1231_005.nc"
REGRID_BC_FILENAME="sstice_bc_prediddle_1yr_clim_relax.f19_g17.HadOIBl_c241003.LE2_1231_005.nc"
DIDDLE_BC_FILENAME="sstice_bc_diddle_1yr_clim_relax.f19_g17.HadOIBl_c241003.LE2_1231_005"


module purge
module load ncarenv/23.10 intel/2023.2.1 netcdf/4.9.2

export LIB_NETCDF="/glade/u/apps/casper/23.10/spack/opt/spack/netcdf/4.9.2/packages/netcdf-c/4.9.2/oneapi/2023.2.1/esck/lib"
export INC_NETCDF="/glade/u/apps/casper/23.10/spack/opt/spack/netcdf/4.9.2/packages/netcdf-fortran/4.6.1/oneapi/2023.2.1/n73i/include"
export LIB_NETCDF_FORTRAN="/glade/u/apps/casper/23.10/spack/opt/spack/netcdf/4.9.2/packages/netcdf-fortran/4.6.1/oneapi/2023.2.1/n73i/lib"
export LD_LIBRARY_PATH="$LIB_NETCDF_FORTRAN:$LIB_NETCDF:$LD_LIBRARY_PATH"



cd $TOOL_DIR/regrid

echo "compiling regrid..."
gmake clean
gmake FC=ifort
echo "compiled regrid."

echo "regridding..."
REGRID_LOG_FILE="$DATA_DIR/${REGRID_BC_FILENAME%.nc}_regrid.log"
./regrid \
    -i $DATA_DIR/$ICE_FILENAME \
    -s $DATA_DIR/$SST_FILENAME \
    -g $GRID_PATH \
    -o $DATA_DIR/$REGRID_BC_FILENAME 2>&1 | tee -a "$REGRID_LOG_FILE"
regrid_status=${PIPESTATUS[0]}
if [[ $regrid_status -ne 0 ]]; then
    echo "regrid failed with status $regrid_status. See $REGRID_LOG_FILE" >&2
    exit $regrid_status
fi
echo "done regridding."



cd $TOOL_DIR/bcgen

echo "compiling bcgen..."
gmake clean
gmake FC=ifort
echo "compiled bcgen."

echo "diddling..."
LOG_FILE="$DATA_DIR/${DIDDLE_BC_FILENAME}_bcgen.log"
ln -sf $DATA_DIR/$REGRID_BC_FILENAME .
./bcgen \
    -i $DATA_DIR/$REGRID_BC_FILENAME \
    -c $DATA_DIR/${DIDDLE_BC_FILENAME}_clim.nc \
    -t $DATA_DIR/${DIDDLE_BC_FILENAME}_ts.nc < $SCRIPT_DIR/bcgen_namelist 2>&1 | tee -a "$LOG_FILE"
bcgen_status=${PIPESTATUS[0]}
if [[ $bcgen_status -ne 0 ]]; then
    echo "bcgen failed with status $bcgen_status. See $LOG_FILE" >&2
    exit $bcgen_status
fi
echo "done diddling."
