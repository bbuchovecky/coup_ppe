# Diddling SST and SEAICE

- I edited the regrid and bcgen Fortran files in `/glade/u/home/bbuchovecky/cesm_source/cesm2.1.5/components/cam/tools/icesst` so that I could compile them on Casper.
- The `ncarenv/23.10` module does not exist on Derecho.

```Bash
cd /glade/u/home/bbuchovecky/projects/coup_ppe/sst
./create_sst_bc_ssp370.py

# Adjust variable names in diddle.sh so that they match the files produced by create_sst_bc_ssp370.py
./diddle.sh
```
