#!/bin/bash

SRCDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/hist/data/cesm2.1.5"
DESTDIR="/glade/u/home/bbuchovecky/projects/coup_ppe/pert"

cp -r $SRCDIR/* $DESTDIR/
cd $DESTDIR

for file in nlmods/*-hist*; do
    if [ -f "$file" ]; then
        newname="${file//-hist/}"
        mv "$file" "$newname"
    fi
done

for file in paramfiles/*-hist*; do
    if [ -f "$file" ]; then
        newname="${file//-hist/}"
        mv "$file" "$newname"
    fi
done

for dir in srcmods/perturbed/*-hist*; do
    if [ -d "$dir" ]; then
        newname="${dir//-hist/}"
        mv "$dir" "$newname"
    fi
done
