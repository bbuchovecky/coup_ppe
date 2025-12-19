#!/bin/bash

START=925300
END=925314
QUEUE="casper-pbs"

for ((i=START; i<=END; i++)); do
    qdel $i.$QUEUE
done
