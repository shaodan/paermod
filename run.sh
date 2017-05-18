#!/bin/bash

#echo $HOSTNAME
echo $(date) ": start!"
./aermod #| while read line; do echo "$(date) : ${line}" ; done
echo $(date) ": end!"

#t=${PWD##*/}
#cp aermod.out /net/20/kun/output/${t}.out
#cp ERRORS.OUT /net/20/kun/output/${t}.error
#cp run.log /net/20/kun/output/${t}.log
