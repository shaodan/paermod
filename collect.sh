#! /bin/bash

#for d in $1/*; do
for d in $1; do
  if [ -d "$d" ]; then
    cd "$d"
    # ls
    p=${PWD##*/}
    # echo $p
    cp aermod.out /net/20/kun/output/${p}.out
    cp ERRORS.OUT /net/20/kun/output/${p}.error
    cp nohup.out /net/20/kun/output/${p}.log
  fi
done
