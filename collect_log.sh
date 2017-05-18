#! /bin/bash

for d in $1/*; do
  if [ -d "$d" ]; then
    echo $d
    cd "$d"
    p=${PWD##*/}
    cp run.log /net/20/kun/output/${p}.log
  fi
done
