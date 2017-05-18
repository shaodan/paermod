#!/bin/bash


p=${PWD##*/}
cp aermod.out /net/20/kun/output/${p}.out
cp ERRORS.OUT /net/20/kun/output/${p}.error
cp nohup.out /net/20/kun/output/${p}.log
