#!/bin/bash
#$wh=$PWD/$1
cd ~/Work/repos/github/unsas/
#echo $wh
java -cp libs/*:bin com.dcc.unsas.main.unsas $1
python3 python/cleanCSV.py "$1/csv"

