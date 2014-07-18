#!/bin/bash
#$wh=$PWD/$1
cd ~/Work/repos/github/unsas/
#echo $wh
java -cp lib/*:bin com.dcc.unsas.main.unsas $1
