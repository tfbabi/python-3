#!/bin/bash
PATH=$PATH:$HOME/bin

export PATH
export ORACLE_SID=dhouse
export ORACLE_BASE=/u01/app/oracle
export ORACLE_HOME=$ORACLE_BASE/product/11.2.0/dbhome_1
export PATH=$PATH:$HOME/bin:$ORACLE_HOME/bin
export LD_LIBRARY_PATH=$ORACLE_HOME/lib
export LANG=en_US.UTF-8
export NLS_LANG=AMERICAN_AMERICA.ZHS16GBK

cd /report
/usr/bin/python orders.py
