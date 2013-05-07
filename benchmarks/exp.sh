#!/bin/bash

# An example Hadoop benchmarking script (launched by the "spot-hadoop" script). 
#
# This script assumes the following steps are done already:
# step 1 reserve nodes in an interactive manner for ample amounts of wall time.
# step 2 ./spot-hadoop environment
# step 3 ./spot-hadoop hadoop 
# step 4 cd MPILAUNCH_HOME/bin

WORKLOAD=$1
EXP_SET=$2
# control parameters
SLAVE_CNT=$3
TERAGEN_DATASET_SIZE=100000000 # 1 is 100 byte.
TERAGEN_OUT=teragen_10G
TERASORT_OUT=terasort_10G

MPILAUNCH_HOME=/tmp/proj/med002/s52c3/MPILaunch
CONFIG_PATH=$MPILAUNCH_HOME/hadoop/conf/${HOSTNAME}
cd $MPILAUNCH_HOME/hadoop/hadoop-1.1.1
RESULT_PATH_TEST=$MPILAUNCH_HOME/${WORKLOAD}/exp_$EXP_SET

case ${WORKLOAD} in
	teragen*) 
		bin/hadoop --config $CONFIG_PATH fs -rmr ${TERAGEN_OUT}
		/usr/bin/time -f "%e sec" -o ${RESULT_PATH}/teragen.timelog --append bin/hadoop --config $CONFIG_PATH jar hadoop*example* teragen -Dmapred.map.tasks=${SLAVE_CNT} ${TERAGEN_DATASET_SIZE} ${TERAGEN_OUT}
	;;
	terasort*)
		bin/hadoop --config $CONFIG_PATH fs -rmr ${TERASORT_OUT}
		/usr/bin/time -f "%e sec" -o ${RESULT_PATH}/terasort.timelog --append bin/hadoop --config $CONFIG_PATH jar hadoop*example* terasort -Dmapred.map.tasks=${SLAVE_CNT} -Dmapred.reduce.tasks=${SLAVE_CNT} ${TERAGEN_OUT} ${TERASORT_OUT}
	;;
esac

