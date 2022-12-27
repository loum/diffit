#!/bin/sh

nohup sh -c /spark-bootstrap.sh &

HOSTNAME=$(cat /proc/sys/kernel/hostname)
python /backoff -d "YARN ResourceManager" -p 8032 $HOSTNAME || { exit 1; }
python /backoff -d "YARN ResourceManager webapp UI" -p 8088 $HOSTNAME || { exit 1; }
python /backoff -d "YARN NodeManager webapp UI" -p 8042 $HOSTNAME || { exit 1; }
python /backoff -d "Spark HistoryServer web UI port" -p 18080 $HOSTNAME || { exit 1; }
python /backoff -d "Spark master" -p 7077 $HOSTNAME || { exit 1; }
python /backoff -d "Spark web UI port" -p 8080 $HOSTNAME || { exit 1; }


#export PYSPARK_PYTHON=python
#/opt/spark/bin/spark-submit\
# --master local[*]\
# --driver-memory ${DRIVER_MEMORY:-8}g\
# --py-files /tmp/dependencies.zip\
# ./.local/lib/python3.*/site-packages/diffit/__main__.py "$@"

export PYSPARK_PYTHON=python
/opt/spark/bin/spark-submit\
 --master yarn\
 --deploy-mode cluster\
 --num-executors ${NUM_EXECUTORS:-2}\
 --executor-cores ${EXECUTOR_CORES:-1}\
 --executor-memory ${EXECUTOR_MEMORY:-4g}\
 --py-files /tmp/dependencies.zip\
 ./.local/lib/python3.*/site-packages/diffit/__main__.py "$@"

# Block until we signal exit.
#trap 'exit 0' TERM
#while true; do sleep 0.5; done
