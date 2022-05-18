#!/bin/sh

nohup sh -c /spark-bootstrap.sh &

HOSTNAME=$(cat /proc/sys/kernel/hostname)
python /backoff -d "YARN ResourceManager" -p 8032 $HOSTNAME || { exit 1; }
python /backoff -d "YARN ResourceManager webapp UI" -p 8088 $HOSTNAME || { exit 1; }
python /backoff -d "YARN NodeManager webapp UI" -p 8042 $HOSTNAME || { exit 1; }
python /backoff -d "Spark HistoryServer web UI port" -p 18080 $HOSTNAME || { exit 1; }
python /backoff -d "Spark master" -p 7077 $HOSTNAME || { exit 1; }
python /backoff -d "Spark web UI port" -p 8080 $HOSTNAME || { exit 1; }

RANGE=""
if [ ! -z "$RANGE_FILTER_COLUMN" ]; then
    RANGE="--range $RANGE_FILTER_COLUMN"
    if [ ! -z "$RANGE_FILTER_LOWER" ]; then
        RANGE="$RANGE --lower $RANGE_FILTER_LOWER"
    fi
    if [ ! -z "$RANGE_FILTER_UPPER" ]; then
        RANGE="$RANGE --upper $RANGE_FILTER_UPPER"
    fi
    if [ ! -z "$RANGE_FILTER_FORCE" ] && [ "$RANGE_FILTER_FORCE" = true ]; then
        RANGE="$RANGE --force-range"
    fi
fi

# --master local[*]\
# --driver-memory ${DRIVER_MEMORY:-8}g\
/opt/spark/bin/spark-submit\
 --master yarn\
 --deploy-mode cluster\
 --num-executors ${NUM_EXECUTORS:-2}\
 --executor-cores ${EXECUTOR_CORES:-1}\
 --executor-memory ${EXECUTOR_MEMORY:-4g}\
 --py-files /data/dependencies.zip\
 /scripts/differ.py row\
 --drop $COLS_TO_DROP\
 --output $OUTPUT_PATH\
 $RANGE\
 -- $DIFFER_SCHEMA $LEFT_DATA_SOURCE $RIGHT_DATA_SOURCE\

# Block until we signal exit.
#trap 'exit 0' TERM
#while true; do sleep 0.5; done
