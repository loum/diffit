#!/bin/sh

# To overcome AWS Batch hostname problems.
#
# HOSTNAME is bash specific.
HOSTNAME=$(cat /proc/sys/kernel/hostname)
LOCAL_IP=$(hostname -I | awk '{print $1}')

cp /etc/hosts /etc/hosts.bak
grep -Fq "$HOSTNAME" /etc/hosts.bak || echo "$LOCAL_IP $HOSTNAME" >> /etc/hosts.bak
sed "s/$HOSTNAME/127.0.0.1 $HOSTNAME/" /etc/hosts.bak > /etc/hosts

exec gosu hdfs /start.sh
