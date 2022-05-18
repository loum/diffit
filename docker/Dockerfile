ARG UBUNTU_BASE_IMAGE
ARG SPARK_PSEUDO_BASE_IMAGE

FROM loum/spark-pseudo:$SPARK_PSEUDO_BASE_IMAGE

USER root
RUN apt-get update && apt-get install -y --no-install-recommends\
 gosu\
 && rm -rf /var/lib/apt/lists/*

COPY docker/scripts/differ-bootstrap.sh /differ-bootstrap.sh
COPY docker/scripts/start.sh /start.sh

USER hdfs
WORKDIR /home/hdfs

RUN python -m pip install --user --upgrade pip setuptools
RUN python -m pip install --user\
 --no-cache-dir\
 backoff

COPY docker/scripts/backoff /backoff
COPY src/bin/diff.py /scripts/diff.py
COPY docker/files/python/dependencies.zip /data/dependencies.zip

USER root
ENTRYPOINT [ "/differ-bootstrap.sh" ]
