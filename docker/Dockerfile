# syntax=docker/dockerfile:1.4

ARG SPARK_PSEUDO_BASE_IMAGE

FROM loum/spark-pseudo:$SPARK_PSEUDO_BASE_IMAGE

COPY docker/scripts/diffit-bootstrap.sh /diffit-bootstrap.sh

ARG RELEASE_VERSION
WORKDIR /home/hdfs/dist
COPY dist/diffit-$RELEASE_VERSION-py3-none-any.whl diffit-$RELEASE_VERSION-py3-none-any.whl
COPY docker/files/python/dependencies.zip /tmp/dependencies.zip

WORKDIR /home/hdfs

RUN python -m pip install --user --upgrade pip setuptools
RUN python -m pip install --user\
 --no-cache-dir\
 backoff\
 dist/diffit-$RELEASE_VERSION-py3-none-any.whl &&\
 find .local/lib/python*/site-packages/ -depth\
   \(\
     \( -type d -a \( -name test -o -name tests -o -name idle_test \) \) \
     -o \
     \( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
  \) -exec rm -rf '{}' +;

COPY docker/scripts/backoff /backoff

USER hdfs
ENTRYPOINT [ "/diffit-bootstrap.sh" ]
