# Scaling out Diffit

Diffit can be scaled out to perform symmetric difference analysis across massive data sets. This
is a natural extension as Diffit is built onto of the [Apache Spark compute
framework](https://spark.apache.org/)

## Diffit Container Image
!!! note
    Diffit image container to be provided via [Docker Hub](https://hub.docker.com/) shortly.

### Manual Build of the Diffit Container Image
The Diffit image container is built on top of this [Spark base image](https://github.com/loum/spark-pseudo)
that runs over YARN on Pseudo Distributed Hadoop. The goal here is to establish a pathway into a
larger Spark cluster.

The container image build process is started with:
``` sh title="Starting the Diffit container image build process"
make diffit-image-build
```

On successful completion, you can search for the newly created container image with:
``` sh title="Container image search"
make image-search
```

### Running jobs on the Diffit Container Image
Refer to the [Diffit `Makefile`](https://github.com/loum/diffit/blob/main/Makefile) for the
targets used in this section.

!!! note
    All Makester Docker subsystem targets can be listed with:
    ``` sh
    make docker-help`
    ```

Here is how the [`diffit row csv` example](../utilities/diffit/row/#example-csv-data-sources)
would be run in the container image:
``` sh title="Running diffit compute on container image"
CMD="row csv --output file:///tmp/data/out --csv-separator ';' /tmp/schema/Dummy.json file:///tmp/data/left file:///tmp/data/right" make container-run
```

This is the actual `docker` command:
``` sh title="Diffit container run make target"
make container-run
```

``` sh
/usr/bin/docker run --rm -d\
 --platform linux/amd64\
 --publish 8032:8032\
 --publish 7077:7077\
 --publish 8080:8080\
 --publish 8088:8088\
 --publish 8042:8042\
 --publish 18080:18080\
 --env CORE_SITE__HADOOP_TMP_DIR=/tmp\
 --env HDFS_SITE__DFS_REPLICATION=1\
 --env YARN_SITE__YARN_NODEMANAGER_RESOURCE_DETECT_HARDWARE_CAPABILITIES=true\
 --env YARN_SITE__YARN_LOG_AGGREGATION_ENABLE=true\
 --env OUTPUT_PATH=file:///tmp/data/out\
 --env NUM_EXECUTORS=6\
 --env EXECUTOR_CORES=2\
 --env EXECUTOR_MEMORY=768m\
 --volume $HOME/diffit/docker/files/python:/data\
 --volume $HOME/diffit/docker/files/schema:/tmp/schema\
 --volume $HOME/diffit/docker/files/data:/tmp/data\
 --hostname diffit-spark\
 --name diffit-spark\
 diffit:0.1.2a4-1\
 row csv --output file:///tmp/data/out --csv-separator ';' /tmp/schema/Dummy.json file:///tmp/data/left file:///tmp/data/right
```

!!! note
    Container image execution is ephemeral. The `--output` switch ensures that the
    results persist on the file system via the mount provided by
    `--volume $HOME/diffit/docker/files/data:/tmp/data`

Progress logs of the container execution can be monitored as follows:
``` sh title="diffit on Spark pseudo-distributed logging"
make container-logs
```
