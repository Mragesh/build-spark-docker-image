#!/bin/bash

export SPARK_VERSION=3.0.0
export HADOOP_VERSION=3.2
export DOCKER_IMAGE_TAG=spark3.0.0
export DOCKER_REPO=mragesh

# download and extract spark
curl http://mirror.metrocast.net/apache/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz | tar xvz -C . 

# rename the directory
mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION spark

# copy custom dependencies and changes for pyspark
cp -r --parents custom/spark/* spark/

# build docker image
./spark/bin/docker-image-tool.sh -p spark/kubernetes/dockerfiles/spark/bindings/python/Dockerfile -r $DOCKER_REPO -t $DOCKER_IMAGE_TAG build

# push docker image
./spark/bin/docker-image-tool.sh -r $DOCKER_REPO -t $DOCKER_IMAGE_TAG push
