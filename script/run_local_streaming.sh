#!/bin/bash

SPARK_DIR="/home/dan/spark-2.4.5-bin-hadoop2.7"
CLASSIFIER_PARAM="./params/mlp_03"
REV_LABEL="./params/label_03"

$SPARK_DIR/bin/spark-submit                                             \
    --deploy-mode client                                                \
    --master local[4]                                                   \
    --supervise                                                         \
    --conf spark.ui.port=36000                                          \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
    --class "StreamingApp"                                              \
    target/scala-2.11/activity_from_sensors_2.11-1.0.jar                \
    localhost 7777 $CLASSIFIER_PARAM $REV_LABEL