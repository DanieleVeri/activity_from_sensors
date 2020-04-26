#!/bin/bash

SPARK_DIR=$1

CLASSIFIER_PARAM="./params/mlp_96"
LABELS="./params/labels"
HOST="localhost"
PORT=7778
PREPROCESS_KIND="core"
MODEL_KIND="mlp"
OUT_FILE="./data/output/processed_stream"
PARTITIONS=100

$SPARK_DIR/bin/spark-submit                                             \
    --deploy-mode client                                                \
    --master local[4]                                                   \
    --supervise                                                         \
    --conf spark.ui.port=36000                                          \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
    --class "StreamingApp"                                              \
    target/scala-2.11/activity_from_sensors_2.11-1.0.jar                \
    $HOST $PORT $CLASSIFIER_PARAM $LABELS                               \
    $PREPROCESS_KIND $MODEL_KIND $OUT_FILE $PARTITIONS
