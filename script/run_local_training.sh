#!/bin/bash

SPARK_DIR=$1

CLASSIFIER_PARAM="./params/mlp_96"
LABELS="./params/labels"
ACC_FILE="./data/acc_train.csv"
GYR_FILE="./data/gyr_train.csv"
PREPROCESS_KIND="core"
CLASSIFIER_KIND="mlp"
PARTITIONS=100

$SPARK_DIR/bin/spark-submit                                             \
    --deploy-mode client                                                \
    --master local[4]                                                   \
    --driver-memory 10g                                                  \
    --supervise                                                         \
    --conf spark.ui.port=36000                                          \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
    --class "TrainingApp"                                               \
    target/scala-2.11/activity_from_sensors_2.11-1.0.jar                \
    $ACC_FILE $GYR_FILE $CLASSIFIER_PARAM $LABELS                       \
    $PREPROCESS_KIND $CLASSIFIER_KIND $PARTITIONS