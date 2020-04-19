#!/bin/bash

SPARK_DIR="/home/dan/spark-2.4.5-bin-hadoop2.7"
CLASSIFIER_PARAM="./params/mlp_00"
ACC_FILE="./data/acc_train.csv"
GYR_FILE="./data/gyr_train.csv"

$SPARK_DIR/bin/spark-submit                                             \
    --deploy-mode client                                                \
    --master local[4]                                                   \
    --driver-memory 2g                                                  \
    --supervise                                                         \
    --conf spark.ui.port=36000                                          \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
    --class "TrainingApp"                                               \
    target/scala-2.11/activity_from_sensors_2.11-1.0.jar                \
    $ACC_FILE $GYR_FILE $CLASSIFIER_PARAM