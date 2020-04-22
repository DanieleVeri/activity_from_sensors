#!/bin/bash

timestamp=$(date +%s)
SPARK_DIR="/Users/buio/spark-2.4.5-bin-hadoop2.7"
CLASSIFIER_PARAM="./params/mlp_22_04"
REV_LABEL="./params/label_22_01"
ACC_FILE="./data/acc_train.csv"
GYR_FILE="./data/gyr_train.csv"
PREPROCESS_KIND="preprocess_sql"
CLASSIFIER_KIND="mlp_classifier"
PARTITIONS=100


$SPARK_DIR/bin/spark-submit                                             \
    --deploy-mode client                                                \
    --master local[4]                                                   \
    --driver-memory 12g                                                 \
    --supervise                                                         \
    --conf spark.ui.port=36000                                          \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
    --class "TrainingApp"                                               \
    target/scala-2.11/activity_from_sensors_2.11-1.0.jar                \
    $ACC_FILE $GYR_FILE $CLASSIFIER_PARAM $REV_LABEL                    \
    $PREPROCESS_KIND $CLASSIFIER_KIND $PARTITIONS