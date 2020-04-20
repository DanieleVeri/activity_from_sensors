#!/bin/bash

SPARK_DIR=$1
CLASSIFIER_PARAM="./params/mlp_03"
REV_LABEL="./params/label_03"
ACC_FILE="./data/acc_train.csv"
GYR_FILE="./data/gyr_train.csv"
PREPROCESS_KIND="preprocess_core"
CLASSIFIER_KIND="mlp_classifier"

$SPARK_DIR/bin/spark-submit                                             \
    --deploy-mode client                                                \
    --master local[4]                                                   \
    --driver-memory 2g                                                  \
    --conf spark.ui.port=36000                                          \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
    --class "TrainingApp"                                               \
    target/scala-2.11/activity_from_sensors_2.11-1.0.jar                \
    $ACC_FILE $GYR_FILE $CLASSIFIER_PARAM $REV_LABEL                    \
    $PREPROCESS_KIND $CLASSIFIER_KIND