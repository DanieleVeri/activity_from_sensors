#!/bin/bash

spark-submit                                                            \
    --master yarn                                                       \
    --deploy-mode cluster                                               \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
    --class TrainingApp                                                 \
    s3://spark.jars/activity_from_sensors_2.11-1.0.jar                  \
    s3://spark.jars/acc_train.csv                                       \
    s3://spark.jars/gyr_train.csv                                       \
    s3://spark.jars/params/mlp_00                                       \
    s3://spark.jars/params/lbl_00                                       \
    preprocess_sql                                                      \
    mlp_classifier
