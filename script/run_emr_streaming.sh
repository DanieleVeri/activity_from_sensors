#!/bin/bash

spark-submit                                                            \
    --master yarn                                                       \
    --deploy-mode cluster                                               \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer  \
    --class StreamingApp                                                \
    s3://spark.jars/activity_from_sensors_2.11-1.0.jar                  \
    *eth0 ip*                                                           \
    7777                                                                \
    s3://spark.jars/params/mlp_00                                       \
    s3://spark.jars/params/lbl_00                                       \
    preprocess_sql                                                      \
    mlp_model                                                           \
    s3://spark.jars/out_stream
