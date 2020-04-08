#!/bin/bash

aws emr create-cluster \
    --applications Name=Ganglia Name=Spark Name=Zeppelin \
    --ec2-attributes '{"KeyName":"spark_cluster","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-3a51035d","EmrManagedSlaveSecurityGroup":"sg-01a43edc21b1bee3b","EmrManagedMasterSecurityGroup":"sg-015b119816a771e17"}' \
    --service-role EMR_DefaultRole \
    --enable-debugging \
    --release-label emr-5.29.0 \
    --log-uri 's3n://aws-logs-651507584207-us-east-1/elasticmapreduce/' \
    --name 'spark_test' \
    --instance-groups '[{"InstanceCount":3,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"}]' \
    --configurations '[{"Classification":"spark","Properties":{}}]' \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
    --region us-east-1