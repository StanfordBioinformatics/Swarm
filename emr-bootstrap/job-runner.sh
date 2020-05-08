#!/bin/bash
set -e -x

#spark-submit --verbose \
#  --conf spark.sql.files.maxPartitionBytes=1073741824 \
#  --conf spark.speculation=false \
#  --conf spark.driver.maxResultSize=8g \
#  partition.py

spark-submit \
  --verbose \
      --conf spark.sql.files.maxPartitionBytes=1073741824 \
      --conf spark.speculation=false \
      --conf spark.dynamicAllocation.enabled=false \
      --conf spark.dynamicAllocation.executorIdleTimeout=600s \
      --conf yarn.scheduler.maximum-allocation-mb=1g \
      --conf spark.executor.memoryOverhead=1g \
      --conf spark.driver.maxResultSize=8g \
      --conf spark.hadoop.fs.s3n.impl="com.amazon.ws.emr.hadoop.fs.EmrFileSystem" \
      --conf spark.hadoop.fs.s3.impl="com.amazon.ws.emr.hadoop.fs.EmrFileSystem" \
      --conf spark.hadoop.fs.s3bfs.impl="org.apache.hadoop.fs.s3.S3FileSystem" \
      --conf spark.hadoop.fs.s3.buffer.dir="/mnt/tmp" \
      --conf spark.hadoop.fs.s3n.endpoint="s3.amazonaws.com" \
      --conf spark.hadoop.fs.s3n.multipart.uploads.enabled="true" \
      --conf spark.hadoop.fs.s3.enableServerSideEncryption="false" \
      --conf spark.hadoop.fs.s3.serverSideEncryptionAlgorithm="AES256" \
      --conf spark.hadoop.fs.s3.consistent="true" \
      --conf spark.hadoop.fs.s3.consistent.retryPolicyType="exponential" \
      --conf spark.hadoop.fs.s3.consistent.retryPeriodSeconds="10" \
      --conf spark.hadoop.fs.s3.consistent.retryCount="5" \
      --conf spark.hadoop.fs.s3.maxRetries="4" \
      --conf spark.hadoop.fs.s3.sleepTimeSeconds="10" \
      --conf spark.hadoop.fs.s3.consistent.throwExceptionOnInconsistency="true" \
      --conf spark.hadoop.fs.s3.consistent.metadata.autoCreate="true" \
      --conf spark.hadoop.fs.s3.consistent.metadata.tableName="EmrFSMetadata" \
      --conf spark.hadoop.fs.s3.consistent.metadata.read.capacity="1000" \
      --conf spark.hadoop.fs.s3.consistent.metadata.write.capacity="1000" \
      --conf spark.hadoop.fs.s3.consistent.fastList="true" \
      --conf spark.hadoop.fs.s3.consistent.fastList.prefetchMetadata="false" \
      --conf spark.hadoop.fs.s3.consistent.notification.CloudWatch="false" \
      --conf spark.hadoop.fs.s3.consistent.notification.SQS="false" \
      --conf spark.hadoop.fs.s3a.retry.limit="10" \
      --conf spark.hadoop.fs.s3a.retry.interval="10" \
      partition.py

#spark-submit \
#  --verbose \
#      --conf spark.sql.shuffle.partitions=40 \
#      --conf spark.sql.files.maxPartitionBytes=1073741824 \
#      --conf spark.speculation=false \
#      --conf yarn.scheduler.maximum-allocation-mb=10g \
#      --conf yarn.nodemanager.resource.memory-mb=14g \
#      --conf spark.yarn.executor.memoryOverhead=6g \
#      --conf spark.driver.maxResultSize=8g \
#      --conf spark.hadoop.fs.s3n.impl="com.amazon.ws.emr.hadoop.fs.EmrFileSystem" \
#      --conf spark.hadoop.fs.s3.impl="com.amazon.ws.emr.hadoop.fs.EmrFileSystem" \
#      --conf spark.hadoop.fs.s3bfs.impl="org.apache.hadoop.fs.s3.S3FileSystem" \
#      --conf spark.hadoop.fs.s3.buffer.dir="/mnt/tmp" \
#      --conf spark.hadoop.fs.s3n.endpoint="s3.amazonaws.com" \
#      --conf spark.hadoop.fs.s3n.multipart.uploads.enabled="true" \
#      --conf spark.hadoop.fs.s3.enableServerSideEncryption="false" \
#      --conf spark.hadoop.fs.s3.serverSideEncryptionAlgorithm="AES256" \
#      --conf spark.hadoop.fs.s3.consistent="true" \
#      --conf spark.hadoop.fs.s3.consistent.retryPolicyType="exponential" \
#      --conf spark.hadoop.fs.s3.consistent.retryPeriodSeconds="10" \
#      --conf spark.hadoop.fs.s3.consistent.retryCount="5" \
#      --conf spark.hadoop.fs.s3.maxRetries="4" \
#      --conf spark.hadoop.fs.s3.sleepTimeSeconds="10" \
#      --conf spark.hadoop.fs.s3.consistent.throwExceptionOnInconsistency="true" \
#      --conf spark.hadoop.fs.s3.consistent.metadata.autoCreate="true" \
#      --conf spark.hadoop.fs.s3.consistent.metadata.tableName="EmrFSMetadata" \
#      --conf spark.hadoop.fs.s3.consistent.metadata.read.capacity="1000" \
#      --conf spark.hadoop.fs.s3.consistent.metadata.write.capacity="1000" \
#      --conf spark.hadoop.fs.s3.consistent.fastList="true" \
#      --conf spark.hadoop.fs.s3.consistent.fastList.prefetchMetadata="false" \
#      --conf spark.hadoop.fs.s3.consistent.notification.CloudWatch="false" \
#      --conf spark.hadoop.fs.s3.consistent.notification.SQS="false" \
#      partition.py
