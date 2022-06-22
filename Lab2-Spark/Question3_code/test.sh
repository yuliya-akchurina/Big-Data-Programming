#!/bin/bash
../../start.sh
hdfs dfsadmin -safemode leave
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /lab2_q3/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /lab2_q3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /mapreduce-test/mapreduce-test-python/project1/Parking_Violations_Issued_-_Fiscal_Year_2021.csv /lab2_q3/input/

/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./lab2_q3.py hdfs://$SPARK_MASTER:9000/lab2_q3/input/ –-conf spark.default.parallelism=3
../../stop.sh