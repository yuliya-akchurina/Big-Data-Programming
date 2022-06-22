# lab2q1

#!/bin/bash
../../start.sh
hadoop dfsadmin -safemode leave

source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /lab2_q1/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /lab2_q1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /mapreduce-test/mapreduce-test-python/project1/shot_logs.csv /lab2_q1/input/
# ../../test-data/kmeans_nba.txt /kmeans/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./lab2_q1.py hdfs://$SPARK_MASTER:9000/lab2_q1/input/
../../stop.sh