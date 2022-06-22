#!/bin/bash
../../start.sh
hdfs dfsadmin -safemode leave

source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /question2/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /question2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /spark-examples/data/question2/framingham.csv /question2/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./q2_heart_disease.py hdfs://$SPARK_MASTER:9000/question2/input/
../../stop.sh