#!/bin/bash
../../start.sh
hdfs dfsadmin -safemode leave

source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /question1/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /question1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /spark-examples/data/question1/train.csv /question1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /spark-examples/data/question1/test.csv /question1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /spark-examples/data/question1/test_labels.csv /question1/input/

/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./q1_toxic_comments.py hdfs://$SPARK_MASTER:9000/question1/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /question1/input/
../../stop.sh