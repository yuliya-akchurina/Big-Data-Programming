#!/bin/bash
../../start.sh
hdfs dfsadmin -safemode leave

source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /question3/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /question3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /spark-examples/data/question3and4/adult_data.csv /question3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /spark-examples/data/question3and4/adult_test.csv /question3/input/

/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./q3_income_prediction.py hdfs://$SPARK_MASTER:9000/question3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /question3/input/
../../stop.sh