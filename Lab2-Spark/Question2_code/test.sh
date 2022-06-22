#!/bin/bash
../../start.sh
hdfs dfsadmin -safemode leave
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /black_car/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /black_car/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /mapreduce-test/mapreduce-test-python/project1/Parking_Violations_Issued_-_Fiscal_Year_2021.csv /black_car/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./black_car.py hdfs://$SPARK_MASTER:9000/black_car/input/
../../stop.sh