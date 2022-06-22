#!/bin/sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q1/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q1/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /Part2Q1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../mapreduce-test-python/project1/shot_logs.csv /Part2Q1/input/
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
-file ../../mapreduce-test-python/Part2Q1/mapper.py -mapper ../../mapreduce-test-python/Part2Q1/mapper.py \
-file ../../mapreduce-test-python/Part2Q1/reducer.py -reducer ../../mapreduce-test-python/Part2Q1/reducer.py \
-input /Part2Q1/input/* -output /Part2Q1/output/
/usr/local/hadoop/bin/hdfs dfs -cat /Part2Q1/output/part-00000
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q1/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q1/output/
../../stop.sh