#!/bin/sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /tl1/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /tl1/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /tl1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../mapreduce-test-data/access.log /tl1/input/
/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
-file ../../mapreduce-test-python/tl1/mapper.py -mapper ../../mapreduce-test-python/tl1/mapper.py \
-file ../../mapreduce-test-python/tl1/reducer.py -reducer ../../mapreduce-test-python/tl1/reducer.py \
-input /tl1/input/* -output /tl1/output/
/usr/local/hadoop/bin/hdfs dfs -cat /tl1/output/part-00000
/usr/local/hadoop/bin/hdfs dfs -rm -r /tl1/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /tl1/output/
../../stop.sh