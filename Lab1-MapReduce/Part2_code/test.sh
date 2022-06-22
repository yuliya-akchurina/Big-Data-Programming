#!/bin/bash

while :; do
  read -p "Enter an integer for the hour of the day from 1 to 24: " number
  [[ $number =~ ^[0-9]+$ ]] || { echo "Enter a valid number"; continue; }
  if ((number >= 1 && number <= 24)); then   
	../../start.sh
	/usr/local/hadoop/bin/hdfs dfs -rm -r /p2tl1/input/
	/usr/local/hadoop/bin/hdfs dfs -rm -r /p2tl1/output/
	/usr/local/hadoop/bin/hdfs dfs -mkdir -p /p2tl1/input/
	/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../mapreduce-test-data/access.log /p2tl1/input/
	/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
	-file ../../mapreduce-test-python/p2tl1/mapper.py -mapper ../../mapreduce-test-python/p2tl1/mapper.py \
	-file ../../mapreduce-test-python/p2tl1/reducer.py -reducer "../../mapreduce-test-python/p2tl1/reducer.py number" \
	-input /p2tl1/input/* -output /p2tl1/output/
	/usr/local/hadoop/bin/hdfs dfs -cat /p2tl1/output/part-00000
	/usr/local/hadoop/bin/hdfs dfs -rm -r /p2tl1/input/
	/usr/local/hadoop/bin/hdfs dfs -rm -r /p2tl1/output/
	../../stop.sh
    break
  else
    echo "number out of range, try again"
  fi
done
