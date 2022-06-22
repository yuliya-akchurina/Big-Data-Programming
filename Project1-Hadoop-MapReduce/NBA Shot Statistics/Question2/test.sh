#!/bin/bash
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q2/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /Part2Q2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../mapreduce-test-python/project1/shot_logs.csv /Part2Q2/input/

rm -f centroids.txt

/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
-file ../../mapreduce-test-python/Part2Q2/mapper_init_centroids.py -mapper ../../mapreduce-test-python/Part2Q2/mapper_init_centroids.py \
-file ../../mapreduce-test-python/Part2Q2/reducer_init_centroids.py -reducer ../../mapreduce-test-python/Part2Q2/reducer_init_centroids.py \
-input /Part2Q2/input/* -output /Part2Q2/output_red_init
hadoop fs -copyToLocal /Part2Q2/output_red_init/part-00000 centroids.txt

i=1
while [ $i -le 30 ]
do
	/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
	-file centroids.txt -file ../../mapreduce-test-python/Part2Q2/mapper.py -mapper ../../mapreduce-test-python/Part2Q2/mapper.py \
	-file ../../mapreduce-test-python/Part2Q2/reducer.py -reducer ../../mapreduce-test-python/Part2Q2/reducer.py \
	-input /Part2Q2/input/* \
	-output /Part2Q2/output_dir$i

	rm -f new_centroids.txt
	hadoop fs -copyToLocal /Part2Q2/output_dir$i/part-00000 new_centroids.txt
	cat new_centroids.txt
	seeiftrue=`python stop_condition.py`
	if [ $seeiftrue = 1 ]
	then
		rm centroids.txt
		hadoop fs -copyToLocal /Part2Q2/output_dir$i/part-00000 centroids.txt
		break
	else
		rm centroids.txt
		hadoop fs -copyToLocal /Part2Q2/output_dir$i/part-00000 centroids.txt
	fi
	i=$((i+1))
done

/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar \
-file new_centroids.txt -file ../../mapreduce-test-python/Part2Q2/mapper2.py -mapper ../../mapreduce-test-python/Part2Q2/mapper2.py \
-file new_centroids.txt -file ../../mapreduce-test-python/Part2Q2/reducer2.py -reducer ../../mapreduce-test-python/Part2Q2/reducer2.py \
-input /Part2Q2/input/* \
-output /Part2Q2/output_red2/

/usr/local/hadoop/bin/hdfs dfs -cat /Part2Q2/output_red2/part-00000

/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q2/output/
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q2/output_red_init
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q2/output_red2/
/usr/local/hadoop/bin/hdfs dfs -rm -r /Part2Q2/output_dir*
../../stop.sh