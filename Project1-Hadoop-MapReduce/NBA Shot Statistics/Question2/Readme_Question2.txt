------------------------------------------------------------------------------------------------------
CISC5950 – Big Data Programming – Spring 2022
Professor: Ying Mao
Project 1 - Part2 Question 2
Date 4/5/2022
Students: Yuliya Akchurina
-------------------------------------------------------------------------------------------------------

                               README

-------------------------------------------------------------------------------------------------------
CONTENTS
-------------------------------------------------------------------------------------------------------

     o Introduction
     o Artifacts
     o Execution
 
-------------------------------------------------------------------------------------------------------
Introduction
-------------------------------------------------------------------------------------------------------

Solving the Map/Reduce problem assigned in the class "Big Data 
Programming" we will be utilizing a Hadoop cluster hosted in Google 
Cloud. The cluster will contain a Master/Worker node and 2 additional 
worker nodes. The source data is provided in a csv format and will be
streamed line-by-line via Hadoop through the Map/Reduce architecture.

-------------------------------------------------------------------------------------------------------
Artifacts
-------------------------------------------------------------------------------------------------------

The following components are required to solve the assigned problem:

3 Node Hadoop Cluster	- Hosted in Google Cloud subscription
1 Source csv data file	- shot_logs.csv
1 Map/Reduce run file	- test.sh written in Bash script
3 Mapper code files	- mapper_init_centroids.py, mapper.py, mapper2.py written in Python
3 Reducer code files	- reducer_init_centroids.py, reducer.py,  reducer2.py written in Python


-------------------------------------------------------------------------------------------------------
Execution
-------------------------------------------------------------------------------------------------------

To execute Map/Reduce to process the source csv file we must prepare the
the system by placing the files in the correct directories on the master
node.

Place the files in directories as follows:

File Name			Directory
shot_logs.csv  			/mapreduce-test/mapreduce-test-python/project1/shot_logs.csv
test.sh				/mapreduce-test/mapreduce-test-python/Part2Q2
mapper_init_centroids.py	/mapreduce-test/mapreduce-test-python/Part2Q2
reducer_init_centroids.py	/mapreduce-test/mapreduce-test-python/Part2Q2
mapper.py			/mapreduce-test/mapreduce-test-python/Part2Q2
reducer.py			/mapreduce-test/mapreduce-test-python/Part2Q2
mapper2.py			/mapreduce-test/mapreduce-test-python/Part2Q2
reducer2.py			/mapreduce-test/mapreduce-test-python/Part2Q2
stop_condition.py		/mapreduce-test/mapreduce-test-python/Part2Q2
centroids.txt			/mapreduce-test/mapreduce-test-python/Part2Q2


To run the Map/Reduce process:
1. Navigate to the directory: /mapreduce-test/mapreduce-test-python/Part2Q2
2. Execute the bash run file:
	bash test.sh
	
Output will be written to the screen. 