------------------------------------------------------------------------------------------------------
CISC5950 – Big Data Programming – Spring 2022
Professor: Ying Mao
Project 1 - Part1 Question 4
Date 4/5/2022
Student: Yuliya Akchurina
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
1 Source csv data file	- Parking_Violations_Issued_-_Fiscal_Year_2021.csv
1 Map/Reduce run file	- test.sh written in Bash script
1 Mapper code file	- mapper.py written in Python
1 Reducer code file	- reducer.py written in Python


-------------------------------------------------------------------------------------------------------
Execution
-------------------------------------------------------------------------------------------------------

To execute Map/Reduce to process the source csv file we must prepare the
the system by placing the files in the correct directories on the master
node.

Place the files in directories as follows:

File Name						Directory
Parking_Violations_Issued_-_Fiscal_Year_2021.csv	/mapreduce-test/mapreduce-test-python/project1
test.sh							/mapreduce-test/mapreduce-test-python/Part1Q4
mapper.py						/mapreduce-test/mapreduce-test-python/Part1Q4
reducer.py						/mapreduce-test/mapreduce-test-python/Part1Q4


To run the Map/Reduce process:
1. Navigate to the directory: /mapreduce-test/mapreduce-test-python/Part1Q4
2. Execute the bash run file:
	bash test.sh
	
Output will be written to the screen. 