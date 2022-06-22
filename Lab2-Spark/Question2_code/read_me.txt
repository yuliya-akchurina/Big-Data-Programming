------------------------------------------------------------------------------------------------------
CISC5950 – Big Data Programming – Spring 2022
Professor: Ying Mao
Lab 2 - Question 2
Date 4/27/2022
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

Solving the Spark problem assigned in the class "Big Data Programming" 
we will be utilizing a Hadoop HDFS cluster and Spark hosted in Google 
Cloud. The cluster will contain a Master/Worker node and 2 additional 
worker nodes. The source data is provided in a csv format and will be
accessed through test.sh file. 

-------------------------------------------------------------------------------------------------------
Artifacts
-------------------------------------------------------------------------------------------------------

The following components are required to solve the assigned problem:

3 Node Hadoop Cluster	- Hosted in Google Cloud subscription
1 Source csv data file	- Parking_Violations_Issued_-_Fiscal_Year_2021.csv
1 Run file	        - test.sh written in Bash script
1 PySpark code file	- written in Python with the use of PySpark libraries 

-------------------------------------------------------------------------------------------------------
Execution
-------------------------------------------------------------------------------------------------------

To execute Spark to process the source csv file we must prepare the
the system by placing the files in the correct directories on the master
node.

Place the files in directories as follows:

File Name						Directory
Parking_Violations_Issued_-_Fiscal_Year_2021.csv  	/mapreduce-test/mapreduce-test-python/project1/Parking_Violations_Issued_-_Fiscal_Year_2021.csv
test.sh							/spark-examples/Lab2/l2q2
black_car.py						/spark-examples/Lab2/l2q2


To run the process:
1. Navigate to the directory: /spark-examples/Lab2/l2q2
2. Execute the bash run file:
	bash test.sh
	
Output will be written to the screen. 