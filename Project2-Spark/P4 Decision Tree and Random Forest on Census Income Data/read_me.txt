------------------------------------------------------------------------------------------------------
CISC5950 – Big Data Programming – Spring 2022
Professor: Ying Mao
Project 2 - Question 4
Date 5/13/2022
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
worker nodes. The source data is provided in a csv format and will bes
accessed through test.sh file. 

-------------------------------------------------------------------------------------------------------
Artifacts
-------------------------------------------------------------------------------------------------------

The following components are required to solve the assigned problem:

3 Node Hadoop Cluster	- Hosted in Google Cloud subscription
2 Source csv data files	- adult_data.csv, adult_test.csv
1 Run file	        - test.sh written in Bash script
1 PySpark code file	- q4.py written in Python

-------------------------------------------------------------------------------------------------------
Execution
-------------------------------------------------------------------------------------------------------

To execute Spark to process the source csv file we must prepare the
the system by placing the files in the correct directories on the master
node.

Place the files in directories as follows:

File Name			Directory	
adult_data.csv			/spark-examples/data/question3and4/adult_data.csv
adult_test.csv			/spark-examples/data/question3and4/adult_test.csv
test.sh				/spark-examples/project2/question4/test.sh
q4.py				/spark-examples/project2/question4/q4.py


To run the process:
1. Navigate to the directory: /spark-examples/project2/question4
2. Execute the bash run file:
	bash test.sh
	
Output will be written to the screen. 