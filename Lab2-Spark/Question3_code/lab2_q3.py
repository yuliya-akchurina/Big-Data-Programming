# Lab2 Question3 
# Yuliya Akchurina 
# When are tickets most likely to be issued? - Find the most frequent date when tickets were issued 

from __future__ import print_function

import sys
import pyspark
from pyspark.sql import SparkSession
#from pyspark.sql import functions as f
from pyspark.sql.functions import *
#from multiprocessing.pool import ThreadPool
import time

# dataset /mapreduce-test/mapreduce-test-python/project1/Parking_Violations_Issued_-_Fiscal_Year_2021.csv
def ticket_date():
    spark = SparkSession.builder.appName("Tickets Date and Time").getOrCreate()

    # Working with data input with test.sh
    df = spark.read.format("csv").option("header", "true").load(sys.argv[1])

    # Working data input without test.sh
    #df=spark.read.csv("/mapreduce-test/mapreduce-test-python/project1/Parking_Violations_Issued_-_Fiscal_Year_2021.csv",\
    #    header=True, inferSchema=True)
    
    ticket_df = df.select(col("Issue Date"))
    ticket_df.printSchema()
   
    #Drop rows with missing values 
    ticket_df = ticket_df.na.drop()

    print("Count of Tickets_per_date")
    df_ticket_count = ticket_df.groupBy("Issue Date").agg(count("Issue Date").alias("tickets_per_date"))\
        .orderBy(col("tickets_per_date").desc())

    print("The Top 5 dates with the most tickets issused")
    print(df_ticket_count.show(5))
    print(f"The date with the most tickets issued: {df_ticket_count.first()}")

    spark.stop()

if __name__=="__main__":
    ticket_date()
	
    #plist = [2, 3, 4, 5]
    #starttime = time.time()
    #pool = ThreadPool(10)
    #pool.map(ticket_date(), plist)
    #pool.close()
    #endtime = time.time()
    #print(f"Time taken {endtime-starttime} seconds")


