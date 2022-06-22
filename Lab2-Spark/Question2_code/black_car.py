# Lab2 Question 2
# Yuliya Akchurina 
# Given a Black vehicle parking illegally at 34510, 10030, 34050 (street codes). 
# What is the probability that it will get an ticket? (very rough prediction).

from __future__ import print_function

import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import lower, col
from pyspark.sql.window import Window
import math

def prob_blackcar():
    spark = SparkSession.builder.appName("Probability of black car getting a ticket").getOrCreate()

    car_color_list = ['b', 'bk', 'blk', 'black']
    street_codes = ['34510', '10030', '34050'] 

    # Working with data input with test.sh
    df = spark.read.format("csv").option("header", "true").load(sys.argv[1])
    
    # Working data input without test.sh
    # Dataset Parking_Violations_Issued_-_Fiscal_Year_2021.csv
    #df=spark.read.csv("/mapreduce-test/mapreduce-test-python/project1/Parking_Violations_Issued_-_Fiscal_Year_2021.csv", \
    #    header=True, inferSchema=True)

    #df.show(2)
    #df.printSchema()

    df_cars = df.select(col("Street Code1"),col("Street Code2"), \
        col("Street Code3"), col("Vehicle Color"), col("Violation Description"), \
        col("No Standing or Stopping Violation"), col("Hydrant Violation"), \
        col("Double Parking Violation"))

    df_cars.printSchema()
    #print("Size of df_cars")
    #print((df_cars.count(), len(df_cars.columns)))

    #print(df_cars.filter(df_cars["Vehicle Color"].isNull()).count())

    #Remove rows where the column Vehicle Color has missing values
    df_cars = df_cars.na.drop(subset = ["Vehicle Color"])

    # convert all columns values to lower case
    df_cars = df_cars.withColumn("vehicle_color", lower(col("Vehicle Color"))).drop("Vehicle Color")
    df_cars = df_cars.withColumn("violation_description", lower(col("Violation Description"))).drop("Violation Description")
    df_cars = df_cars.withColumn("no_standing_or_stopping", lower(col("No Standing or Stopping Violation"))).drop("No Standing or Stopping Violation")
    df_cars = df_cars.withColumn("hydrant_violation", lower(col("Hydrant Violation"))).drop("Hydrant Violation")
    df_cars = df_cars.withColumn("double_parking", lower(col("Double Parking Violation"))).drop("Double Parking Violation")

    # rename columns 
    df_cars = df_cars.withColumnRenamed("Street Code1","street_code1")\
        .withColumnRenamed("Street Code2","street_code2")\
        .withColumnRenamed("Street Code3","street_code3")

    df_cars.printSchema()
    print((df_cars.count(), len(df_cars.columns)))
    df_cars.show(2)
    
    df_black = df_cars.filter((df_cars.vehicle_color.isin(car_color_list))\
        &((df_cars.street_code1.isin(street_codes))\
        |(df_cars.street_code2.isin(street_codes))\
        |(df_cars.street_code3.isin(street_codes))))
    
    print("Size of DF with black cars in 3 street codes 34510, 10030, 34050")
    print((df_black.count(), len(df_black.columns)))
    #df_black.show()

    count_black_3codes = df_black.count()
    #print(f"Count of black cars in 3 street codes {count_black_3codes}")

    df_ticket = df_black.filter((df_black.violation_description.contains('park'))\
        |(df_black.violation_description.contains('stand'))\
        |(df_black.no_standing_or_stopping.contains('park'))\
        |(df_black.no_standing_or_stopping.contains('stand'))\
        |(df_black.hydrant_violation.contains('park'))\
        |(df_black.hydrant_violation.contains('stand'))\
        |(df_black.double_parking.contains('park'))\
        |(df_black.double_parking.contains('stand')) )

    print("Size of DF for parking tickets of black cars in 3 street codes 34510, 10030, 34050")
    print((df_ticket.count(), len(df_ticket.columns)))

    count_ticket = df_ticket.count()
    #print(f"Count of black cars ticketed for parking in 3 street codes {count_ticket}")

    if count_black_3codes != 0:
        probability = (count_ticket/count_black_3codes) * 100

        #print(f"Probability of getting a ticket is {probability}%")
        print(f"\nProbability of getting a parking ticket for a black car \n \
            in street codes 34510, 10030, 34050 is {math.ceil(probability*100)/100}%\n")
        
    else: 
        print("Unable to calculate probability")

    spark.stop()

if __name__=="__main__":
    prob_blackcar()
