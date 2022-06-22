# Lab2 Question1
# Yuliya Akchurina 

from __future__ import print_function

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count, round
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DecimalType, FloatType
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as f
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.window import Window
import pyspark
#import pandas as pd

import sys

# dataset ../../../   /mapreduce-test/mapreduce-test-python/project1/shot_logs.csv
def calculate_kmeans():
    spark = SparkSession.builder.appName("KMeans_NBA").getOrCreate()
    
    # Working data input without test.sh
    #df=spark.read.csv("/mapreduce-test/mapreduce-test-python/project1/shot_logs.csv", header=True, inferSchema=True).withColumn("id", monotonically_increasing_id())
    
    # Working with data input with test.sh
    df = spark.read.format("csv").option("header", "true").load(sys.argv[1])

    # add row number column 
    df= df.withColumn("new_column",lit("ABC"))

    w = Window().partitionBy('new_column').orderBy(lit('A'))
    df = df.withColumn("id", row_number().over(w)).drop("new_column")

    #print("DF Schema")
    #df.printSchema()

    player_list = ['james harden', 'chris paul', 'stephen curry', 'lebron james']

    player_df = df.select(col("id"), col("player_name"),col("SHOT_DIST"),col("CLOSE_DEF_DIST"), col("SHOT_CLOCK"), col("SHOT_RESULT"))
    player_df = player_df.filter(player_df.player_name.isin(player_list))

    player_df.printSchema()
    player_df.show(truncate = False)
   
    #Drop rows with missing values 
    player_df = player_df.na.drop()
   
    # encode column shot_reult into 0 and 1
    # cast 'SHOT_DIST', 'CLOSE_DEF_DIST', 'SHOT_CLOCK' from strings into floats
    player_df = player_df.na.replace({'missed':'0', 'made':'1'})
    
    # Convert dataframe to Double Type and Round to 2 decimals 
    player_df = player_df.withColumn("SHOT_RESULT",round(player_df.SHOT_RESULT.cast(IntegerType()),2))
    player_df = player_df.withColumn("SHOT_DIST",round(player_df.SHOT_DIST.cast(DoubleType()),2))
    player_df = player_df.withColumn("CLOSE_DEF_DIST",round(player_df.CLOSE_DEF_DIST.cast(DoubleType()),2))
    player_df = player_df.withColumn("SHOT_CLOCK",round(player_df.SHOT_CLOCK.cast(DoubleType()),2))

    #Convert <class 'pyspark.sql.dataframe.DataFrame'> to dataframe with label and Features, Features is a sparse vector 
    FEATURES_COL = ["SHOT_DIST", "CLOSE_DEF_DIST", "SHOT_CLOCK"]
    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    df_kmeans = vecAssembler.transform(player_df).select('id', 'features')
    
    # Trains a k-means model.
    kmeans = KMeans().setK(4).setSeed(1)
    model = kmeans.fit(df_kmeans)

    # Make prediction
    prediction = model.transform(df_kmeans)

    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()

    silhouette = evaluator.evaluate(prediction)
    print("Silhouette with squared Euclidean distance = " + str(silhouette))

    # Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    #Assign rows to clusters
    transformed = model.transform(df_kmeans).select('id', 'prediction')
    rows = transformed.collect()

    #df_kmeans.show(truncate = False)

    schema = StructType([ \
    StructField("id",StringType(),True), \
    StructField("cluster_id",IntegerType(),True)])

    df_pred = spark.createDataFrame(rows, schema)

    # Join dataframes on the ID column 
    df_pred = df_pred.join(player_df, 'id')
    #df_pred.show()

    # Calculate hit rate per each player for each of the 4 clusters = 16 rows
    df_avg = df_pred.groupBy("player_name", "cluster_id").agg(avg("SHOT_RESULT").alias("hit_rate"))\
        .sort(["player_name","cluster_id"], ascending = True)

    #rename col player name to avoid confusion 
    df_avg = df_avg.withColumnRenamed("player_name","player_name_four_zones")

    #print("df_avg")
    #df_avg.show()
    
    # highest hit rate per payer
    df_max = df_pred.groupBy("player_name", "cluster_id").agg(avg("SHOT_RESULT").alias("hit_rate"))\
        .groupBy("player_name").agg(max("hit_rate").alias("max_hit_rate"))\
        .sort(["player_name"], ascending = True)

    #print("df_pred")
    #df_pred.show()

    #print("df_max")
    #df_max.show()
    
    #df_result = df_avg.join(df_max, df_max.max_hit_rate == df_avg.hit_rate, "inner")
    df_result = df_max.join(df_avg, df_max.max_hit_rate == df_avg.hit_rate, "left")

    #print("df_result")
    #df_result.show()

    print("Here are the most comfortable zone for each player \n\
        based of the highest hit rate")
    df_result.select("player_name", "max_hit_rate", "cluster_id").show()

    #df_result_pandas = df_result.toPandas()
    #print(df_result_pandas)

    print("Here are the most comfortable zone for each player \n\
        based of the highest hit rate. \n\
        With max hit rate rounded to two decimal points.")
    df_result_round = df_result.withColumn("max_hit_rate", f.round(df_result["max_hit_rate"], 2))
    df_result_round.select("player_name", "max_hit_rate", "cluster_id").show()
  
    spark.stop()

if __name__=="__main__":
    calculate_kmeans()
