# Project 2 Question 2

# Project2 Question2
# Yuliya Akchurina 
# Heart Disease Prediction using Logistic Regression

from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator 
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
#from pyspark.mllib.evaluation import MulticlassMetrics



def predict_heart_disease():
    spark = SparkSession.builder.appName("Heart Disease Prediction using Logistic Regression").getOrCreate()
     
    # Working data input without test.sh
    # load data with infer schema to keep data formats 
    #df = spark.read.load("/spark-examples/data/question2/framingham.csv", format="csv", inferSchema="true", header="true")
    
    # Working with data input with test.sh
    df = spark.read.format("csv").option("header", "true").load(sys.argv[1])

    print(df.show())
    print(df.printSchema())

    # rename col TenYearCHD to label
    df = df.withColumnRenamed("TenYearCHD","label")

    # convert column types into floats 
    df = df.withColumn("education",df.education.cast(DoubleType()))
    df = df.withColumn("cigsPerDay",df.cigsPerDay.cast(DoubleType()))
    df = df.withColumn("BPMeds",df.BPMeds.cast(DoubleType()))
    df = df.withColumn("totChol",df.totChol.cast(DoubleType()))
    df = df.withColumn("BMI",df["BMI"].cast(DoubleType()))
    df = df.withColumn("heartRate",df.heartRate.cast(DoubleType()))
    df = df.withColumn("glucose",df.glucose.cast(DoubleType()))
    df = df.withColumn("label",df.label.cast(DoubleType()))

    # rename col TenYearCHD to label
    df = df.withColumnRenamed("TenYearCHD","label")

    #print(df.printSchema())
    #print(f"Dataframe size {df.count(), len(df.columns)}")

    # remove rows with missing values to be able to convert to sparce vector 
    df = df.na.drop()
    print(f"Dataframe size {df.count(), len(df.columns)}")

    #Convert Features to a sparse vector 
    FEATURES_COL = ["male", "age", "education", "currentSmoker", "cigsPerDay", "BPMeds", "prevalentStroke", "prevalentHyp", \
        "diabetes", "totChol", "sysBP", "diaBP", "BMI", "heartRate", "glucose"]

    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    df_model = vecAssembler.transform(df).select('features', 'label')

    #print(df_model.show())
    #print(df_model.printSchema()) 

    # split data into train and test 80/20
    df_train, df_test = df_model.randomSplit([0.8,0.2], seed = 32)

    #print(df_train.printSchema()) 
    #print(df_test.printSchema()) 
    print(f"Train Dataframe Size {df_train.count(), len(df_train.columns)}")
    print(f"Test Dataframe Size {df_test.count(), len(df_test.columns)}")

    # check that train and test dataset have balanced classes 
    print("Full dataset")
    print(df_model.groupBy('label').count().show())
    print("Train data")
    print(df_train.groupBy('label').count().show())
    print("Test data")
    print(df_test.groupBy('label').count().show())


    # Binary Logistic regression
    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    lrModel = lr.fit(df_train)

    # predictions 
    predictions = lrModel.transform(df_test)

    predictions.show()
    print(f"Predictions Columns {predictions.columns}")

    print("Predictions DF class distribution")
    print(predictions.groupBy('label').count().show())

    # evaluate 
    evaluator = BinaryClassificationEvaluator()
    print("Test Area Under ROC ", evaluator.evaluate(predictions))

    # model accuracy 
    accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(predictions.count())
    print("Accuracy : ",accuracy, "\n")
    
    df_pred = predictions.select('label','rawPrediction', 'probability', 'prediction')

    #Prediction Accuracy with MulticlassClassificationEvaluator
    evaluator2 = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label' ,metricName='accuracy')
    accuracy2 = evaluator2.evaluate(df_pred)
    print(f"Prediction Accuracy is {accuracy2}")


    spark.stop()

if __name__=="__main__":
    predict_heart_disease()

