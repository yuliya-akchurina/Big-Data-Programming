# Project2 Question3
# Yuliya Akchurina 
# Logistic Regression classifier on Census Income Data

from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator 

def predict_income():
    spark = SparkSession.builder.appName("Logistic Regression classifier on Census Income Data").getOrCreate()
    
    # Train Data 
    # Working data input without test.sh
    # load data with infer schema to keep data formats 
    #df = spark.read.load("/spark-examples/data/question3and4/adult_data.csv", format="csv", inferSchema="true", header="false")

    # Test data 
    #df_test = spark.read.load("/spark-examples/data/question3and4/adult_test.csv", format="csv", inferSchema="true", header="false")
    
    # Working with data input with test.sh
    df = spark.read.load("/question3/input/adult_data.csv", format="csv", inferSchema="true", header="false")
    df_test = spark.read.load("/question3/input/adult_test.csv", format="csv", inferSchema="true", header="false")

    # add column names to dataframe
    col_names = ["age", "workclass", "fnlwgt", "education", "education-num", "marital-status", \
        "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", \
        "native-country", "label"]
    
    df = df.toDF(*col_names)
    df_test = df_test.toDF(*col_names)
    #print(df.printSchema())
    #print(f"Train Dataframe size {df.count(), len(df.columns)}")
    #print(f"Test Dataframe size {df_test.count(), len(df_test.columns)}")

    # drop two columns  
    df = df.drop("fnlwgt", "education")
    df_test = df_test.drop("fnlwgt", "education")
    print(df.printSchema())
    print(f"Train Dataframe size {df.count(), len(df.columns)}")

    # Train Data - Remove rows containing "?"
    expr = "\?"
    new_df = df.filter(~ df["age"].rlike(expr))
    new_df = new_df.filter(~ new_df["workclass"].rlike(expr))
    new_df = new_df.filter(~ new_df["education-num"].rlike(expr))
    new_df = new_df.filter(~ new_df["marital-status"].rlike(expr))
    new_df = new_df.filter(~ new_df["occupation"].rlike(expr))
    new_df = new_df.filter(~ new_df["relationship"].rlike(expr))
    new_df = new_df.filter(~ new_df["race"].rlike(expr))
    new_df = new_df.filter(~ new_df["sex"].rlike(expr))
    new_df = new_df.filter(~ new_df["capital-gain"].rlike(expr))
    new_df = new_df.filter(~ new_df["capital-loss"].rlike(expr))
    new_df = new_df.filter(~ new_df["hours-per-week"].rlike(expr))
    new_df = new_df.filter(~ new_df["native-country"].rlike(expr))

    print(f"Train Dataframe size after removal of rows containing missing values\n {new_df.count(), len(new_df.columns)}")
    #new_df.show(30)

    # Test Data - Remove rows containing "?"
    expr = "\?"
    new_df_test = df_test.filter(~ df_test["age"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["workclass"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["education-num"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["marital-status"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["occupation"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["relationship"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["race"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["sex"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["capital-gain"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["capital-loss"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["hours-per-week"].rlike(expr))
    new_df_test = new_df_test.filter(~ new_df_test["native-country"].rlike(expr))

    print(f"Test Dataframe size after removal of rows containing missing values\n {new_df_test.count(), len(new_df_test.columns)}")

    # Train Data - convert column label to 0 and 1
    new_df = new_df.withColumn('label', regexp_replace('label', '<=50K', '0'))\
        .withColumn('label', regexp_replace('label', '>50K', '1'))
    # change column label to integer
    new_df = new_df.withColumn('label',col('label').cast(IntegerType()))\
        .withColumn('age',col('age').cast(IntegerType()))\
        .withColumn('education-num',col('education-num').cast(IntegerType()))\
        .withColumn('capital-gain',col('capital-gain').cast(IntegerType()))\
        .withColumn('capital-loss',col('capital-loss').cast(IntegerType()))\
        .withColumn('hours-per-week',col('hours-per-week').cast(IntegerType()))

    # Test Data - convert column label to 0 and 1
    new_df_test = new_df_test.withColumn('label', regexp_replace('label', '<=50K', '0'))\
        .withColumn('label', regexp_replace('label', '>50K', '1'))
    # change column label to integer
    new_df_test = new_df_test.withColumn('label',col('label').cast(IntegerType()))\
        .withColumn('age',col('age').cast(IntegerType()))\
        .withColumn('education-num',col('education-num').cast(IntegerType()))\
        .withColumn('capital-gain',col('capital-gain').cast(IntegerType()))\
        .withColumn('capital-loss',col('capital-loss').cast(IntegerType()))\
        .withColumn('hours-per-week',col('hours-per-week').cast(IntegerType()))

    # Train Data - columns with strings = workclass, marital-status, occupation, relationship, race, sex, native-country
    # encode columns 
    workclassEncoder = StringIndexer(inputCol='workclass',outputCol='workclass_encoded').fit(new_df)
    new_df = workclassEncoder.transform(new_df)
    maritalEncoder = StringIndexer(inputCol='marital-status',outputCol='marital_status_encoded').fit(new_df)
    new_df = maritalEncoder.transform(new_df)
    occupationEncoder = StringIndexer(inputCol='occupation',outputCol='occupation_encoded').fit(new_df)
    new_df = occupationEncoder.transform(new_df)
    relationshipEncoder = StringIndexer(inputCol='relationship',outputCol='relationship_encoded').fit(new_df)
    new_df = relationshipEncoder.transform(new_df)
    raceEncoder = StringIndexer(inputCol='race',outputCol='race_encoded').fit(new_df)
    new_df = raceEncoder.transform(new_df)
    sexEncoder = StringIndexer(inputCol='sex',outputCol='sex_encoded').fit(new_df)
    new_df = sexEncoder.transform(new_df)
    countryEncoder = StringIndexer(inputCol='native-country',outputCol='country_encoded').fit(new_df)
    new_df = countryEncoder.transform(new_df)

    print(f" Train data {new_df.printSchema()}")
    #new_df.show()
    
    new_df = new_df.select('age', 'workclass_encoded', 'education-num', 'marital_status_encoded', \
        'occupation_encoded', 'relationship_encoded', 'race_encoded', 'sex_encoded', 'capital-gain',\
        'capital-loss', 'hours-per-week', 'country_encoded', 'label')
    
    print(f"Finalized Train Dataframe Schema {new_df.printSchema()}")
    #new_df.show()
    print(f"Finalized Train Dataframe Size {new_df.count(), len(new_df.columns)}")
    new_df = new_df.na.drop()
    print(f"Finalized Train Dataframe Size {new_df.count(), len(new_df.columns)}")

    # Test Data - columns with strings = workclass, marital-status, occupation, relationship, race, sex, native-country
    # encode columns 
    workclassEncoder = StringIndexer(inputCol='workclass',outputCol='workclass_encoded').fit(new_df_test)
    new_df_test = workclassEncoder.transform(new_df_test)
    maritalEncoder = StringIndexer(inputCol='marital-status',outputCol='marital_status_encoded').fit(new_df_test)
    new_df_test = maritalEncoder.transform(new_df_test)
    occupationEncoder = StringIndexer(inputCol='occupation',outputCol='occupation_encoded').fit(new_df_test)
    new_df_test = occupationEncoder.transform(new_df_test)
    relationshipEncoder = StringIndexer(inputCol='relationship',outputCol='relationship_encoded').fit(new_df_test)
    new_df_test = relationshipEncoder.transform(new_df_test)
    raceEncoder = StringIndexer(inputCol='race',outputCol='race_encoded').fit(new_df_test)
    new_df_test = raceEncoder.transform(new_df_test)
    sexEncoder = StringIndexer(inputCol='sex',outputCol='sex_encoded').fit(new_df_test)
    new_df_test = sexEncoder.transform(new_df_test)
    countryEncoder = StringIndexer(inputCol='native-country',outputCol='country_encoded').fit(new_df_test)
    new_df_test = countryEncoder.transform(new_df_test)

    print(new_df_test.printSchema())
    new_df_test.show()
    
    new_df_test = new_df_test.select('age', 'workclass_encoded', 'education-num', 'marital_status_encoded', \
        'occupation_encoded', 'relationship_encoded', 'race_encoded', 'sex_encoded', 'capital-gain',\
        'capital-loss', 'hours-per-week', 'country_encoded', 'label')
    
    print(f"Test Dataframe: \n{new_df_test.printSchema()}")
    new_df_test.show()
    print(f"Finalized Test Dataframe Size {new_df_test.count(), len(new_df_test.columns)}")
    new_df_test = new_df_test.na.drop()
    print(f"Finalized Test Dataframe Size {new_df_test.count(), len(new_df_test.columns)}")

    

    # Create features vector from Training Dataset
    FEATURES_COL = ['age', 'workclass_encoded', 'education-num', 'marital_status_encoded', \
        'occupation_encoded', 'relationship_encoded', 'race_encoded', 'sex_encoded', 'capital-gain',\
        'capital-loss', 'hours-per-week', 'country_encoded']

    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    train_data = vecAssembler.transform(new_df).select('features', 'label')

    # Create features vector from Testing Dataset
    FEATURES_COL = ['age', 'workclass_encoded', 'education-num', 'marital_status_encoded', \
        'occupation_encoded', 'relationship_encoded', 'race_encoded', 'sex_encoded', 'capital-gain',\
        'capital-loss', 'hours-per-week', 'country_encoded']

    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    test_data = vecAssembler.transform(new_df_test).select('features', 'label')

    # Logistic Regression model trained On Training data 
    lr = LogisticRegression(maxIter=15, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    lrModel = lr.fit(train_data)

    # Print the coefficients and intercept for logistic regression
    print("Coefficients: " + str(lrModel.coefficients))
    print("Intercept: " + str(lrModel.intercept))

    #Extract the summary from the returned LogisticRegressionModel instance trained
    trainingSummary = lrModel.summary

    # Obtain the objective per iteration
    objectiveHistory = trainingSummary.objectiveHistory
    print("objectiveHistory:")
    for objective in objectiveHistory:
        print(objective)

    # Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
    trainingSummary.roc.show()
    print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

    # Set the model threshold to maximize F-Measure
    fMeasure = trainingSummary.fMeasureByThreshold
    maxFMeasure = fMeasure.groupBy().max('F-Measure').select('max(F-Measure)').head()
    bestThreshold = fMeasure.where(fMeasure['F-Measure'] == maxFMeasure['max(F-Measure)']) \
        .select('threshold').head()['threshold']
    lr.setThreshold(bestThreshold)

    #for multiclass, we can inspect metrics on a per-label basis
    print("False positive rate by label:")
    for i, rate in enumerate(trainingSummary.falsePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("True positive rate by label:")
    for i, rate in enumerate(trainingSummary.truePositiveRateByLabel):
        print("label %d: %s" % (i, rate))

    print("Precision by label:")
    for i, prec in enumerate(trainingSummary.precisionByLabel):
        print("label %d: %s" % (i, prec))

    print("Recall by label:")
    for i, rec in enumerate(trainingSummary.recallByLabel):
        print("label %d: %s" % (i, rec))

    print("F-measure by label:")
    for i, f in enumerate(trainingSummary.fMeasureByLabel()):
        print("label %d: %s" % (i, f))

    accuracy = trainingSummary.accuracy
    falsePositiveRate = trainingSummary.weightedFalsePositiveRate
    truePositiveRate = trainingSummary.weightedTruePositiveRate
    fMeasure = trainingSummary.weightedFMeasure()
    precision = trainingSummary.weightedPrecision
    recall = trainingSummary.weightedRecall
    print("LR Accuracy: %s\nFPR: %s\nTPR: %s\nF-measure: %s\nPrecision: %s\nRecall: %s"
        % (accuracy, falsePositiveRate, truePositiveRate, fMeasure, precision, recall))

    # Test model 
    lr_predictions = lrModel.transform(test_data)
    lr_predictions.show()

    print(f"Predictions Columns {lr_predictions.columns}")
    print("Predictions class distribution")
    print(lr_predictions.groupBy('label').count().show())

    # evaluate predictions
    lr_evaluator = BinaryClassificationEvaluator()
    print("Test Area Under ROC ", lr_evaluator.evaluate(lr_predictions))

    # model accuracy 
    accuracy = lr_predictions.filter(lr_predictions.label == lr_predictions.prediction).count() / float(lr_predictions.count())
    print("Logistic Regression Test Data Prediction Accuracy : ",accuracy, "\n")

    spark.stop()

if __name__=="__main__":
    predict_income()

