# Project2 Question3
# Yuliya Akchurina 
# Decision Tree and Random Forest Classifier on Census Income Data

from __future__ import print_function

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def predict_income():
    spark = SparkSession.builder.appName("Decision Tree and Random Forest Classifier on Census Income Data").getOrCreate()
    
    # Train Data 
    # Working data input without test.sh
    # load data with infer schema to keep data formats 
    #df = spark.read.load("/spark-examples/data/question3and4/adult_data.csv", format="csv", inferSchema="true", header="false")
    # Test data 
    #df_test = spark.read.load("/spark-examples/data/question3and4/adult_test.csv", format="csv", inferSchema="true", header="false")
    
    # Working with data input with test.sh
    df = spark.read.load("/question4/input/adult_data.csv", format="csv", inferSchema="true", header="false")
    df_test = spark.read.load("/question4/input/adult_test.csv", format="csv", inferSchema="true", header="false")

    # add column names to dataframe
    col_names = ["age", "workclass", "fnlwgt", "education", "education-num", "marital-status", \
        "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", \
        "native-country", "label"]
    
    df = df.toDF(*col_names)
    df_test = df_test.toDF(*col_names)
    print(df.printSchema())
    print(f"Train Dataframe size {df.count(), len(df.columns)}")
    print(f"Test Dataframe size {df_test.count(), len(df_test.columns)}")

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
    #print(f"Finalized Train Dataframe Size {new_df.count(), len(new_df.columns)}")

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
    new_df_test = new_df_test.na.drop()
    #print(f"Finalized Test Dataframe Size {new_df_test.count(), len(new_df_test.columns)}")


    # Create features vector from Training Dataset
    FEATURES_COL = ['age', 'workclass_encoded', 'education-num', 'marital_status_encoded', \
        'occupation_encoded', 'relationship_encoded', 'race_encoded', 'sex_encoded', 'capital-gain',\
        'capital-loss', 'hours-per-week', 'country_encoded']

    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    trainingData = vecAssembler.transform(new_df).select('features', 'label')

    # Create features vector from Testing Dataset
    FEATURES_COL = ['age', 'workclass_encoded', 'education-num', 'marital_status_encoded', \
        'occupation_encoded', 'relationship_encoded', 'race_encoded', 'sex_encoded', 'capital-gain',\
        'capital-loss', 'hours-per-week', 'country_encoded']

    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    testData = vecAssembler.transform(new_df_test).select('features', 'label')

    #Decision Tree Classifier - Train a model
    dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxBins = 42)

    # Train model.  This also runs the indexers
    tree_model = dt.fit(trainingData)

    # Make predictions
    predictions = tree_model.transform(testData)
	predictions.show()

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"Decision Tree Classifier Accuracy {accuracy}")
    print("Test Error = %g " % (1.0 - accuracy))

    # model summary 
    #print(tree_model.toDebugString)

    # Random Forest - Train a RandomForest model
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", maxBins = 42, numTrees=10)

    # Convert indexed labels back to original labels
    #labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)

    # Train model.  This also runs the indexers
    forest_model = rf.fit(trainingData)

    # Make predictions.
    predictions = forest_model.transform(testData)
    predictions.show()

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"Random Forest Classifier Accuracy {accuracy}")
    print("Test Error = %g" % (1.0 - accuracy))

	# summary only
    #print(forest_model.toDebugString)


    spark.stop()

if __name__=="__main__":
    predict_income()
