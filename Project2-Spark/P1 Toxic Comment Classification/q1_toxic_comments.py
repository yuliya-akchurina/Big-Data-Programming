# Project2 Question1
# Yuliya Akchurina 
# Classify toxic comments using Logistic Regression 

from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import sys
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import regexp_replace

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.feature import HashingTF, IDF


def classify_toxic_comments():
    spark = SparkSession.builder.appName("Toxic Commment Classification").getOrCreate()
    
    # Upload csv data files 
    #csv_path = "/spark-examples/data/question1/train.csv"
    csv_path = "/question1/input/train.csv"
    df=spark.read.option("header","true")\
        .option("encoding", "UTF-8")\
        .option('multiline', 'true')\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .option("quote", "\"""")\
        .option("escape", "\"""")\
        .csv(csv_path)
    
    #csv_path_test = "/spark-examples/data/question1/test.csv"
    csv_path_test = "/question1/input/test.csv"
    df_test=spark.read.option("header","true")\
        .option("encoding", "UTF-8")\
        .option('multiline', 'true')\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .option("quote", "\"""")\
        .option("escape", "\"""")\
        .csv(csv_path_test)
    
    #csv_path_test_labels = "/spark-examples/data/question1/test_labels.csv"
    csv_path_test_labels = "/question1/input/test_labels.csv"
    df_test_labels=spark.read.option("header","true")\
        .option("encoding", "UTF-8")\
        .option('multiline', 'true')\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .option("quote", "\"""")\
        .option("escape", "\"""")\
        .csv(csv_path_test_labels)

    df_test_labels = df_test_labels.withColumnRenamed("id", "comment_id")

    #join test comments with labels
    test_data = df_test.join(df_test_labels, df_test.id ==df_test_labels.comment_id, "inner")
    test_data = test_data.drop("comment_id")
    #print(test_data.show())
    #print(test_data.printSchema())
    #print(f"Dataframe Size {test_data.count(), len(test_data.columns)}")

    # concatenate the train and test data into one dataframe
    combined_data = df.union(test_data)
    #print(combined_data.show())
    #print(combined_data.printSchema())
    #print(f"Dataframe Size {combined_data.count(), len(combined_data.columns)}")

    
    # convert 6 label columns to integers 
    data_cleaning = combined_data.withColumn('toxic',col('toxic').cast(IntegerType()))\
        .withColumn('severe_toxic',col('severe_toxic').cast(IntegerType()))\
        .withColumn('obscene',col('obscene').cast(IntegerType()))\
        .withColumn('threat',col('threat').cast(IntegerType()))\
        .withColumn('insult',col('insult').cast(IntegerType()))\
        .withColumn('identity_hate',col('identity_hate').cast(IntegerType()))
    
    # combine 6 class columns into one with 64 possible outcomes 
    data_cleaning=data_cleaning.select("*", concat(col("toxic"),lit(","),\
        col("severe_toxic"),lit(","),\
        col("obscene"), lit(","),\
        col("threat"), lit(","),\
        col("insult"), lit(","), col("identity_hate"))\
        .alias("class_label"))
    
    # convert to lower case 
    data_cleaning = data_cleaning.withColumn("comment_text",lower(col("comment_text")))

    data_cleaning = data_cleaning.withColumn('comment_text', regexp_replace('comment_text', '\n', ' '))\
        .withColumn('comment_text', regexp_replace('comment_text', "can't", 'can not'))\
        .withColumn('comment_text', regexp_replace('comment_text', "i'm", 'i am'))\
        .withColumn('comment_text', regexp_replace('comment_text', "don't", 'do not'))\
        .withColumn('comment_text', regexp_replace('comment_text', "doesn't", 'does not'))\
        .withColumn('comment_text', regexp_replace('comment_text', "haven't", 'have not'))\
        .withColumn('comment_text', regexp_replace('comment_text', "hasn't", 'has not'))\
        .withColumn('comment_text', regexp_replace('comment_text', "'s", ' s'))\
        .withColumn('comment_text', regexp_replace('comment_text', "isn't", 'is not'))\
        .withColumn('comment_text', regexp_replace('comment_text', "aren't", 'are not'))\
        .withColumn('comment_text', regexp_replace('comment_text', "'re", ' are'))\
        .withColumn('comment_text', regexp_replace('comment_text', "'ve", ' have'))\
        .withColumn('comment_text', regexp_replace('comment_text', "'ll", ' will'))\
        .withColumn('comment_text', regexp_replace('comment_text', "'d'", ' would '))
        
    # drop rows with missing values 
    data_cleaning = data_cleaning.na.drop()

    #print(data_cleaning.printSchema())
    #data_cleaning.show(10, False)
    #data_cleaning.select('comment_text').show(10, False)

    def clean_text(c):
        c = lower(c)
        c = regexp_replace(c, "(https?\://)\S+", "") # Remove links
        c = regexp_replace(c, "(\\n)|\n|\r|\t", "") # Remove CR, tab, and LR
        c = regexp_replace(c, "(?:(?:[0-9]{2}[:\/,]){2}[0-9]{2,4})", "") # Remove dates
        c = regexp_replace(c, "@([A-Za-z0-9_]+)", "") # Remove usernames
        c = regexp_replace(c, "[0-9]", "") # Remove numbers
        c = regexp_replace(c, "\:|\/|\#|\.|\?|\!|\&|\"|\,", "") # Remove symbols
        return c
    
    data_cleaning = data_cleaning.withColumn("comment_text", clean_text(col("comment_text")))

    # trim white space, lowercase, remove punctuation 
    data_cleaning = data_cleaning.withColumn("comment_text", lower(trim(regexp_replace("comment_text",'\\p{Punct}',''))).alias('sentence'))
    
    #data_cleaning.show(truncate=False)
    #data_cleaning.show()
    #print(data_cleaning.printSchema())

    # print a row from the dataframe
    #print(data_cleaning.collect()[0])
    # row 1 col 2
    #print(data_cleaning.collect()[0][1])

    # drop rows with class label "-1,-1,-1,-1,-1,-1"
    data_cleaning = data_cleaning.filter(data_cleaning.class_label != "-1,-1,-1,-1,-1,-1")
    print(f"Dataframe Size after removing class label -1,-1,-1,-1,-1,-1 {data_cleaning.count(), len(data_cleaning.columns)}")

    # show count of distinct labels with count of comments for each
    count_labels=data_cleaning.select(countDistinct("class_label"))
    print(f"Count of distinct class values: {count_labels.show()}")
   
    print(f"Class values: {data_cleaning.groupBy('class_label').count().orderBy(col('count').desc()).show(64)}")
    #data_cleaning.groupBy('class_label').count().orderBy(col('count').desc()).show(64)

    # remove the not needed columns 
    req_col = ["comment_text", "class_label"]

    cleandata = data_cleaning.select([column for column in data_cleaning.columns if column in req_col])
    cleandata.show(5)



    # regular expression tokenizer
    regexTokenizer = RegexTokenizer(inputCol="comment_text", outputCol="words", pattern="\\W")
    # stop words
    add_stopwords = ["http","https","c","i", "me", "my", "myself", "we", "our", "ours", "ourselves",\
        "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", \
        "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", \
        "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", \
        "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", \
        "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", \
        "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", \
        "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", \
        "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", \
        "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", \
        "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", \
        "will", "just", "don", "should", "would", "now"] 

    stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)
    
    # bag of words count
    countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)

    label_stringIdx = StringIndexer(inputCol = "class_label", outputCol = "label")
    pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx])
    # Fit the pipeline to training documents.
    pipelineFit = pipeline.fit(cleandata)
    dataset = pipelineFit.transform(cleandata)
    dataset.show(5)

    """
    #show count for each word in text vocabulary  
    countdf = dataset.withColumn('word', explode('filtered'))\
    .groupBy('word')\
    .count()\
    .sort('count', ascending=False)
    
    print(countdf.show())

    uniqueWordsCount=countdf.count()
    print(f" Unique words count {uniqueWordsCount}")
    """

    #split into train and test 
    # set seed for reproducibility
    (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 21)
    print("Training Dataset Count: " + str(trainingData.count()))
    print("Test Dataset Count: " + str(testData.count()))


    lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
    lrModel = lr.fit(trainingData)
    predictions = lrModel.transform(testData)
    predictions.printSchema()
    print(predictions.show())

    predictions.filter(predictions['prediction'] == 0) \
        .select("comment_text","class_label","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)
    
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    accuracy = evaluator.evaluate(predictions)
    print(f"Prediction Accuracy {accuracy}")


    # Logistic Regression using TF-IDF Features
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=20) #minDocFreq: remove sparse terms
    pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, hashingTF, idf, label_stringIdx])
    pipelineFit = pipeline.fit(cleandata)
    dataset = pipelineFit.transform(cleandata)
    (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 21)
    lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
    lrModel = lr.fit(trainingData)
    predictions = lrModel.transform(testData)
    predictions.filter(predictions['prediction'] == 0) \
        .select("Descript","Category","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)
    
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    accuracy2 = evaluator.evaluate(predictions)
    print(f"Prediction Accuracy using TF-IDF Features {accuracy2}")


    spark.stop()

if __name__=="__main__":
    classify_toxic_comments()

