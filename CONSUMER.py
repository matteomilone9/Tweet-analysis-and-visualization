import textblob
from textblob import TextBlob
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk
from nltk import *
from nltk.corpus import *
from nltk.stem import *
from nltk.stem.porter import *
download("stopwords")
stopwords = stopwords.words('english')
download("punkt")  # download data needed by word_tokenizer
import pymongo
from pymongo import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import *
from pyspark.ml import *
import re
import findspark
import time
from IPython.display import clear_output
from kafka import *
import json

###BOOTSTRAP SERVER KAFKA-------------------------------------------------------------------------------------------
bs = 'kafka1:9092'
###TOPIC DI KAFKA
topic_name = 'twitterStream'

###CREAZIONE FUNZIONE DI PREPROCESSING-------------------------------------------------------------------------------------------
def preprocess(x):
    #converti in minuscolo
    x = x.lower()
    #tokenize
    tokens = nltk.word_tokenize(x)
    tokens_x =  [w for w in tokens if w.isalpha()]   
    #stopwords
    stop_x = [word for word in tokens_x if not word in stopwords]
    #remove "rt"
    no_rt_x = [word for word in stop_x if not re.search(r'\brt\b', word) 
               and not re.search(r'http\S+', word) and word != 'message']
    #stemming
    stemmer = PorterStemmer()
    stemm_x = [stemmer.stem(word) for word in no_rt_x]
    #restore sentences
    return ( " ".join(stemm_x))


###FUNZIONE PER LA SENT. ANALYSIS CON TEXTBLOB-------------------------------------------------------------------------------------------
def analyze_sentiment(text):
    # Calcola il sentiment del testo utilizzando TextBlob
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity

    # Restituisci il risultato in base al valore di sentiment
    if sentiment > 0:
        return "POSITIVE"
    elif sentiment < 0:
        return "NEGATIVE"
    else:
        return "NEUTRAL"

###FUNZIONE PER LA SCRITTURA IN MONGODB-------------------------------------------------------------------------------------------
def write_row_in_mongo2(batch_df, batch_id):
    # Connessione al database MongoDB
    client = pymongo.MongoClient("mongodb://mongodb:27017")
    db = client.twitter
    collection = db.TWEET

    # Converte il DataFrame in un dizionario
    rows = batch_df.toJSON().map(lambda j: json.loads(j)).collect()

    # Inserisce ogni riga come documento individuale
    for row in rows:
        collection.insert_one(row)
#-------------------------------------------------------------------------------------------

if __name__ == "__main__":
    findspark.init()
        
    #Config
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.mongodb.uri", "mongodb://mongodb:27017/twitter.TWEET") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
    
    ###SPARK CONTEXT
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')
    
    ###STREAM SCHEMA
    schema = StructType([StructField("message", StringType())])
    
    ###LETTURA DATI DA KAFKA A SPARK
    df = spark \
         .readStream \
         .format("kafka") \
         .option("kafka.bootstrap.servers", bs) \
         .option("subscribe", topic_name) \
         .option("startingOffsets", "latest") \
         .option("header", "true") \
         .load() \
         .selectExpr("CAST(value AS STRING) as message")
    
    
    df = df.withColumn("raw_data", from_json("message", schema))
    df = df.withColumn("timestamp", current_timestamp())
    ###PREPR. FUNCT.-------------------------------------------------------------------------------------------
    preprocess_udf = udf(preprocess)
    df = df.withColumn("cleaned_data", preprocess_udf(df.message)).dropna()
    words_df = df.select(explode(split("cleaned_data"," ")).alias("word"))
    counts_df = words_df.groupBy("word").count()
    ###APPL. SENTIMENT FUNCT.-------------------------------------------------------------------------------------------
    sentiment_udf = udf(lambda text: analyze_sentiment(text), StringType())
    df = df.withColumn("sentiment", sentiment_udf(df.cleaned_data))
    sents_df = df.select("sentiment").groupBy("sentiment").count()
    ###WATERMARK-------------------------------------------------------------------------------------------
    df = df.withWatermark("timestamp", "10 minutes")
    
    ###------------------------------------------------------------------------------------------- 
    # Visualizza il contenuto del DataFrame in streaming
    #query = df.writeStream \
     #   .outputMode("append") \
      #  .format("console") \
       # .start()

    # Attendi la terminazione della query
    #query.awaitTermination()
    ###------------------------------------------------------------------------------------------- 
     
    ###UTILIZZO FUNZIONE PER SCRITTURA IN MONGODB-------------------------------------------------------------------------------------------
    MNGquery = df.writeStream.queryName("tweets") \
        .foreachBatch(write_row_in_mongo2).start()

    ###------------------------------------------------------------------------------------------- 
    # Load WC
    word_count_query = counts_df.writeStream \
         .outputMode("complete") \
         .format("memory") \
         .queryName("WORDQ") \
         .option("path", "results") \
         .option("checkpointLocation", "checkpoint") \
         .option("forceDeleteTempCheckpointLocation", "true") \
         .trigger(processingTime="30 seconds") \
         .start() 
    
    #frequenza di scrittura con trigger
    
    # Load SA
    sentiment_query = sents_df.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("SENTQ") \
        .option("path", "results_sa") \
        .option("checkpointLocation", "checkpoint_sa") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .trigger(processingTime="30 seconds") \
        .start() 
    
###-------------------------------------------------------------------------------------------    
    while True:        
        # Word Cloud
        word_count_data = spark.sql("SELECT * FROM WORDQ").toPandas()
        if len(word_count_data) > 0:
            wordcloud = WordCloud(width=800,height=400).generate_from_frequencies(word_count_data.set_index('word')['count'])
            plt.subplot(121)
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.title("Word Cloud")
        else:
            plt.subplot(121)
            plt.text(0.5, 0.5, 'LOADING...', horizontalalignment='center', verticalalignment='center', fontsize=12, color='gray')
            plt.axis('off')

        # Sentiment Analysis
        sentiment_data = spark.sql("SELECT * FROM SENTQ").toPandas()
        if len(sentiment_data) > 0:
            sentiment_counts = sentiment_data.set_index('sentiment')['count']
            labels = sentiment_counts.index.tolist()
            values = sentiment_counts.values.tolist()
            plt.subplot(122)
            plt.pie(values, labels=labels, autopct='%1.1f%%')
            plt.title("Sentiment Analysis")
        else:
            plt.subplot(122)
            plt.text(0.5, 0.5, 'LOADING...', horizontalalignment='center', verticalalignment='center', fontsize=12, color='gray')
            plt.axis('off')
        
        # Display the plots
        clear_output(wait=True)
        plt.tight_layout()
        plt.show()
        
        time.sleep(5)  # Wait for 5 seconds before checking again
