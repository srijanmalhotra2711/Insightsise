#Installing pyspark
# !pip install pyspark

#Importing required libraries
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import StopWordsRemover
from wordcloud import WordCloud, STOPWORDS
import os
import pandas as pd
import matplotlib.pyplot as plt
import io
import urllib.parse
import base64

import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')

#Initializing spark session
conf = SparkConf().set("spark.ui.port", "4050")
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

#Function: Getting Sentiment Score of reviews
def get_sentiment(review_text):
  #Initalizing Sentiment Intensity Analyzer
  sia = SentimentIntensityAnalyzer()
  sia_score = sia.polarity_scores(review_text)
  if sia_score['compound']>0.25:
    sentiment = 'Positive'
  elif sia_score['compound']<-0.25:
    sentiment = 'Negative'
  else:
    sentiment = 'Neutral'
  return sentiment

#Defining a user defined function to apply get_sentiment function on Review column
udf_sentiment_score = udf(lambda x:get_sentiment(x), StringType())

#Function: Load the spark dataframe from company data file
def load_blind_data(company_name):
  #Conversion of company_name according to our filenames in dataset
  data_filename = company_name.lower().replace(" ", "-") + "-data.csv"
  directory = 'Blind Data'
  f = os.path.join(directory, data_filename)
  if os.path.isfile(f):
    company_df = spark.read.csv(f, header=True, inferSchema=True).withColumn('Review No', monotonically_increasing_id()+1).select('Review No',concat('Description', lit('. '), 'Pros', lit('. '), 'Cons').alias('Review')).dropna()
#   print('\nThe final data contains', company_df.count(),'reviews for',company_name)

  company_df = company_df.select('*', udf_sentiment_score(col('Review')).alias('Review Sentiment'))

  return company_df

#Function: Calculating Positive, Negative, Neutral reviews for the specific company
def company_review_analysis(company_df):
  analysis_df = company_df.groupby('Review Sentiment').count().withColumnRenamed('count', 'Number of Reviews')
  return analysis_df

#Function: Getting WordCloud from a dataframe
def get_wordcloud(dataframe_name):
  #Lower casing review text and adding a column for review tag words
  data_words = dataframe_name.select("*", lower('Review'))
  data_words = data_words.withColumn("review_words", regexp_replace("lower(Review)",r"""[!\"#$%&'()*+,\-.\/:;<=>?@\[\\\]^_`{|}~]"""," ")).drop('lower(Review)')
  data_words = data_words.select("*",split(col("review_words")," ")).drop('review_words').withColumnRenamed('split(review_words,  , -1)','review_words')

  #Removing Stopwords from tag words
  stopwords_remover = StopWordsRemover().setInputCol("review_words").setOutputCol("words")
  blind_data_words = stopwords_remover.transform(data_words).drop('review_words')

  #Creating a list of all the words in the reviews
  review_words = []
  blind_datal = blind_data_words.collect()
  for i in range(len(blind_datal)):
    for each_word in blind_datal[i][3]:
      review_words.append(each_word)

  #Creating a pandas dataframe of all the words in review
  word_count = pd.DataFrame({'words':review_words})
  review_text = " ".join(i for i in word_count.words)
  review_cloud = WordCloud(stopwords=STOPWORDS, background_color='white').generate(review_text)

  return review_cloud

#Function: To get base64 image from wordcloud object
def wordcloud_image(company_reviews_cloud, company_name):
  plt.figure(figsize=(15,10))
  plt.imshow(company_reviews_cloud, interpolation="bilinear")
  wc_title = "Blind Reviews' WordCloud for " + company_name
  plt.title(wc_title, fontsize=20)
  plt.axis("off")
  company_cloud_image = io.BytesIO()
  plt.savefig(company_cloud_image, format="png")
  company_cloud_image.seek(0)
  string = base64.b64encode(company_cloud_image.read())
  image_64 = "data:image/png;base64," +   urllib.parse.quote_plus(string)
  return image_64

#Function: To get final analysis output for every company
def get_blind_analysis(company_name):
  
  #Loading the spark dataframe for the specific company
  company_df = load_blind_data(company_name)

  #Calculating Positive, Negative, Neutral reviews for the specific company
  company_analysis_df = company_review_analysis(company_df)

  #Get wordcloud of reviews from the 
  company_reviews_cloud = get_wordcloud(company_df)

  #Get base64 image from wordcloud object
  company_cloud_image = wordcloud_image(company_reviews_cloud, company_name)

  return company_df, company_analysis_df, company_cloud_image

#Getting all visualization about the input company
# inp_company_name = input("\nEnter Company Name: ")
# company_df, company_analysis_df, company_cloud_image = get_blind_analysis(inp_company_name)

# company_df.show()
# company_analysis_df.show()