from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
import nltk
from datetime import datetime
from nltk.sentiment import SentimentIntensityAnalyzer
es = Elasticsearch('http://localhost:9200')
nltk.download('vader_lexicon')
from dateutil.parser import parse
import dateutil
consumer = KafkaConsumer('test_topic1', bootstrap_servers='192.168.0.146:9092', group_id=None)
print('Listening')

def get_sentiment(column_name):
  #Initalizing Sentiment Intensity Analyzer
  sia = SentimentIntensityAnalyzer()
  sia_score = sia.polarity_scores(column_name)
  if sia_score['compound']>0.25:
    sentiment = 'Positive'
  elif sia_score['compound']<-0.25:
    sentiment = 'Negative'
  else:
    sentiment = 'Neutral'
  return sentiment

for message in consumer:
    t = json.loads(message.value.decode('utf-8'))
    print(t)
    company_list = ["layoff", "ADBE", "AAPL", "ABNB", "AMZN",
     "TEAM", "CSCO", "GS", "GOOGL", "IBM", "INTC", "META", "MSFT", "NFLX",
     "ORCL", "CRM", "UBER", "WMT", "TSLA"]
    result = [ele for ele in company_list if ele in t['text']]
    if len(result) ==0:
        result.append("layoff")

    dt = parse(t['created_at'])
    timestamp = dt.strftime('%Y-%m-%d %H:%M:%S.%f%z')
    dt = dt.astimezone(dateutil.tz.gettz("America/New_York"))
    timestamp = "{0}:{1}".format(
    timestamp[:-2],
    timestamp[-2:]
)
    for company in result:
        body = {
        'name': company,
        'text': t['text'],
        'sentiment' : get_sentiment(t['text']),
        'followers_count': t['user']['followers_count'],
        'is_verified': t['user']['verified'],
        'timestamp':  timestamp
        }
        es.index(
         index='tweets',
         body=body)
