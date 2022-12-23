import tweepy
import time
from kafka import KafkaProducer
import threading
from yahoo_fin import stock_info
import yfinance as yf
import re
import time
import datetime
import json
from pytz import timezone
tz = timezone('EST')
from yahoo_fin import stock_info
company_list = ["ADBE", "AAPL", "ABNB", "AMZN",
     "TEAM", "CSCO", "GS", "GOOGL", "IBM", "INTC", "META", "MSFT", "NFLX",
     "ORCL", "CRM", "UBER", "WMT"]
import json
producer = KafkaProducer(bootstrap_servers='192.168.0.146:9092',api_version=(0,1,0))
class StreamListener(tweepy.StreamListener):
  def on_status(self, status):
    print(type(status)
    if not status.retweeted and 'RT @' not in status.text:
        data = status._json
        producer.send(topic='test_topic1', value=json.dumps(data, default = str).encode('utf-8'), timestamp_ms=time.time())
    else:
        print("This is a re-tweeted message!!! Ignore it...")
  def on_error(self, status_code):
    if status_code == 420:
      return False
def twitter():
    consumer_key = ''
    consumer_secret = ''
    access_token = ''
    access_secret = ''
    auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_secret)
    api = tweepy.API(auth)
    # searched_tweets = [status._json for status in tweepy.Cursor(api.search_tweets, q="Microsoft").items(10)]
    auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_secret)
    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=auth, listener=stream_listener)
    stream.filter(languages=["en"],track=["layoff", "$ADBE", "$AAPL", "$ABNB", "$AMZN",
     "$TEAM", "$CSCO", "$GS", "$GOOGL", "$IBM", "$INTC", "$META", "$MSFT", "$NFLX",
     "$ORCL", "$CRM", "$TWTR", "$UBER", "$WMT"])
    for tweet in searched_tweets:
        producer.send(topic='test_topic1', value=bytes(tweet['text'], "UTF-8"), timestamp_ms=time.time())

    producer.flush()
def stock():
    while True:
        list_prices = []
        for company in company_list:
            data = {}
            data['name'] = company
            data['price'] = stock_info.get_live_price(company)
            data['timestamp'] = datetime.datetime.now(tz=tz)
            list_prices.append(data)
        producer.send(topic='test_topic2', value=json.dumps(list_prices, default = str).encode('utf-8'), timestamp_ms=time.time())
        time.sleep(1)
        producer.flush()
def main():
    t1 = threading.Thread(target=twitter)
    t1.start()
    t2 = threading.Thread(target=stock)
    t2.start()

if __name__ == "__main__":
    main()

def clean_text(text):
    # clean up text
    text = text.replace("\n", " ")
    text = re.sub(r"https?\S+", "", text)
    text = re.sub(r"&.*?;", "", text)
    text = re.sub(r"<.*?>", "", text)
    text = text.replace("RT", "")
    text = text.replace(u"…", "")
    text = text.strip()
    return text


#
