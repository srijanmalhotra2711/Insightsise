from elasticsearch import Elasticsearch
from dateutil.parser import parse
import dateutil
import yfinance as yf
es = Elasticsearch('http://localhost:9200')
# print(es)
mappings = {
    "mappings": {
            "properties":{
                   "name": {
                        "type": "text"
                    },
                       "price": {
                        "type": "float"
                    },
                       "timestamp": {
                        "type": "date",
                         "format": "yyyy-MM-dd HH:mm:ss.SSSSSSZZZZZ"
                    }
            }

    }
}

mappings1 = {
    "mappings": {
            "properties":{
                   "name": {
                        "type": "text"
                    },
                    "text": {
                        "type": "text"
                    },
                    "timestamp": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss.SSSSSSZZZZZ"
                    },
                    "sentiment":{
                        "type": "text"
                    },
                    "followers_count":{
                        "type": "integer"
                    },
                    "is_verified": {
                        "type": "boolean"
                    }
            }
    }
}

mappings2 = {
    "mappings": {
            "properties":{
                   "name": {
                        "type": "text"
                    },
                    "review": {
                        "type": "text"
                    },
                    "sentiment":{
                        "type": "text"
                    }
            }
    }
}

es.indices.create(index="stocks", body=mappings)
es.indices.create(index="tweets", body=mappings1)
es.indices.create(index="reviews", body=mappings2)
#Test index creation using following example entries
# es.index(
#  index='tweets',
#  document={
#   'name': 'AAPL',
#   'text': 'yo',
#   'timestamp': '2022-12-14 13:45:04',
#   "followers_count": 20,
#   "sentiment": "Positive",
#   "verified": True
#  })
#
# es.index(
#  index='stocks',
#  document={
#   'name': 'META',
#   'timestamp': timestamp,
#   'price': yca.iloc[0]['Close']
#  })
res = es.search(body={"query": {"match_all": {}}}, index = 'reviews')
res = es.count(body={"query": {"match_all": {}}}, index = 'stocks')['count']
#
#
print(res)
