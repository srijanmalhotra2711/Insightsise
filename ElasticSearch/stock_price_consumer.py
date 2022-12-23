from kafka import KafkaConsumer
import json
from datetime import datetime
consumer = KafkaConsumer('test_topic2', bootstrap_servers='192.168.0.146:9092', group_id=None)
from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')

print('Listening')

for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    print(data)
    for element in data:
        es.index(
         index='stocks',
         body={
          'name': element['name'],
          'price': element['price'],
          'timestamp': datetime.strptime(element['timestamp'], '%Y-%m-%d %H:%M:%S.%f%z')
         })
