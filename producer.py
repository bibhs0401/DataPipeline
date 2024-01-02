from confluent_kafka import Producer
import json
import time
import random
from faker import Faker

fake = Faker()

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}
# Kafka producer function
def produce():
    producer = Producer(conf)
    # Produce some dummy data to Kafka topic
    netflix_data = {
        'userid': fake.uuid4(),
        'channelid': fake.random_int(min=1, max=50),
        'genre': random.choice(['thriller', 'comedy', 'romcom', 'fiction']),
        'lastactive': fake.date_time_between(start_date='-10m', end_date='now').isoformat(),
        'title': fake.name(),
        'watchfrequency': fake.random_int(min=1, max=10),
        'etags': fake.uuid4()
    }
    producer.produce('netflix-data', value=json.dumps(netflix_data))
    print(netflix_data)
    time.sleep(5)
    producer.flush()
# Kafka consumer function
produce()