import sys
import json
import random
from faker import Faker
from confluent_kafka import Producer
import argparse

class RecordGenerator(object):
    # generate data used as input for Kafka

    def __init__(self):
        self.userid = 0
        self.channelid = 0
        self.genre = ""
        self.lastactive = None
        self.title = ""
        self.watchfrequency = 0
        self.etags = None

    def get_netflixrecords(self, fake):
        # generates fake metrics
        netflixdata = {
            'userid': fake.uuid4(),
            'channelid': fake.pyint(min_value=1, max_value=50),
            'genre': random.choice(['thriller', 'comedy', 'romcom', 'fiction']),
            'lastactive': fake.date_time_between(start_date='-10m', end_date='now').isoformat(),
            'title': fake.name(),
            'watchfrequency': fake.pyint(min_value=1, max_value=10),
            'etags': fake.uuid4()
        }
        return netflixdata

    def get_netflixrecord(self, rate, fake):
        return [self.get_netflixrecords(fake) for _ in range(rate)]

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():

    parser = argparse.ArgumentParser(description='Faker based streaming data generator')
    parser.add_argument('--bootstrap-servers', action='store', dest='bootstrap_servers',
                        help='Provide Kafka bootstrap servers (comma-separated)')
    parser.add_argument('--topic', action='store', dest='topic', help='Provide Kafka topic name')

    args = parser.parse_args()

    try:
        fake = Faker()
        conf = {
            'bootstrap.servers': args.bootstrap_servers
        }
        producer = Producer(conf)
        rate = 100  # rate at which data is generated

        generator = RecordGenerator()

        # generate data
        while True:
            fake_data = generator.get_netflixrecord(rate, fake)
            for data in fake_data:
                producer.produce(args.topic, json.dumps(data), callback=delivery_report)
                producer.poll(0)  # Trigger delivery reports for sent messages

    except KeyboardInterrupt:
        sys.stderr.write('Aborted by user\n')
    except Exception as e:
        print(f"Error: {str(e)}")

    producer.flush()  # Wait for any outstanding messages to be delivered
    sys.exit()

if __name__ == "__main__":
    main()
