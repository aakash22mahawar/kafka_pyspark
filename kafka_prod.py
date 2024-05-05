from random_name import random_api
from kafka import KafkaProducer
import json
import time


def create_kafka_producer():
    """
    Creates the Kafka producer object
    """

    return KafkaProducer(bootstrap_servers=['localhost:9092'])


def start_streaming(api_url):
    """
    Writes the API data every 5 seconds to Kafka topic random_name
    """
    producer = create_kafka_producer()
    kafka_item = random_api(api_url)

    end_time = time.time() + 600  # the script will run for 10 minutes

    try:
        while time.time() < end_time:
            kafka_item = random_api(api_url)
            if kafka_item:
                producer.send("random_name", json.dumps(kafka_item).encode('utf-8'))
                time.sleep(5)
    except Exception as e:
        print("Error:", e)
    finally:
        producer.close()


if __name__ == "__main__":
    api_url = 'https://randomuser.me/api/?results=1'
    start_streaming(api_url)
