# log_sender.py
from kafka import KafkaProducer, KafkaConsumer
import json
import os

def log_sender(
    dir_path=os.path.abspath(os.getcwd()), 
    log_name = 'custom_sample_data.json',
    topic_name = 'test_topic'
):

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
#        bootstrap_servers=['ec2-15-164-250-21.ap-northeast-2.compute.amazonaws.com:9092'], 
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    log_path = '/'.join([dir_path, log_name])
    with open(log_path, "r") as file:
        logs = json.load(file)

    for log in logs:
        producer.send(topic_name, value=log)

    producer.flush()

log_sender()


