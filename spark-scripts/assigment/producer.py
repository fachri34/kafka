import json
import uuid
import os
import json
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep
from datetime import datetime, timedelta
import random

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
faker = Faker()


car_brands_and_models = {
    "Toyota": ["Corolla", "Camry", "RAV4"],
    "Honda": ["Civic", "Accord", "CR-V"],
    "Ford": ["Focus", "Mustang", "Explorer"],
    "BMW": ["3 Series", "5 Series", "X5"],
    "Mercedes": ["A-Class", "C-Class", "E-Class"]
}

class DataGenerator:
    @staticmethod
    def get_data():
        now = datetime.now()
        
        brand = random.choice(list(car_brands_and_models.keys()))
        model = random.choice(car_brands_and_models[brand])
        car_name = f"{brand} {model}"
        
        return [
            uuid.uuid4().__str__(),
            faker.random_int(min=1, max=100),
            car_name,
            faker.safe_color_name(),
            faker.random_int(min=10000, max=100000),
            faker.unix_time(
                start_datetime=now - timedelta(minutes=60), end_datetime=now
            ), 
        ]

while True:
    columns = [
        "order_id",
        "customer_id",
        "car_name",
        "color",
        "price",
        "ts",
    ]
    data_list = DataGenerator.get_data()
    json_data = dict(zip(columns, data_list))
    _payload = json.dumps(json_data).encode("utf-8")
    print(_payload, flush=True)
    print("=-" * 5, flush=True)
    response = producer.send(topic=kafka_topic, value=_payload)
    print(response.get())
    print("=-" * 20, flush=True)
    sleep(3)