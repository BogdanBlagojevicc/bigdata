import json as json
import time
import os
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
import datetime
import uuid

load_dotenv()
api_url = os.environ.get("API_URL")
kafka_broker = os.environ["KAFKA_BROKER"]

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    api_version=(0, 11, 5),
    bootstrap_servers=[kafka_broker],
    value_serializer=serializer
)

def read_location_coordinates(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
        locations = []
        for group in data:
            for location in group:
                locations.append((location["topic"], location["name"], location["key"]))
        return locations
    
def get_weather_data(api_url, params):
    response = requests.get(api_url, params=params)
    return response.json()


def fetch_weather():
        filename = "location_coordinates.json"
        locations = read_location_coordinates(filename)
        for topic, name, key in locations:
            coordinates = {
                "key": key,
                "q" : name,
                "aqi" : "no"
            }
            weather_data = get_weather_data(api_url, coordinates)
            desired_keys = [
                "wind_mph", "wind_kph", "wind_degree", "wind_dir", 
                "pressure_mb", "pressure_in", "precip_mm", "precip_in", 
                "humidity", "cloud", "feelslike_c", "feelslike_f", 
                "vis_km", "vis_miles", "uv", "gust_mph", "gust_kph", "temp_c", "temp_f", "is_day",
            ]
            filtered_weather_data = {key: weather_data['current'].get(key) for key in desired_keys}

            debug = True
            if debug:
                print(f"Params: {coordinates}")
                print(f"Sending record to Kafka topics: {topic};")
                print(f"Record: {filtered_weather_data}")
                print(f"Timestamp: {datetime.datetime.now()}")
            producer.send(topic, filtered_weather_data)
            producer.flush()


def main():
    while True:
        fetch_weather()
        time.sleep(900) #15min


if __name__ == "__main__":
    main()

