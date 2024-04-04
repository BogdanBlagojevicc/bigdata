FROM python:3.9

WORKDIR /usr/src/app

COPY . .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENV API_URL="http://api.weatherapi.com/v1/current.json"
ENV TOPIC="weather_raw"
ENV KAFKA_BROKER="broker1:19092"

CMD ["python3", "-u", "./producer.py"]