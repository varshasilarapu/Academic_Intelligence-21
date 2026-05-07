from kafka import KafkaProducer
import requests
import json

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Enter Roll Number
roll_no = input("Enter Roll Number: ")

# API URL
url = f"https://info.aec.edu.in/adityaapi/api/studentdata/{roll_no}"

try:
    response = requests.get(url, verify=False)

    data = response.json()

    producer.send("student_topic", data)

    producer.flush()

    print("✅ Data sent to Kafka successfully")

except Exception as e:
    print("❌ Error:", e)
