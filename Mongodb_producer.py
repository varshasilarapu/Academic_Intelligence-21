from pymongo import MongoClient
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import json

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))

db = client["varshini"]

collection = db["student_collection"]

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_SERVER"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Sending MongoDB data to Kafka...\n")

students = collection.find()

for student in students:

    filtered_data = {

        "student_id": str(student.get("_id")),

        "studentname":
            student.get("first_name", "N/A"),

        "rollno":
            student.get("roll_no", "N/A"),

        "gender":
            student.get("gender", "N/A"),

        "college":
            student.get("college", "N/A"),

        "branch":
            ",".join(student.get("branch", [])),

        "passout_year":
            student.get("passout_year", 0),

        "dob":
            student.get("dob", "N/A"),

        "section":
            ",".join(student.get("section", [])),

        "backlogs":
            student.get("backlogs", 0),

        "btech":
            student.get("btech", 0)
    }

    producer.send("mongodbtopic", filtered_data)

    producer.flush()

    print(f"✅ Sent {filtered_data['rollno']}")
