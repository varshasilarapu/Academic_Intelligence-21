from kafka import KafkaConsumer
import snowflake.connector
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka Consumer
consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Snowflake Connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

print("Waiting for Kafka data...\n")

for message in consumer:

    data = message.value

    student = data[0]

    studentname = student.get("studentname")
    rollno = student.get("rollno")
    branch = student.get("branch")
    overallpercent = student.get("overallpercent")
    mobilenumber = student.get("mobilenumber")

    query = """
    INSERT INTO students
    (studentname, rollno, branch, overallpercent, mobilenumber)
    VALUES (%s, %s, %s, %s, %s)
    """

    cursor.execute(query, (
        studentname,
        rollno,
        branch,
        overallpercent,
        mobilenumber
    ))

    conn.commit()

    print("✅ Data inserted into Snowflake")
