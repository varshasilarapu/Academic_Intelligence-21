from kafka import KafkaConsumer
import snowflake.connector
from dotenv import load_dotenv
import os
import json

load_dotenv()

consumer = KafkaConsumer(
    'mongodbtopic',
    bootstrap_servers=os.getenv("KAFKA_SERVER"),
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse='COMPUTE_WH',
    database='ACADEMIC_INTELLIGENCE',
    schema='PUBLIC'
)

cursor = conn.cursor()

print("Receiving MongoDB Kafka data...\n")

for message in consumer:

    data = message.value

    query = """
    INSERT INTO mongodb_students (
        student_id,
        studentname,
        rollno,
        gender,
        college,
        branch,
        passout_year,
        dob,
        section,
        backlogs,
        btech
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        data.get("student_id"),

        data.get("studentname"),
        data.get("rollno"),
        data.get("gender"),
        data.get("college"),
        data.get("branch"),
        float(data.get("passout_year", 0)),
        data.get("dob"),
        data.get("section"),
        float(data.get("backlogs", 0)),
        float(data.get("btech", 0))
    )

    cursor.execute(query, values)

    conn.commit()

    print(f"✅ Inserted {data.get('rollno')}")
