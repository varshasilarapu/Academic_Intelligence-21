from kafka import KafkaConsumer
import snowflake.connector
from dotenv import load_dotenv
import json
import os
import logging
import snowflake.connector

# Use logging instead of print
logging.basicConfig(level=logging.INFO)

def insert_to_snowflake(data):
    try:
        # Use context manager (the 'with' statement) to auto-close connections
        with snowflake.connector.connect(...) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO ...", (data))
                conn.commit()
                logging.info("Successfully inserted record.")
    except snowflake.connector.Error as e:
        logging.error(f"Snowflake error: {e}")
load_dotenv()

consumer = KafkaConsumer(
    'apitopic',
    bootstrap_servers=os.getenv("KAFKA_SERVER"),
    auto_offset_reset='latest',
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

print("Receiving API Kafka data...\n")

for message in consumer:

    data = message.value

    query = """
    INSERT INTO api_students (
        rollno,
        platform,
        attendance_percentage,
        total_problems,
        easy,
        medium,
        hard,
        rank,
        streak,
        weekly_solved,
        profile_url,
        raw_data
    )

    SELECT
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        PARSE_JSON(%s)
    """

    values = (
        data.get("rollno"),
        data.get("platform"),
        float(data.get("attendance_percentage", 0)),
        float(data.get("total_problems", 0)),
        float(data.get("easy", 0)),
        float(data.get("medium", 0)),
        float(data.get("hard", 0)),
        float(data.get("rank", 0)),
        float(data.get("streak", 0)),
        float(data.get("weekly_solved", 0)),
        data.get("profile_url"),
        json.dumps(data.get("raw_data"))
    )

    cursor.execute(query, values)

    conn.commit()

    print(f"✅ Inserted {data.get('rollno')} - {data.get('platform')}")
