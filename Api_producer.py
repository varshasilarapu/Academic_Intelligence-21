from kafka import KafkaProducer
from dotenv import load_dotenv
import snowflake.connector
import requests
import json
import os
import urllib3


load_dotenv()

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_SERVER"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Snowflake Connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse='COMPUTE_WH',
    database='ACADEMIC_INTELLIGENCE',
    schema='PUBLIC'
)

cursor = conn.cursor()

# Get ALL roll numbers
cursor.execute("""
SELECT DISTINCT rollno
FROM mongodb_students
WHERE rollno IS NOT NULL
""")

roll_numbers = [row[0] for row in cursor.fetchall()]

# APIs
apis = {
    "leetcode": "https://maya.technicalhub.io/node/api/get-leetcode-details-by-rollno",
    "gfg": "https://maya.technicalhub.io/node/api/get-geeksforgeeks-details-by-rollno",
    "codechef": "https://maya.technicalhub.io/node/api/get-codechef-details-by-rollno",
    "hackerrank": "https://maya.technicalhub.io/node/api/get-hackerrank-details-by-rollno"
}

print("Sending API data to Kafka...\n")

for roll in roll_numbers:

    for platform, url in apis.items():

        body = {
            "roll_no": roll
        }

        try:

            response = requests.post(
                url,
                json=body,
                verify=True
            )

            data = response.json()

            final_data = {

                "rollno": roll,

                "platform": platform,

                "attendance_percentage": 0,

                "total_problems":
                    data.get("lc_total_progarms") or
                    data.get("total_problems") or 0,

                "easy":
                    data.get("lc_easy") or
                    data.get("easy") or 0,

                "medium":
                    data.get("lc_medium") or
                    data.get("medium") or 0,

                "hard":
                    data.get("lc_hard") or
                    data.get("hard") or 0,

                "rank":
                    data.get("lc_rank") or
                    data.get("rank") or 0,

                "streak":
                    data.get("lc_streak") or
                    data.get("streak") or 0,

                "weekly_solved":
                    data.get("lc_weekly_solved") or
                    data.get("weekly_solved") or 0,

                "profile_url":
                    data.get("lc_profile") or
                    data.get("profile_url") or
                    "N/A",

                "raw_data": data
            }

            producer.send("apitopic", final_data)

            producer.flush()

            print(f"✅ Sent {roll} - {platform}")

        except Exception as e:

            print(f"❌ Error for {roll}: {e}")
