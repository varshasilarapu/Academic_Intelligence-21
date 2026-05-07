from kafka import KafkaConsumer
import snowflake.connector
import json

# Kafka Consumer
consumer = KafkaConsumer(
    'student_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Snowflake Connection
conn = snowflake.connector.connect(
    user='VARSHA',
    password='Academicintelli@21',
    account='ba83678.ap-southeast-1',
    warehouse='COMPUTE_WH',
    database='ACADEMIC_INTELLIGENCE',
    schema='PUBLIC'
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
