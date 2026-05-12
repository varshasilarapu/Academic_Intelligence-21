🎓 Student 360 — Academic Intelligence Platform
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Microsoft Copilot Studio](https://img.shields.io/badge/Microsoft%20Copilot%20Studio-0078D4?style=for-the-badge&logo=microsoft&logoColor=white)
---
📌 Project Overview
Student 360 is a real-time academic intelligence platform designed to unify multi-source university data, monitor student performance holistically, and proactively detect dropout risks using machine learning. The platform integrates data engineering, analytics, data science, and AI-powered natural language access into a single end-to-end solution.
> **Institution:** Aditya Engineering College  
> **Domain:** EdTech / Academic Analytics  
> **Type:** Real-Time Data Pipeline + ML + AI Chatbot
---
🔴 Problem Statement
Universities store academic, attendance, financial, and placement data in disconnected systems, making it extremely difficult to:
Monitor student performance holistically across departments
Detect dropout risks and academic decline before it is too late
Enable data-driven decisions by faculty, advisors, and administrators
Provide students with transparent, real-time visibility into their own performance
The lack of a centralized, intelligent platform means interventions happen reactively rather than proactively, leading to higher dropout rates and poor placement outcomes.
---
✅ Solution
We built a real-time Student 360 Analytics Platform that:
Integrates multi-source university data (MongoDB + REST APIs) using Apache Kafka pipelines into Snowflake
Transforms raw data into unified student profiles using dbt (Data Build Tool) and SQL inside Snowflake
Orchestrates the entire ELT pipeline using Apache Airflow DAGs for automated scheduling and monitoring
Analyzes historical and current student data using SQL-based analytics and Power BI dashboards
Predicts future student performance and dropout risk using Machine Learning (Random Forest, Logistic Regression)
Enables natural language access to student data via an AI-powered chatbot built on Microsoft Copilot Studio, with role-based access control (Admin vs Student)
---
🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│   MongoDB (Student Records)    REST API (Attendance & Marks)        │
└───────────────┬─────────────────────────┬───────────────────────────┘
                │                         │
                ▼                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     APACHE KAFKA (Message Broker)                     │
│         Topic: mongodb-student-data    Topic: api-student-data       │
└───────────────┬─────────────────────────┬───────────────────────────┘
                │                         │
                ▼                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        SNOWFLAKE (Data Warehouse)                     │
│                                                                       │
│  RAW LAYER           TRANSFORM LAYER          ANALYTICS LAYER         │
│  ─────────           ────────────────         ───────────────         │
│  STG_MONGODB    ──►  STUDENT_UNIFIED   ──►   DROPOUT_RISK_SCORE      │
│  STG_API        ──►  (dbt models)      ──►   PERFORMANCE_SUMMARY     │
│                                                                       │
└───────────────┬──────────────────────────┬───────────────────────────┘
                │                          │
                ▼                          ▼
┌───────────────────────┐    ┌─────────────────────────────────────────┐
│   APACHE AIRFLOW       │    │         ANALYTICS & ML LAYER            │
│   (Orchestration)      │    │                                         │
│   - Pipeline DAGs      │    │  Power BI Dashboards                    │
│   - Scheduling         │    │  ML Models (Dropout Prediction)         │
│   - Monitoring         │    │  Performance Analytics                  │
└───────────────────────┘    └──────────────┬──────────────────────────┘
                                             │
                                             ▼
                              ┌──────────────────────────────┐
                              │  AI CHATBOT (Copilot Studio)  │
                              │  - Admin: All student data    │
                              │  - Student: Own data only     │
                              │  - Natural language queries   │
                              └──────────────────────────────┘
```
---
🛠️ Technology Stack
Layer	Technology	Purpose
Data Ingestion	Apache Kafka	Real-time message streaming from MongoDB and REST APIs
Data Storage	Snowflake	Cloud data warehouse for raw and transformed data
Data Transformation	dbt (Data Build Tool)	SQL-based transformation, testing, and documentation
Orchestration	Apache Airflow	Pipeline scheduling, DAG management, and monitoring
Source 1	MongoDB	Student academic and profile records
Source 2	REST API	Real-time attendance and marks data
Analytics	SQL + Power BI	Historical analysis and interactive dashboards
Machine Learning	Python (Scikit-learn)	Dropout risk prediction and performance forecasting
AI Chatbot	Microsoft Copilot Studio	Natural language interface for students and admins
Language	Python 3.10+	All pipeline and ML code
---
📁 Project Structure
```
student360/
│
├── kafka/
│   ├── mongodb_producer.py        # Streams MongoDB student data to Kafka
│   ├── mongodb_consumer.py        # Consumes MongoDB data → writes to Snowflake
│   ├── api_producer.py            # Streams REST API data to Kafka
│   └── api_consumer.py            # Consumes API data → writes to Snowflake
│
├── dbt/
│   ├── dbt_project.yml            # dbt project configuration
│   ├── profiles.yml               # Snowflake connection profile
│   └── models/
│       ├── staging/
│       │   ├── stg_mongodb.sql    # Cleans raw MongoDB data
│       │   └── stg_api.sql        # Cleans raw API data
│       ├── intermediate/
│       │   └── student_unified.sql  # Joins both sources into one table
│       └── marts/
│           ├── dropout_risk.sql     # Calculates dropout risk score
│           └── performance_summary.sql  # Academic performance metrics
│
├── airflow/
│   └── dags/
│       └── student_pipeline_dag.py  # Main orchestration DAG
│
├── ml/
│   ├── dropout_prediction.py      # ML model training and prediction
│   ├── feature_engineering.py     # Feature extraction from student data
│   └── model_evaluation.py        # Model accuracy and metrics
│
├── chatbot/
│   └── README_chatbot.md          # Copilot Studio setup documentation
│
├── snowflake/
│   └── create_tables.sql          # Snowflake table creation scripts
│
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment variables template
└── README.md                      # This file
```
---
⚙️ Setup & Installation
Prerequisites
Python 3.10+
Apache Kafka (local or cloud)
Snowflake account
Apache Airflow
dbt-snowflake installed
1. Clone the repository
```bash
git clone https://github.com/your-username/student360.git
cd student360
```
2. Install Python dependencies
```bash
pip install -r requirements.txt
```
3. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your Snowflake, Kafka, and MongoDB credentials
```
4. Run Kafka producers
```bash
# Terminal 1 - MongoDB producer
python kafka/mongodb_producer.py

# Terminal 2 - API producer
python kafka/api_producer.py
```
5. Run Kafka consumers
```bash
# Terminal 3 - MongoDB consumer
python kafka/mongodb_consumer.py

# Terminal 4 - API consumer
python kafka/api_consumer.py
```
6. Run dbt transformations
```bash
cd dbt
dbt run
dbt test
```
7. Start Airflow
```bash
airflow db init
airflow scheduler &
airflow webserver
```
---
🗄️ Snowflake Data Model
Table 1: STG_MONGODB (Raw student profiles from MongoDB)
Column	Type	Description
STUDENT_ID	VARCHAR	Unique student identifier
NAME	VARCHAR	Student full name
DEPARTMENT	VARCHAR	Department/Branch
YEAR	NUMBER	Current academic year
CGPA	FLOAT	Cumulative GPA
BACKLOGS	NUMBER	Number of backlogs
Table 2: STG_API (Raw attendance and marks from REST API)
Column	Type	Description
STUDENT_ID	VARCHAR	Unique student identifier
SUBJECT	VARCHAR	Subject name
MARKS	FLOAT	Marks obtained
ATTENDANCE_PCT	FLOAT	Attendance percentage
EXAM_TYPE	VARCHAR	Internal / External
Table 3: STUDENT_UNIFIED (Joined master table)
Column	Type	Description
STUDENT_ID	VARCHAR	Unique student identifier
NAME	VARCHAR	Student name
DEPARTMENT	VARCHAR	Department
CGPA	FLOAT	Overall CGPA
AVG_MARKS	FLOAT	Average marks across subjects
ATTENDANCE_PCT	FLOAT	Overall attendance percentage
BACKLOGS	NUMBER	Number of backlogs
DROPOUT_RISK_SCORE	FLOAT	ML-calculated risk score (0-1)
RISK_CATEGORY	VARCHAR	LOW / MEDIUM / HIGH
---
🤖 Machine Learning — Dropout Risk Prediction
Model Details
Algorithm: Random Forest Classifier + Logistic Regression (ensemble)
Target Variable: Dropout Risk (Low / Medium / High)
Features Used:
CGPA
Attendance percentage
Number of backlogs
Average marks
Department
Academic year
Model Performance
Metric	Score
Accuracy	87%
Precision	85%
Recall	83%
F1 Score	84%
---
💬 AI Chatbot — Microsoft Copilot Studio
The chatbot is built using Microsoft Copilot Studio integrated with Snowflake via Power Automate.
Role-Based Access Control
Role	Access
Admin	View all students' marks, attendance, predictions, risk scores
Student	View only their own marks, attendance, and predictions
Sample Conversations
```
Student: "What are my marks?"
Bot: "Here are your marks: Math 85, Science 90, English 78. Overall Grade: B+"

Student: "What is my attendance?"
Bot: "Your attendance this semester is 87%. You have 6 absences."

Student: "Will I pass this semester?"
Bot: "Based on your performance, your predicted outcome is: PASS (82% probability)"

Admin: "Show me all at-risk students"
Bot: "15 students are currently HIGH risk. Top 3: [Student A, Student B, Student C]"
```
---
📊 Analytics Dashboards (Power BI)
The platform provides the following dashboards:
Student Performance Dashboard — Individual marks, CGPA trend, subject-wise analysis
Attendance Analytics — Department-wise attendance heatmap, absentee alerts
Dropout Risk Dashboard — Risk score distribution, high-risk student list
Placement Readiness — CGPA vs placement eligibility analysis
Department Overview — Comparative performance across departments
---
🔄 Apache Airflow — Pipeline Orchestration
The main DAG (`student_pipeline_dag.py`) runs the following tasks in sequence:
```
start
  │
  ├── extract_mongodb_data
  │       │
  │       └── kafka_produce_mongodb
  │               │
  │               └── kafka_consume_mongodb → load_to_snowflake_stg1
  │
  ├── extract_api_data
  │       │
  │       └── kafka_produce_api
  │               │
  │               └── kafka_consume_api → load_to_snowflake_stg2
  │
  └── [after both above complete]
          │
          └── run_dbt_transformations
                  │
                  └── run_ml_predictions
                          │
                          └── update_dropout_risk_scores → end
```
Schedule: Runs every 6 hours automatically
---
🚀 Future Scope
Mobile App — React Native app for students to access their data on mobile
Real-time Alerts — SMS/email alerts to faculty when a student becomes high-risk
Parent Portal — Secure parent access to their child's academic data
Advanced ML — Deep learning models for more accurate predictions
Multi-institution Support — Scale platform across multiple universities
Integration with ERP — Connect with college ERP systems (fees, hostel, library)
Recommendation Engine — AI-powered subject and career recommendations
---
👥 Team Members
Name	Role
[Team Member 1]	Data Engineering (Kafka + Snowflake)
[Team Member 2]	Data Transformation (dbt + SQL)
[Team Member 3]	Machine Learning (Dropout Prediction)
[Team Member 4]	AI Chatbot (Copilot Studio)
[Team Member 5]	Analytics & Power BI Dashboards
---
📄 License
This project is developed as part of an academic project at Aditya Engineering College.
---
🙏 Acknowledgements
Apache Kafka, Snowflake, dbt, Apache Airflow open-source communities
Microsoft Copilot Studio documentation
Scikit-learn Machine Learning library
Our faculty mentors and project guides at Aditya Engineering College
