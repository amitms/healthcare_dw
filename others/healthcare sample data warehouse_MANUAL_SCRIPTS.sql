/*
This project is a sample data warehousing project for a healthcare organization. It includes a star schema with a fact table and several dimension tables.
*/
-- core tables: strurcture is in 3NF 

CREATE TABLE Patients (
    patient_id SERIAL PRIMARY KEY,
    name VARCHAR2(100),
    dob DATE,
    gender VARCHAR2(10)
);
CREATE TABLE Doctors (
    doctor_id SERIAL PRIMARY KEY,
    name VARCHAR2(100),
    specialization VARCHAR2(100)
);
CREATE TABLE Visits (
    visit_id SERIAL PRIMARY KEY,
    patient_id INT,
    doctor_id INT,
    visit_date DATE,
    diagnosis VARCHAR2(255),
    FOREIGN KEY (patient_id) REFERENCES Patients(patient_id),
    FOREIGN KEY (doctor_id) REFERENCES Doctors(doctor_id)
);

CREATE TABLE Treatments (
    treatment_id SERIAL PRIMARY KEY,
    visit_id INT,
    treatment_type VARCHAR2(100),
    cost NUMERIC,
    FOREIGN KEY (visit_id) REFERENCES Visits(visit_id)
);

CREATE TABLE Billing (
    bill_id SERIAL PRIMARY KEY,
    visit_id INT,
    total_amount NUMERIC,
    insurance_provider VARCHAR2(100),
    FOREIGN KEY (visit_id) REFERENCES Visits(visit_id)
);

/*
Dimensional Modeling (Star Schema)
normalized OLTP → Analytics model
*/
CREATE TABLE Fact_Visits (
    visit_key SERIAL PRIMARY KEY,
    patient_key INT,
    doctor_key INT,
    date_key INT,
    total_cost NUMERIC
);
CREATE TABLE Dim_Patient (
    patient_key SERIAL PRIMARY KEY,
    patient_id INT,
    age INT,
    gender VARCHAR2(10)
);
CREATE TABLE Dim_Doctor (
    doctor_key SERIAL PRIMARY KEY,
    doctor_id INT,
    specialization VARCHAR2(100)
);
CREATE TABLE Dim_Date (
    date_key INT PRIMARY KEY,
    date DATE,
    month INT,
    year INT
);
CREATE TABLE Dim_Date (
    date_key INT PRIMARY KEY,
    date DATE,
    month INT,
    year INT
);

ETL Process: ETL pipeline
1. Extract: Data is extracted from the OLTP system (Patients, Doctors, Visits, Treatments, Billing).CSV/API/DB Sources
</> Python
import pandas as pd
patients = pd.read_csv('patients.csv')
visits = pd.read_csv('visits.csv')

2. Transform: cleand data, create surrgate keys, handle nullsData is transformed to fit the dimensional model.
patients['age'] = 2026 - pd.to_datetime(patients['dob']).dt.year

3. Load: Transformed data is loaded into the data warehouse.
</> Python
from sqlalchemy import create_engine
engine = create_engine('postgresql://user:password@localhost:5432/healthcare_dw')
patients.to_sql('Dim_Patient', engine, if_exists='append', index=False)

Orchestration with Apache Airflow 
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def etl():
    print("Run ETL")

dag = DAG("healthcare_dw", start_date=datetime(2024,1,1))

task = PythonOperator(
    task_id="run_etl",
    python_callable=etl,
    dag=dag
)
containeraizaion with Docker
</> Dockerfile
FROM python:3.10
WORKDIR /app
COPY . .
RUN pip install pandas sqlalchemy psycopg2 airflow
CMD ["python", "etl.py"]

CI/CD Pipeline with jenkins
</> groovy
pipeline {
    agent any

    stages {
        stage('Clone Repo') {
            steps {
                git 'https://github.com/your-repo/healthcare-dw.git'
            }
        }

        stage('Build Docker Image') {
            steps {
                sh 'docker build -t healthcare_dw .'
            }
        }

        stage('Run ETL') {
            steps {
                sh 'docker run healthcare_dw'
            }
        }

        stage('Deploy') {
            steps {
                echo 'Deploy to server/cloud'
            }
        }
    }
}

BI Dashboard ideas: Tableau/Power BI
Patient visits per month
Revenue by department
Doctor performance ranking
Insurance claim trends
Top diseases


CSV/API → Python ETL → Airflow DAG → PostgreSQL DW
            ↓
        Dockerized
            ↓
        Jenkins CI/CD
            ↓
        Power BI Dashboard

healthcare_dw/
│
├── README.md
├── LICENSE
├── .gitignore
│
├── sql/
│   ├── 01_create_oltp_tables.sql
│   ├── 02_create_staging_tables.sql
│   ├── 03_create_dimensional_tables.sql
│   ├── 04_etl_procedures.sql
│   ├── 05_test_queries.sql
│
├── data/
│   ├── patients.csv
│   ├── doctors.csv
│   ├── visits.csv
│   ├── treatments.csv
│   ├── billing.csv
│
├── etl/
│   ├── load_staging_patient.sql
│   ├── load_staging_doctor.sql
│   ├── load_staging_visits.sql
│   ├── load_staging_treatments.sql
│   ├── load_staging_billing.sql
│   └── run_etl_pipeline.sql
│
├── scripts/
│   ├── truncate_staging.sql
│   ├── validate_staging.sql
│   ├── merge_dim_patient.sql
│   ├── merge_dim_doctor.sql
│   └── merge_fact_visits.sql
│
├── airflow/
│   ├── dags/
│   │   └── healthcare_dw_dag.py
│   └── requirements.txt
│
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
│
└── jenkins/
    ├── Jenkinsfile
    └── README_Jenkins.md
Patients

------------------------------
sqlplus sys as sysdba  /texas
SHOW CON_NAME;
-- Switch to your PDB (example PDB name: orclpdb1)
ALTER SESSION SET CONTAINER = XEPDB1;


CREATE USER staging IDENTIFIED BY "texas" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
GRANT CREATE SESSION, CREATE TABLE TO staging;
CREATE USER oltp IDENTIFIED BY "texas" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
GRANT CREATE SESSION, CREATE TABLE TO oltp;
CREATE USER dw IDENTIFIED BY "texas" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
GRANT CREATE SESSION, CREATE TABLE TO dw;

GRANT CREATE ANY TABLE TO amit;
GRANT CREATE ANY INDEX TO amit;

alter user oltp quota unlimited on users;

alter user oltp quota unlimited on users;


-- 1. Configure the environment to output only the SQL commands
SET HEADING OFF;
SET FEEDBACK OFF;
SET PAGESIZE 0;
SET TRIMSPOOL ON;
SET LINESIZE 200;

-- 2. Define the output file name
SPOOL oltp_grants.sql

-- 3. Generate the GRANT statements for all tables in the OLTP schema
SELECT 'GRANT ALL ON OLTP.' || table_name || ' TO amit;'
FROM all_tables
WHERE owner = 'OLTP';

-- 4. Close the file
SPOOL OFF;


SELECT username, default_tablespace, temporary_tablespace 
FROM dba_users 
WHERE username = 'AMIT';

SELECT privilege FROM dba_sys_privs WHERE grantee = 'AMIT';

SELECT SYS_CONTEXT('USERENV', 'CON_NAME') AS container FROM DUAL;

d
SELECT username, account_status, default_tablespace
FROM   dba_users
WHERE  username IN ('OLTP', 'STAGING', 'AMIT')
ORDER BY username;

SELECT privilege
FROM   dba_sys_privs
WHERE  grantee IN ('OLTP','STAGING','AMIT')
ORDER BY grantee, privilege;

SELECT tablespace_name, username, bytes/1024/1024 AS mb 
FROM dba_ts_quotas 
WHERE username = 'OLTP';
COMMIT;


   SELECT dp.patient_key, dd.doctor_key, dt.date_key, SUM(st.cost)
    FROM STG_Visits sv
    JOIN STG_Treatments st ON sv.src_visit_id = st.src_visit_id
    JOIN Dim_Patient dp ON dp.patient_id = sv.src_patient_id
    JOIN Dim_Doctor dd ON dd.doctor_id = sv.src_doctor_id
    JOIN Dim_Date dt ON dt.the_date = sv.visit_date
    GROUP BY dp.patient_key, dd.doctor_key, dt.date_key;

drop table STG_Patients;
drop table stg_doctors;
drop table STG_VISITS;
drop table stg_treatments;
drop table stg_billing;

----------MANUAL TESTING
truncate table stg_treatments;
truncate table stg_billing;
truncate table STG_VISITS;
truncate table STG_Patients;
truncate table stg_doctors;

truncate table BILLING;
truncate table TREATMENTS;
truncate table VISITS;
truncate table PATIENTS;
truncate table DOCTORS;

truncate table FACT_VISITS;
truncate table DIM_DATE;
truncate table DIM_DOCTOR;
truncate table DIM_PATIENT;


        select * from STG_PATIENTS;
                select * from STG_PATIENTS;

    select * from stg_doctors;
            select * from stg_treatments;


                select * from STG_VISITS;

                    select * from stg_billing;



select * from PATIENTS;
select * from DOCTORS;
select * from VISITS;
select * from TREATMENTS;
select * from BILLING;


select * from DIM_DATE;
select * from DIM_DOCTOR;
select * from DIM_PATIENT;
select * from FACT_VISITS;



SELECT * FROM ALL_TABLES WHERE TABLE_NAME in ('STG_Patients','STG_PATIENTS')


select (MONTHS_BETWEEN(SYSDATE,to_date(dob,'YYYY-MM-DD'))/12)) from STG_Patients;
select * from Dim_Patient

sele
desc Patients
commit;