-- Staging Tables
CREATE TABLE STG_Patients (
    src_patient_id VARCHAR2(50),
    name VARCHAR2(100),
    dob DATE,
    gender VARCHAR2(10),
    load_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE STG_Doctors (
    src_doctor_id VARCHAR2(50),
    name VARCHAR2(100),
    specialization VARCHAR2(100),
    load_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE STG_Visits (
    src_visit_id VARCHAR2(50),
    src_patient_id VARCHAR2(50),
    src_doctor_id VARCHAR2(50),
    visit_date DATE,
    diagnosis VARCHAR2(255),
    load_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE STG_Treatments (
    src_treatment_id VARCHAR2(50),
    src_visit_id VARCHAR2(50),
    treatment_type VARCHAR2(100),
    cost NUMBER(10,2),
    load_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
);

CREATE TABLE STG_Billing (
    src_bill_id VARCHAR2(50),
    src_visit_id VARCHAR2(50),
    total_amount NUMBER(10,2),
    insurance_provider VARCHAR2(100),
    load_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP
);