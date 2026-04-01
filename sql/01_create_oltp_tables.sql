-- OLTP tables
CREATE TABLE Patients(
    -- patient_id VARCHAR2(10) PRIMARY KEY,
    patient_id VARCHAR2(10) PRIMARY KEY,
    name VARCHAR2(100),
    dob DATE,
    age number,
    gender VARCHAR2(10)
);

CREATE TABLE Doctors(
    doctor_id VARCHAR2(10) PRIMARY KEY,
    name VARCHAR2(100),
    specialization VARCHAR2(100)
);

CREATE TABLE Visits(
    visit_id VARCHAR2(10) PRIMARY KEY,
    patient_id VARCHAR2(10) ,
    doctor_id VARCHAR2(10) ,
    visit_date DATE,
    diagnosis VARCHAR2(255),
    CONSTRAINT fk_patient FOREIGN KEY (patient_id) REFERENCES Patients(patient_id),
    CONSTRAINT fk_doctor FOREIGN KEY (doctor_id) REFERENCES Doctors(doctor_id)
);

CREATE TABLE Treatments(
    treatment_id VARCHAR2(10) PRIMARY KEY,
    visit_id VARCHAR2(10),
    treatment_type VARCHAR2(100),
    cost NUMERIC,
    CONSTRAINT fk_visit FOREIGN KEY (visit_id) REFERENCES Visits(visit_id)
);

CREATE TABLE Billing(
    bill_id VARCHAR2(10) PRIMARY KEY,
    visit_id VARCHAR2(10),
    total_amount NUMERIC,
    insurance_provider VARCHAR2(100),
    CONSTRAINT fk_visit_bill FOREIGN KEY (visit_id) REFERENCES Visits(visit_id)
);






