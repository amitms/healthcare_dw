BEGIN
     -- transform Patient
    INSERT INTO PATIENTS(patient_id,name,dob,age,gender)
    SELECT
        SUBSTR(patient_id, 1, 10)                            AS patient_id,
        SUBSTR(NAME, 1, 100)                                 AS NAME,
        TO_DATE(DOB, 'YYYY-MM-DD')                           AS DOB,
        TRUNC(MONTHS_BETWEEN(SYSDATE, TO_DATE(DOB, 'YYYY-MM-DD')) / 12) AS AGE,
        SUBSTR(GENDER, 1, 10)                                AS GENDER
    FROM STG_PATIENTS
    WHERE PATIENT_ID IS NOT NULL AND DOB IS NOT NULL
    AND REGEXP_LIKE(DOB, '^\d{4}-\d{2}-\d{2}$');

    -- transform Doctor
    INSERT INTO DOCTORS(doctor_id,name, specialization)
    SELECT doctor_id,name,specialization FROM STG_Doctors;

    -- transform visits
    INSERT INTO VISITS(VISIT_ID,PATIENT_ID,DOCTOR_ID,VISIT_DATE, DIAGNOSIS)
    SELECT visit_id, patient_id, doctor_id,to_date(visit_date,'YYYY-MM-DD'), diagnosis 
    FROM STG_Visits;

   -- transform treatments
    INSERT INTO TREATMENTS(TREATMENT_ID,VISIT_ID,TREATMENT_TYPE, COST)
    SELECT TREATMENT_ID,VISIT_ID, TREATMENT_TYPE, TO_NUMBER(COST)
    FROM STG_TREATMENTS;
    
    -- transform billing
    INSERT INTO BILLING (bill_id,visit_id, total_amount, insurance_provider)
    SELECT bill_id, visit_id, to_number(total_amount), insurance_provider 
    FROM STG_Billing;
commit;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END;
/