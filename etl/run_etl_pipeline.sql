BEGIN
     -- Load Dim_Patient
    INSERT INTO Dim_Patient(patient_id,age,gender)
    SELECT patient_id, age, gender
    FROM Patients;

    -- Load Dim_Doctor
    INSERT INTO Dim_Doctor(doctor_id,specialization)
    SELECT doctor_id,specialization FROM Doctors;

    -- Load Dim_Date
    INSERT INTO Dim_Date(date_key,the_date,month,year)
    SELECT TO_NUMBER(TO_CHAR(visit_date,'YYYYMMDD')), visit_date, TO_NUMBER(TO_CHAR(visit_date,'MM')), TO_NUMBER(TO_CHAR(visit_date,'YYYY'))
    FROM Visits;

    -- Load Fact_Visits
    INSERT INTO Fact_Visits(patient_key,doctor_key,date_key,total_cost)
    SELECT dp.patient_key, dd.doctor_key, dt.date_key, SUM(st.cost)
    FROM VISITS sv
    JOIN TREATMENTS st ON sv.visit_id = st.visit_id
    JOIN Dim_Patient dp ON dp.patient_id = sv.patient_id
    JOIN Dim_Doctor dd ON dd.doctor_id = sv.doctor_id
    JOIN Dim_Date dt ON dt.the_date = sv.visit_date
    GROUP BY dp.patient_key, dd.doctor_key, dt.date_key;
commit;
EXCEPTION WHEN OTHERS THEN ROLLBACK;
END;
/