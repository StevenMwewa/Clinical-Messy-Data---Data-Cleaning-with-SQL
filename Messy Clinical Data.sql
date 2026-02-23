
-- importing the Data into the Table
DROP TABLE IF EXISTS clinical_data;
CREATE TABLE clinical_data (
    patient_id VARCHAR(20),
    full_name VARCHAR(100),
    gender VARCHAR(10),
    date_of_birth VARCHAR(20),
    phone VARCHAR(50),
    admission_time VARCHAR(50),
    discharge_time VARCHAR(50),
    vital_type VARCHAR(50),
    vital_value VARCHAR(50),
    lab_test VARCHAR(50),
    lab_result VARCHAR(50)
);

SELECT * FROM clinical_data

-- Check for data quality issues
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT patient_id) as unique_patients,
    SUM(CASE WHEN full_name IS NULL OR full_name = '' THEN 1 ELSE 0 END) as missing_names,
    SUM(CASE WHEN gender NOT IN ('male', 'female', 'M', 'F', 'Male', 'Female') THEN 1 ELSE 0 END) as unusual_genders
FROM clinical_data;

-- standardize the patient_id column 
SELECT DISTINCT patient_id
FROM clinical_data
ORDER BY patient_id;

-- removing the extra spaces 
SELECT
    patient_id,
    TRIM(patient_id) AS trimmed_id
FROM clinical_data;


-- extract the numeric part only using regexp_replace
SELECT
    patient_id,
    REGEXP_REPLACE(patient_id, '[^0-9]', '', 'g') AS numeric_part
FROM clinical_data;


-- rebuild the patient_id column to have P-XXXX  formart

SELECT
    patient_id,
    'P-' || LPAD(
        REGEXP_REPLACE(patient_id, '[^0-9]', '', 'g'),
        4,
        '0'
    ) AS standardized_patient_id
FROM clinical_data;


-- LPAD(string, length, [pad_string])
-- Adding leading zeros: SELECT LPAD('42', 5, '0'); ---> 00042
-- Padding with characters: SELECT LPAD('SQL', 7, '*'); ---> ****SQL
-- Using multiple characters: SELECT LPAD('Hi', 6, 'ab'); ---> ababHi

-- Truncation: If the original string is longer than the specified length, the function will truncate (cut off) 
-- the string from the right to fit the length.Example: LPAD('Database', 4, '*') ---> Data.


CREATE TABLE cleaned_clinical_data AS
SELECT
    *,
    'P-' || LPAD(
        REGEXP_REPLACE(patient_id, '[^0-9]', '', 'g'),
        4,
        '0'
    ) AS standardized_patient_id
FROM clinical_data;

SELECT * FROM cleaned_clinical_data;

-- checking for duplicates

SELECT
    standardized_patient_id,
    COUNT(*) AS records
FROM cleaned_clinical_data
GROUP BY standardized_patient_id
HAVING COUNT(*) > 1;
------------------------------------------------------------------------------------------------------------------------------------
-- keep the earliest submission ------------ WE WILL REMOVE DUPLICATES AFTER CLEANING THE ENTIRE TABLE.

CREATE TABLE deduplicated_patients AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY standardized_patient_id
               ORDER BY admission_time
           ) AS rn
    FROM cleaned_clinical_data
) t
WHERE rn = 1;

SELECT * FROM cleaned_clinical_data;

-------------------------------------------------------------------------------------------------------------------------------------


-- ------------------------------------ STANDARDIZING NAMES -------------------------------------------------------------------------

SELECT
    full_name,
    TRIM(full_name) AS trimmed_name
FROM cleaned_clinical_data;

SELECT
    full_name,
    INITCAP(TRIM(full_name)) AS proper_name
FROM cleaned_clinical_data;

SELECT
    COALESCE(INITCAP(TRIM(full_name)), 'Unknown') AS clean_full_name
FROM cleaned_clinical_data;

-- Now to add these in one step and add the new column to the table.

ALTER TABLE cleaned_clinical_data
ADD COLUMN clean_full_name TEXT;

UPDATE cleaned_clinical_data
SET clean_full_name =
    COALESCE(
        INITCAP(TRIM(full_name)),
        'Unknown'
    );

SELECT * FROM cleaned_clinical_data;

ALTER TABLE cleaned_clinical_data
DROP COLUMN patient_id;

ALTER TABLE cleaned_clinical_data
DROP COLUMN full_name;



-- -------------------------------------------------- STANDARDIZING GENDER ---------------------------------------------------------------------



SELECT DISTINCT gender
FROM cleaned_clinical_data
ORDER BY gender;


-- -------------------------------------
SELECT
    gender,
    LOWER(TRIM(gender)) AS normalized_gender
FROM cleaned_clinical_data;

-- ------------------------------------
SELECT
    gender,
    CASE
        WHEN LOWER(TRIM(gender)) IN ('m', 'male') THEN 'M'
        WHEN LOWER(TRIM(gender)) IN ('f', 'female') THEN 'F'
        ELSE 'Unknown'
    END AS standardized_gender
FROM cleaned_clinical_data;


-- ------------------------------------
ALTER TABLE cleaned_clinical_data
ADD COLUMN standardized_gender TEXT;

UPDATE cleaned_clinical_data
SET standardized_gender =
    CASE
        WHEN LOWER(TRIM(gender)) IN ('m', 'male') THEN 'M'
        WHEN LOWER(TRIM(gender)) IN ('f', 'female') THEN 'F'
        ELSE 'Unknown'
    END;


ALTER TABLE cleaned_clinical_data
DROP COLUMN gender;

SELECT * FROM cleaned_clinical_data;


-- ------------------------------------------- STANDARDIZING DATE COLUMNS ---------------------------------------------------------------------

-- -------------------------- DATE OF BIRTH

SELECT
    date_of_birth,
    CASE
        WHEN date_of_birth ~ '^\d{4}-\d{2}-\d{2}$'
            THEN date_of_birth::DATE

        WHEN date_of_birth ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(date_of_birth, 'DD/MM/YYYY')

        WHEN date_of_birth ~ '^\d{2}-\d{2}-\d{4}$'
            THEN TO_DATE(date_of_birth, 'MM-DD-YYYY')

        ELSE NULL
    END AS clean_dob
FROM cleaned_clinical_data;

-- ------------------------------------------

ALTER TABLE cleaned_clinical_data
ADD COLUMN clean_date_of_birth DATE;

UPDATE cleaned_clinical_data
SET clean_date_of_birth =
    CASE
        WHEN date_of_birth ~ '^\d{4}-\d{2}-\d{2}$'
            THEN date_of_birth::DATE
        WHEN date_of_birth ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(date_of_birth, 'DD/MM/YYYY')
        WHEN date_of_birth ~ '^\d{2}-\d{2}-\d{4}$'
            THEN TO_DATE(date_of_birth, 'MM-DD-YYYY')
        ELSE NULL
    END;

SELECT * FROM cleaned_clinical_data;

ALTER TABLE cleaned_clinical_data
DROP COLUMN date_of_birth;

-- --------------------- ADMISSION & DISCHARGE TIMES


SELECT
    admission_time,
    CASE
        WHEN admission_time ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$'
            THEN admission_time::TIMESTAMP

        WHEN admission_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
            THEN TO_TIMESTAMP(admission_time, 'DD/MM/YYYY HH24:MI')
        ELSE NULL
    END AS clean_admission
FROM cleaned_clinical_data;

-- --------------------------------------------------

ALTER TABLE cleaned_clinical_data
ADD COLUMN clean_admission_time TIMESTAMP,
ADD COLUMN clean_discharge_time TIMESTAMP;

UPDATE cleaned_clinical_data
SET
    clean_admission_time =
        CASE
            WHEN admission_time ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$'
                THEN admission_time::TIMESTAMP
            WHEN admission_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
                THEN TO_TIMESTAMP(admission_time, 'DD/MM/YYYY HH24:MI')
            ELSE NULL
        END,

    clean_discharge_time =
        CASE
            WHEN discharge_time ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$'
                THEN discharge_time::TIMESTAMP
            WHEN discharge_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
                THEN TO_TIMESTAMP(discharge_time, 'DD/MM/YYYY HH24:MI')
            ELSE NULL
        END;


ALTER TABLE cleaned_clinical_data
DROP COLUMN admission_time;

ALTER TABLE cleaned_clinical_data
DROP COLUMN discharge_time;

SELECT * FROM cleaned_clinical_data;



-- -------------------------------------------- CORRECTING THE PHONE NUMBERS -------------------------------------------------------------------


ALTER TABLE cleaned_clinical_data
ADD COLUMN standardized_phone TEXT,
ADD COLUMN is_invalid_phone BOOLEAN;



UPDATE cleaned_clinical_data
SET
    standardized_phone =
        CASE
            -- 0971234567
            WHEN REGEXP_REPLACE(phone, '[^0-9]', '', 'g') ~ '^0\d{9}$'
                THEN '+260' || SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g') FROM 2)

            -- 260971234567
            WHEN REGEXP_REPLACE(phone, '[^0-9]', '', 'g') ~ '^260\d{9}$'
                THEN '+' || REGEXP_REPLACE(phone, '[^0-9]', '', 'g')

            -- 971234567 (missing 0 or 260)
            WHEN REGEXP_REPLACE(phone, '[^0-9]', '', 'g') ~ '^\d{9}$'
                THEN '+260' || REGEXP_REPLACE(phone, '[^0-9]', '', 'g')

            ELSE NULL
        END,

    is_invalid_phone =
        CASE
            WHEN REGEXP_REPLACE(phone, '[^0-9]', '', 'g') ~ '^0\d{9}$'
              OR REGEXP_REPLACE(phone, '[^0-9]', '', 'g') ~ '^260\d{9}$'
              OR REGEXP_REPLACE(phone, '[^0-9]', '', 'g') ~ '^\d{9}$'
            THEN FALSE
            ELSE TRUE
        END;




SELECT * FROM cleaned_clinical_data;

ALTER TABLE cleaned_clinical_data
DROP COLUMN phone;



-- -------------------------------------------- STANDARDIZING VITAL TYPES ------------------------------------------------------------------------


SELECT 
	DISTINCT(vital_type)
FROM cleaned_clinical_data;


SELECT 
	vital_type,
	CASE 
		WHEN LOWER(TRIM(vital_type)) IN ('temperature', 'temp') THEN 'Temperature'
		WHEN LOWER(TRIM(vital_type)) IN ('hr', 'heart rate') THEN 'Heart Rate'
		WHEN LOWER(TRIM(vital_type)) IN ('bp') THEN 'Blood Pressure'
	ELSE 'Unknown'
	END as vitals
FROM cleaned_clinical_data;

-- -------------------------------------------------------

ALTER TABLE cleaned_clinical_data
ADD COLUMN standard_vital_types TEXT;

UPDATE cleaned_clinical_data
SET
    standard_vital_types =
		CASE 
			WHEN LOWER(TRIM(vital_type)) IN ('temperature', 'temp') THEN 'Temperature'
			WHEN LOWER(TRIM(vital_type)) IN ('hr', 'heart rate') THEN 'Heart Rate'
			WHEN LOWER(TRIM(vital_type)) IN ('bp') THEN 'Blood Pressure'
		ELSE 'Unknown'
		END;


SELECT * FROM cleaned_clinical_data;

ALTER TABLE cleaned_clinical_data
DROP COLUMN vital_type;


-- ------------------------------------------------- STANDARDIZING VITAL VALUES -------------------------------------------------------------

ALTER TABLE cleaned_clinical_data
ADD COLUMN clean_vital_values VARCHAR(50);

UPDATE cleaned_clinical_data
SET 
clean_vital_values = 
	TRIM(vital_value);

ALTER TABLE cleaned_clinical_data
DROP COLUMN vital_value;


SELECT * FROM cleaned_clinical_data;


-- ------------------------------------------------- STANDARDIZING LAB TESTS -------------------------------------------------------------------


SELECT 
	DISTINCT(lab_test)
FROM cleaned_clinical_data;


SELECT 
	lab_test,
	CASE 
		WHEN LOWER(TRIM(lab_test)) IN ('wbc') THEN 'WBC'
		WHEN LOWER(TRIM(lab_test)) IN ('hb', 'hgb') THEN 'Hgb'
		WHEN LOWER(TRIM(lab_test)) IN ('creatinine') THEN 'Creatinine'
	ELSE 'Unknown'
	END as clean_lab_test
FROM cleaned_clinical_data;

-- ----------------------------------------------------------

ALTER TABLE cleaned_clinical_data
ADD COLUMN clean_lab_test TEXT;

UPDATE cleaned_clinical_data
SET clean_lab_test = 
		CASE 
		WHEN LOWER(TRIM(lab_test)) IN ('wbc') THEN 'WBC'
		WHEN LOWER(TRIM(lab_test)) IN ('hb', 'hgb') THEN 'Hgb'
		WHEN LOWER(TRIM(lab_test)) IN ('creatinine') THEN 'Creatinine'
	ELSE 'Unknown'
	END;
	

ALTER TABLE cleaned_clinical_data
DROP COLUMN lab_test;

SELECT * FROM cleaned_clinical_data;


-- --------------------------------------------- STANDARDIZING LAB RESULTS ----------------------------------------------------------------------

ALTER TABLE cleaned_clinical_data
ADD COLUMN clean_lab_results VARCHAR(50);

UPDATE cleaned_clinical_data
SET 
clean_lab_results = 
	TRIM(lab_result);

ALTER TABLE cleaned_clinical_data
DROP COLUMN lab_result;


SELECT * FROM cleaned_clinical_data;



-- --------------------------------------- EXPLORATORY DATA ANALYSIS ----------------------------------------------------------------------------

-- ---------------------------------------- Observing the Age Distribution

SELECT
    FLOOR(EXTRACT(YEAR FROM AGE(CURRENT_DATE, clean_date_of_birth))) AS age,
    COUNT(*) AS patients
FROM cleaned_clinical_data
WHERE clean_date_of_birth IS NOT NULL
GROUP BY age
ORDER BY age;

-- ---------------------------------------- Observing the Gender Distribution

SELECT standardized_gender, COUNT(*)
FROM cleaned_clinical_data
GROUP BY standardized_gender;


-- ---------------------------------------- Observing the Hospital Lengh of Stay (LOS) 

SELECT
    standardized_patient_id,
    ROUND((EXTRACT(EPOCH FROM (clean_discharge_time - clean_admission_time)) / 3600) / 24, 0) AS DAYS_in_hospital
FROM cleaned_clinical_data
WHERE clean_discharge_time IS NOT NULL;

-- ---------------------------------------- Observing the Admission trends

SELECT
    DATE(clean_admission_time) AS admission_date,
    COUNT(*) AS admissions
FROM cleaned_clinical_data
GROUP BY admission_date
ORDER BY admission_date;
















