USE ACCOUNT ACCOUNTADMIN;

-- Task 1: Create Snowflake Objects

CREATE  OR REPLACE WAREHOUSE HEALTHCARE_WH
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE;


-- SCAL UP TO SMALL 
ALTER WAREHOUSE HEALTHCARE_WH SET WAREHOUSE_SIZE = 'SMALL';

-- SCAL IT DOWN BACK TO XSAMLL 
ALTER WAREHOUSE HEALTHCARE_WH SET WAREHOUSE_SIZE = 'XSMALL';

-- SUSPENDE THE WAREHOUSE MANUALLY (TURN IT OFFLINE)
ALTER WAREHOUSE HEALTHCARE_WH SUSPEND;

-- RESUME THE WAREHOUSE MANUALLY (TURN IT OFFLINE)
ALTER WAREHOUSE HEALTHCARE_WH RESUME;


-- CREATE THE DATABASE AND SCHEMA
CREATE OR REPLACE DATABASE HEALTHCARE_DB;

CREATE OR REPLACE SCHEMA HEALTHCARE_DB.APPOINTMENT_SCHEMA;

USE WAREHOUSE HEALTHCARE_WH;
USE DATABASE HEALTHCARE_DB;
USE SCHEMA APPOINTMENT_SCHEMA;

-- CREATE FILE FORMAT FOR CSV FILE 

CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER =  ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';


DESC FILE FORMAT MY_CSV_FORMAT;

-- CREATE INTERNAL STAGE 
CREATE OR REPLACE STAGE APPOINTMENT_STAGE
FILE_FORMAT = MY_CSV_FORMAT;

-- CREATE RAW TABLE 

CREATE OR REPLACE TABLE APPOINTMENT_RAW (
    PATIENTID NUMBER(20,2),
    APPOINTMENTID NUMBER(20,0),
    GENDER VARCHAR(10),
    SCHEDULEDDAY VARCHAR(50),
    APPOINTMENTDAY VARCHAR(50),
    AGE NUMBER(3,0),
    NEIGHBOURHOOD VARCHAR(255),
    SCHOLARSHIP NUMBER(1,0),
    HIPERTENSION NUMBER(1,0),
    DIABETES NUMBER(1,0),
    ALCOHOLISM NUMBER(1,0),
    HANDCAP NUMBER(1,0),
    SMS_RECEIVED NUMBER(1,0),
    NO_SHOW VARCHAR(10)
);


-- COPY DATA TO INTERNAL STAGE TO RAW TABLE 
COPY INTO APPOINTMENT_RAW 
FROM @APPOINTMENT_STAGE/appointment_no_show.csv
FILE_FORMAT = MY_CSV_FORMAT
ON_ERROR = 'CONTINUE';

list @APPOINTMENT_STAGE;

SELECT  * FROM APPOINTMENT_RAW LIMIT 10;



-- CREATE THE FINAL TABLE 
CREATE OR REPLACE TABLE APPOINTMENT_FINAL (
    PATIENT_ID NUMBER(20,2),
    APPOINTMENT_ID NUMBER(20 , 2),
    GENDER VARCHAR(10),
    AGE NUMBER(3,0),
    AGE_GROUP VARCHAR(20),
    SCHEDULED_DATE TIMESTAMP, 
    APPOINTMENT_DATE TIMESTAMP,
    WAITING_DAYS NUMBER(5, 0),
    NEIGHBORHOOD VARCHAR(255), 
    SCHOLARSHIP NUMBER(1, 0),
    HYPERTENSION NUMBER(1,0),
    DIABETES NUMBER(1,0),
    ALCOHOLISM NUMBER(1,0),
    HANDICAP NUMBER(1,0),
    SMS_RECEIVED NUMBER(1,0),
    NO_SHOW_STATUS VARCHAR(10),
    ATTENDANCE_STATUS VARCHAR(20)
);


-- CREATE PROCEDURE SNOWPARK ==> PYTHON ==> FOR TRANSFORMATION 

CREATE OR REPLACE PROCEDURE sp_transform_healthcare_data()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9' 
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, to_timestamp, datediff, abs

def main(session: snowpark.Session):
    # 1. Read from raw table
    df_raw = session.table("HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_RAW")
    
    # 2. Transform the columns
    df_transformed = df_raw.select(
        col("PatientId").alias("Patient_ID"),
        col("AppointmentID").alias("Appointment_ID"),
        col("Gender"),
        col("Age"),
        # Age Group conditional clustering
        when(col("Age") < 12, "Child")
        .when((col("Age") >= 12) & (col("Age") < 20), "Teen")
        .when((col("Age") >= 20) & (col("Age") < 65), "Adult")
        .otherwise("Senior").alias("Age_Group"),
        # Convert strings to proper Timestamps
        to_timestamp(col("ScheduledDay")).alias("Scheduled_Date"),
        to_timestamp(col("AppointmentDay")).alias("Appointment_Date"),
        # Calculate Waiting Days (absolute difference)
        abs(datediff("day", to_timestamp(col("ScheduledDay")), to_timestamp(col("AppointmentDay")))).alias("Waiting_Days"),
        col("Neighbourhood").alias("Neighborhood"),
        col("Scholarship"),
        col("Hipertension").alias("Hypertension"),
        col("Diabetes"),
        col("Alcoholism"),
        col("Handcap").alias("Handicap"),
        col("SMS_received"),
        col("No_show").alias("No_Show_Status"),
        # Attendance Status translation
        when(col("No_show") == "Yes", "Missed").otherwise("Visited").alias("Attendance_Status")
    )
    
    # 3. Write transformed results into the Final table
    df_transformed.write.mode("append").save_as_table("HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_FINAL")
    
    return "Data Transformation Completed Successfully via Snowpark Python!"
$$;


CALL sp_transform_healthcare_data();

-- AS WE KNOW SNOWPARK CODE NOT WORK IN STANDARD ACCOUNT SO I WRITE SQL CODE FOR TRANSFORMATION 
INSERT INTO HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_FINAL
(
    PATIENT_ID,
    APPOINTMENT_ID,
    GENDER,
    AGE,
    AGE_GROUP,
    SCHEDULED_DATE,
    APPOINTMENT_DATE,
    WAITING_DAYS,
    NEIGHBORHOOD,
    SCHOLARSHIP,
    HYPERTENSION,
    DIABETES,
    ALCOHOLISM,
    HANDICAP,
    SMS_RECEIVED,
    NO_SHOW_STATUS,
    ATTENDANCE_STATUS
)
SELECT
    PATIENTID AS PATIENT_ID,
    APPOINTMENTID AS APPOINTMENT_ID,
    GENDER,
    AGE,

    CASE
        WHEN AGE < 12 THEN 'Child'
        WHEN AGE >= 12 AND AGE < 20 THEN 'Teen'
        WHEN AGE >= 20 AND AGE < 65 THEN 'Adult'
        ELSE 'Senior'
    END AS AGE_GROUP,

    TO_TIMESTAMP(SCHEDULEDDAY) AS SCHEDULED_DATE,
    TO_TIMESTAMP(APPOINTMENTDAY) AS APPOINTMENT_DATE,

    ABS(
        DATEDIFF(
            DAY,
            TO_TIMESTAMP(SCHEDULEDDAY),
            TO_TIMESTAMP(APPOINTMENTDAY)
        )
    ) AS WAITING_DAYS,

    NEIGHBOURHOOD AS NEIGHBORHOOD,
    SCHOLARSHIP,
    HIPERTENSION AS HYPERTENSION,
    DIABETES,
    ALCOHOLISM,
    HANDCAP AS HANDICAP,
    SMS_RECEIVED,
    NO_SHOW AS NO_SHOW_STATUS,

    CASE
        WHEN NO_SHOW = 'Yes' THEN 'Missed'
        ELSE 'Visited'
    END AS ATTENDANCE_STATUS

FROM HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_RAW;

SELECT * FROM HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_RAW LIMIT 10;
SELECT * FROM HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_FINAL LIMIT 10;



-- Create a stream to monitor new inserts on the raw table
CREATE OR REPLACE STREAM STR_APPOINTMENT_RAW ON TABLE APPOINTMENT_RAW;

-- Let's simulate a insert new record coming from the hospital check-in desk
INSERT INTO APPOINTMENT_RAW VALUES 
(99999999, 8888888, 'F', '2026-05-20T08:00:00Z', '2026-05-25T10:00:00Z', 34, 'JARDIM CAMBURI', 0, 0, 0, 0, 0, 1, 'No');

-- CHECK THE STREAM NEW RECORD INSERT IN STREAM OR NOT 
SELECT * FROM STR_APPOINTMENT_RAW;


-- Run an incremental load using the Stream instead of the base raw table
INSERT INTO APPOINTMENT_FINAL (
    Patient_ID, Appointment_ID, Gender, Age, Age_Group, 
    Scheduled_Date, Appointment_Date, Waiting_Days, Neighborhood, 
    Scholarship, Hypertension, Diabetes, Alcoholism, Handicap, 
    SMS_received, No_Show_Status, Attendance_Status
)
SELECT
    PATIENTID AS PATIENT_ID,
    APPOINTMENTID AS APPOINTMENT_ID,
    GENDER,
    AGE,

    CASE
        WHEN AGE < 12 THEN 'Child'
        WHEN AGE >= 12 AND AGE < 20 THEN 'Teen'
        WHEN AGE >= 20 AND AGE < 65 THEN 'Adult'
        ELSE 'Senior'
    END AS AGE_GROUP,

    TO_TIMESTAMP(SCHEDULEDDAY) AS SCHEDULED_DATE,
    TO_TIMESTAMP(APPOINTMENTDAY) AS APPOINTMENT_DATE,

    ABS(
        DATEDIFF(
            DAY,
            TO_TIMESTAMP(SCHEDULEDDAY),
            TO_TIMESTAMP(APPOINTMENTDAY)
        )
    ) AS WAITING_DAYS,

    NEIGHBOURHOOD AS NEIGHBORHOOD,
    SCHOLARSHIP,
    HIPERTENSION AS HYPERTENSION,
    DIABETES,
    ALCOHOLISM,
    HANDCAP AS HANDICAP,
    SMS_RECEIVED,
    NO_SHOW AS NO_SHOW_STATUS,

    CASE
        WHEN NO_SHOW = 'Yes' THEN 'Missed'
        ELSE 'Visited'
    END AS ATTENDANCE_STATUS
FROM STR_APPOINTMENT_RAW 
WHERE METADATA$ACTION = 'INSERT';

-- Query the stream again. It automatically flushes clean and empty!
SELECT * FROM STR_APPOINTMENT_RAW;

SELECT * FROM APPOINTMENT_FINAL LIMIT 10;



-- step 9
-- Create the automated ingestion pipe wrapper
CREATE OR REPLACE PIPE PIPE_APPOINTMENT_INGEST
AUTO_INGEST = FALSE
AS
COPY INTO HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_RAW
FROM @HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_STAGE
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

SHOW PIPES;  

-- FOR RUN MANUALLY SNOWPIPE  
ALTER PIPE PIPE_APPOINTMENT_INGEST REFRESH;


-- Create a pristine Database View for clean consumption by Power BI
CREATE OR REPLACE VIEW VW_APPOINTMENT_DASHBOARD AS
SELECT 
    Patient_ID,
    Appointment_ID,
    Gender,
    Age,
    Age_Group,
    Scheduled_Date,
    Appointment_Date,
    Waiting_Days,
    Neighborhood,
    Scholarship AS Has_Scholarship,
    Hypertension AS Has_Hypertension,
    Diabetes AS Has_Diabetes,
    Alcoholism AS Has_Alcoholism,
    Handicap AS Is_Handicapped,
    SMS_received AS SMS_Reminders_Received,
    No_Show_Status,
    Attendance_Status
FROM APPOINTMENT_FINAL;






