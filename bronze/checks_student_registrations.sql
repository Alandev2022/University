--quality check for loading into silver for student_registrations
--==========================================================================================================================
-- student_id

select student_id
from bronze.student_registrations
where student_id != trim(student_id)

---Result : Nothing, confirm no leading/trailing spaces

SELECT 
    'STU-' + RIGHT('000000' + 
        REPLACE(REPLACE(REPLACE(trim(student_id), 'STU-', ''), 'STU', ''), '-', ''), 
    6) AS student_id
FROM bronze.student_registrations;

-- student_id column contains various formats such as 'STU-100123','100124','STU100125' which should all be standardised to 'STU-######'  -- facility_id column

--==========================================================================================================================
--first_name

select first_name
from bronze.student_registrations
where first_name != trim(first_name)

---Result : Nothing, confirm no leading/trailing spaces
--- All values are distinct

--============================================================================================================================
---surname

select surname
from bronze.student_registrations
where surname != trim(surname)

---Result : Nothing, confirm no leading/trailing spaces
---No NULL values

--============================================================================================================================
---id_number

select id_number
from bronze.student_registrations

---Some of the entries are NULL

--============================================================================================================================
---date_of_birth
SELECT 
    date_of_birth,
    CONVERT(VARCHAR(10), 
        COALESCE(
            TRY_CONVERT(DATE, date_of_birth, 103),   -- dd/MM/yyyy
            TRY_CONVERT(DATE, date_of_birth, 101),   -- MM/dd/yyyy
            TRY_CONVERT(DATE, date_of_birth, 106),   -- dd-MMM-yy
            TRY_CONVERT(DATE, date_of_birth, 113),   -- dd-MMM-yyyy HH:mi:ss
            TRY_CONVERT(DATE, date_of_birth, 120),   -- yyyy-MM-dd HH:mi:ss
            TRY_CONVERT(DATE, date_of_birth, 1),     -- mm/dd/yy
            TRY_CONVERT(DATE, date_of_birth, 3),     -- dd/mm/yy
            TRY_CONVERT(DATE, date_of_birth)         -- Fallback
        ), 23) AS date_of_birth
FROM bronze.student_registrations;

--Standardise all dates to yyyy-mm-dd

--==============================================================================================================================
--gender

select distinct gender
from bronze.student_registrations

---Result : 
--gender
--F
--female
--M
--Male

select 
gender,
CASE WHEN gender in ('F','Female') then 'Female'
     ELSE 'Male'
END as gender
from bronze.student_registrations

--Fix so that we can have only two values 'Female' and 'Male'

--==============================================================================================================================
--email

select email
from bronze.student_registrations
where email != trim(email)

---Result : Nothing, confirm no leading/trailing spaces
--==============================================================================================================================
--cellphone
select
CASE 
        -- Remove formatting and unify to +27 format
        WHEN LEFT(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 2) = '27'
            AND LEN(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', '')) = 11
            THEN 
                '+27 ' + 
                SUBSTRING(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 3, 2) + ' ' +
                SUBSTRING(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 5, 3) + ' ' +
                SUBSTRING(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 8, 4)

        -- Starts with 0 and is 10 digits: 0831234567 → +27 83 123 4567
        WHEN LEFT(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 1) = '0'
            AND LEN(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', '')) = 10
            THEN 
                '+27 ' + 
                SUBSTRING(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 2, 2) + ' ' +
                SUBSTRING(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 4, 3) + ' ' +
                SUBSTRING(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 7, 4)

        -- Starts with 278, etc. (no + sign): 27833456789 → +27 83 345 6789
        WHEN LEFT(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 3) = '278'
            AND LEN(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', '')) = 11
            THEN 
                '+27 ' + 
                SUBSTRING(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 3, 2) + ' ' +
                SUBSTRING(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 5, 3) + ' ' +
                SUBSTRING(REPLACE(REPLACE(REPLACE(cellphone, ' ', ''), '+', ''), '-', ''), 8, 4)

        -- Default if format unrecognized
        ELSE 'Invalid Format'
    END AS cellphone
	from bronze.student_registrations


--- phone numbers are in different formats and need to be standardised to for example '+27 82 111 2222' 





--==============================================================================================================================
--registration_date

SELECT 
    registration_date,
    CONVERT(VARCHAR(10), 
        COALESCE(
            TRY_CONVERT(DATE, registration_date, 103),   -- dd/MM/yyyy
            TRY_CONVERT(DATE, registration_date, 101),   -- MM/dd/yyyy
            TRY_CONVERT(DATE, registration_date, 106),   -- dd-MMM-yy
            TRY_CONVERT(DATE, registration_date, 113),   -- dd-MMM-yyyy HH:mi:ss
            TRY_CONVERT(DATE, registration_date, 120),   -- yyyy-MM-dd HH:mi:ss
            TRY_CONVERT(DATE, registration_date, 1),     -- mm/dd/yy
            TRY_CONVERT(DATE, registration_date, 3),     -- dd/mm/yy
            TRY_CONVERT(DATE, registration_date)         -- Fallback
        ), 23) AS registration_date
FROM bronze.student_registrations;

--Standardise all dates to yyyy-mm-dd

--==============================================================================================================================
--enrollment_status

select distinct enrollment_status
from bronze.student_registrations
where enrollment_status != trim(enrollment_status)

---Result : Nothing, confirm no leading/trailing spaces
---Low cardinality column

select enrollment_status,
CASE WHEN enrollment_status IN ('ACTIVE','Active','active') THEN 'Active'
     WHEN enrollment_status IN ('INACTIVE','Inactive','inactive') THEN 'Inactive'
     WHEN enrollment_status IN ('PENDING','Pending','pending') THEN 'Pending'
	 WHEN enrollment_status IN ('SUSPENDED','Suspended','suspended') THEN 'Suspended'
 ELSE enrollment_status
END AS enrollment_status
     
from bronze.student_registrations

-- Standardise the possible 4 conditions
--==============================================================================================================================
--nationality

select distinct nationality
from bronze.student_registrations
where nationality != trim(nationality)

--Result
--SA
--South Africa
--South African
--ZA
--Zimbabwe

select nationality,
CASE WHEN nationality IN ('SA','South Africa','South African','ZA') THEN 'South African'
     WHEN nationality IN ('Zimbabwe') THEN 'Zimbabwean'
	 ELSE nationality
END AS nationality
from bronze.student_registrations

-- Standardise the possible 2 conditions

--==============================================================================================================================
--province

select province
from bronze.student_registrations
where province != trim(province)

---Result : Nothing, confirm no leading/trailing spaces
---Low cardinality column

--==============================================================================================================================
--program_code

select program_code
from bronze.student_registrations
where program_code != trim(program_code)

---Result : Nothing, confirm no leading/trailing spaces
---Low cardinality column

--==============================================================================================================================
--study_year

select distinct
study_year
from bronze.student_registrations

-- Looks good, low cardinality column

--==============================================================================================================================
--campus

select campus
from bronze.student_registrations

--Results
--Health Sciences
--Hiddingh
--Main 
--Main Campus

select campus,
CASE WHEN campus IN ('Main','Main Campus') THEN 'Main Campus'
ELSE campus
END AS campus
from bronze.student_registrations;

----- We need to find out if Main and Main Campus 