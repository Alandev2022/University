/*
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

Usage Example:
    EXEC silver.load_silver;
	*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

-- Loading silver.staff_information
       SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.staff_information';
		TRUNCATE TABLE silver.staff_information;
		PRINT '>> Inserting Data Into: silver.staff_information';
		INSERT INTO silver.staff_information (
	[staff_id],
	[first_name],
	[last_name],
	[id_number] ,
	[date_of_birth] ,
	[gender] ,
	[email] ,
	[contact_number] ,
	[department] ,
	[faculty] ,
	[position] ,
	[employment_type],
	[start_date] ,
	[end_date] ,
	[qualification] ,
	[office_location] 
		)
       SELECT
	   [staff_id]
      ,[first_name]
      ,[last_name]
      ,CAST(id_number as bigint) as id_number
      ,CASE
        WHEN TRY_CAST(date_of_birth AS DATE) IS NOT NULL THEN TRY_CAST(date_of_birth AS DATE) -- ISO format
        WHEN TRY_CONVERT(DATE, date_of_birth, 103) IS NOT NULL THEN TRY_CONVERT(DATE, date_of_birth, 103) -- dd/mm/yyyy
        WHEN TRY_CONVERT(DATE, date_of_birth, 105) IS NOT NULL THEN TRY_CONVERT(DATE, date_of_birth, 105) -- dd-mm-yyyy
        WHEN TRY_CONVERT(DATE, date_of_birth, 106) IS NOT NULL THEN TRY_CONVERT(DATE, date_of_birth, 106) -- dd-MMM-yy (e.g., 15-Jul-79)
        ELSE NULL
      END as [date_of_birth]
      ,CASE WHEN gender in ('F','Female') then 'Female'
       ELSE 'Male'
      END as gender
      ,[email]
      ,CASE 
        WHEN contact_number LIKE '+27%' THEN 
            '+27 ' +
            SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(contact_number, '+27', ''), ' ', ''), '-', ''), '(', ''), 1, 2) + ' ' +
            SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(contact_number, '+27', ''), ' ', ''), '-', ''), '(', ''), 3, 3) + ' ' +
            SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(contact_number, '+27', ''), ' ', ''), '-', ''), '(', ''), 6, 4)

        WHEN contact_number LIKE '0%' THEN 
            '+27 ' +
            SUBSTRING(REPLACE(REPLACE(REPLACE(contact_number, ' ', ''), '-', ''), '(', ''), 2, 2) + ' ' +
            SUBSTRING(REPLACE(REPLACE(REPLACE(contact_number, ' ', ''), '-', ''), '(', ''), 4, 3) + ' ' +
            SUBSTRING(REPLACE(REPLACE(REPLACE(contact_number, ' ', ''), '-', ''), '(', ''), 7, 4)
        ELSE NULL
      END as contact_number
      ,[department]
      ,[faculty]
      ,[position]
      ,[employment_type]
      ,CASE
        WHEN TRY_CAST(start_date AS DATE) IS NOT NULL THEN TRY_CAST(start_date AS DATE) -- ISO format
        WHEN TRY_CONVERT(DATE, start_date, 103) IS NOT NULL THEN TRY_CONVERT(DATE, start_date, 103) -- dd/mm/yyyy
        WHEN TRY_CONVERT(DATE, start_date, 105) IS NOT NULL THEN TRY_CONVERT(DATE, start_date, 105) -- dd-mm-yyyy
        WHEN TRY_CONVERT(DATE, start_date, 106) IS NOT NULL THEN TRY_CONVERT(DATE, start_date, 106) -- dd-MMM-yy (e.g., 15-Jul-79)
        ELSE NULL
      END as started_date
      ,CASE
        WHEN TRY_CAST(end_date AS DATE) IS NOT NULL THEN TRY_CAST(end_date AS DATE) -- ISO format
        WHEN TRY_CONVERT(DATE, end_date, 103) IS NOT NULL THEN TRY_CONVERT(DATE, end_date, 103) -- dd/mm/yyyy
        WHEN TRY_CONVERT(DATE, end_date, 105) IS NOT NULL THEN TRY_CONVERT(DATE, end_date, 105) -- dd-mm-yyyy
        WHEN TRY_CONVERT(DATE, end_date, 106) IS NOT NULL THEN TRY_CONVERT(DATE, end_date, 106) -- dd-MMM-yy (e.g., 15-Jul-79)
        ELSE NULL
      END as end_date
      ,[qualification]
      ,[office_location]
  FROM [University].[bronze].[staff_information]
  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


--======================================================================================================================================
-- Loading silver.academic_results

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.academic_results';
		TRUNCATE TABLE silver.academic_results;
		PRINT '>> Inserting Data Into: silver.academic_results';
		INSERT INTO silver.academic_results (
	[student_id],
	[course_code],
	[academic_year],
	[semester],
	[assessment_type],
	[mark],
	[grade],
	[status],
	[submission_date],
	[examiner],
	CASE when moderator is not null then moderator
	  else 'STAFF-' + CAST(CAST(SUBSTRING(examiner, 7, LEN(examiner)) AS INT) + 1 AS VARCHAR) 
	  end as [moderator]
		)
SELECT 
       'STU-' + RIGHT('000000' + 
        REPLACE(REPLACE(REPLACE(student_id, 'STU-', ''), 'STU', ''), '-', ''), 
        6) AS [student_id]
      ,[course_code]
      ,CAST([academic_year] AS INT) as academic_year
      ,[semester]
      ,[assessment_type]
      ,[mark]
      ,[grade]
      ,[status]
      ,CAST(submission_date AS DATE) AS submission_date
      ,[examiner]
      ,[moderator]
  FROM [University].[bronze].[academic_results]
  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

--====================================================================================================================================
--Loading silver.campus_facilities

SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.campus_facilities';
		TRUNCATE TABLE silver.campus_facilities;
		PRINT '>> Inserting Data Into: silver.campus_facilities';
		INSERT INTO silver.campus_facilities (
	[facility_id],
	[facility_name] ,
	[building] ,
	[campus] ,
	[capacity] ,
	[facility_type] ,
	[availability] ,
	[booking_contact] ,
	[maintenance_status] ,
	[last_service_date] ,
	[accessibility] ,
	[equipment] ,
	[operating_hours] ,
	[operating_hours_start_time] ,
	[operating_hours_end_time]
		)
SELECT [facility_id]
      ,[facility_name]
      ,[building]
      ,[campus]
      ,[capacity]
      ,[facility_type]
      ,[availability]
      ,[booking_contact]
      ,[maintenance_status]
      ,CASE	
        WHEN last_service_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, last_service_date, 103) -- dd/mm/yyyy
        WHEN last_service_date LIKE '____/__/__' THEN TRY_CONVERT(DATE, last_service_date, 111) -- yyyy/mm/dd
        WHEN last_service_date LIKE '__-__-____' THEN TRY_CONVERT(DATE, last_service_date, 105) -- dd-mm-yyyy
        WHEN last_service_date LIKE '___-__-__' THEN TRY_CONVERT(DATE, last_service_date, 106) -- dd-Mon-yy
		WHEN last_service_date LIKE '__-___-____' THEN TRY_CONVERT(DATE, last_service_date, 106) -- dd-MMM-yyyy
        WHEN last_service_date LIKE '____-__-__' THEN TRY_CONVERT(DATE, last_service_date, 120)
		WHEN last_service_date LIKE '__-___-__' THEN TRY_CONVERT(DATE, last_service_date, 6) -- yyyy-mm-dd
        ELSE NULL
        END AS last_service_date
      ,[accessibility]
      ,[equipment]
      ,[operating_hours]
	  ,FORMAT(CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN TRY_CAST('00:00' AS TIME)
        WHEN CHARINDEX('-', operating_hours) > 0 
             AND TRY_CAST(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1) AS TIME) IS NOT NULL
        THEN TRY_CAST(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1) AS TIME)
        ELSE NULL
	END,N'hh\:mm') AS operating_hours_start_time,
    -- End time logic (handles 24 hours and 24:00)
    FORMAT(CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN TRY_CAST('00:00' AS TIME)
        WHEN operating_hours LIKE '%24:00' THEN TRY_CAST('00:00' AS TIME)
        WHEN CHARINDEX('-', operating_hours) > 0 
             AND TRY_CAST(SUBSTRING(operating_hours, CHARINDEX('-', operating_hours) + 1, 5) AS TIME) IS NOT NULL
        THEN TRY_CAST(SUBSTRING(operating_hours, CHARINDEX('-', operating_hours) + 1, 5) AS TIME)
        ELSE NULL
    END,N'hh\:mm') as operating_hours_end_time
-- you can split the column into start,end and duration times for further analysis if required
  FROM [University].[bronze].[campus_facilities]
  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
  --====================================================================================================================================
  -- Loading silver.financial_transactions

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.financial_transactions';
		TRUNCATE TABLE silver.financial_transactions;
		PRINT '>> Inserting Data Into: silver.financial_transactions';
		INSERT INTO silver.financial_transactions (
			[transaction_id] ,
	[student_id] ,
	[transaction_date] ,
	[transaction_type] ,
	[amount] ,
	[payment_method] ,
	[fee_type] ,
	[academic_year] ,
	[receipt_number] ,
	[processed_by] ,
	[status] 
		)
  SELECT [transaction_id]
      ,'STU-' + RIGHT('000000' + 
        REPLACE(REPLACE(REPLACE(student_id, 'STU-', ''), 'STU', ''), '-', ''), 
        6) AS student_id
      ,CASE	
        WHEN transaction_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, transaction_date, 103) -- dd/mm/yyyy
        WHEN transaction_date LIKE '____/__/__' THEN TRY_CONVERT(DATE, transaction_date, 111) -- yyyy/mm/dd
        WHEN transaction_date LIKE '__-__-____' THEN TRY_CONVERT(DATE, transaction_date, 105) -- dd-mm-yyyy
        WHEN transaction_date LIKE '___-__-__' THEN TRY_CONVERT(DATE, transaction_date, 106) -- dd-Mon-yy
		WHEN transaction_date LIKE '__-___-____' THEN TRY_CONVERT(DATE, transaction_date, 106) -- dd-MMM-yyyy
        WHEN transaction_date LIKE '____-__-__' THEN TRY_CONVERT(DATE, transaction_date, 120)
		WHEN transaction_date LIKE '__-___-__' THEN TRY_CONVERT(DATE, transaction_date, 6) -- yyyy-mm-dd
        ELSE NULL
        END AS transaction_date
      ,[transaction_type]
      ,CAST(
        REPLACE(
            REPLACE(
                LTRIM(RTRIM(
                    -- Remove 'R ' and commas
                    CASE 
                        WHEN amount LIKE 'R%' THEN SUBSTRING(amount, 3, LEN(amount) - 2) -- Remove 'R ' at the beginning
                        ELSE amount
                    END
                )), ',', '.'), -- Remove commas
                ' ', ''
            ) AS DECIMAL(18,2)
    ) AS amount
      ,[payment_method]
      ,[fee_type]
      ,[academic_year]
      ,[receipt_number]
      ,[processed_by]
      ,[status]
  FROM [University].[bronze].[financial_transactions]
  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
--=============================================================================================================================
-- Loading silver.course_data

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.course_data';
		TRUNCATE TABLE silver.course_data;
		PRINT '>> Inserting Data Into: silver.course_data';
		INSERT INTO silver.course_data (
	[course_code] ,
	[course_name] ,
	[department] ,
	[faculty] ,
	[credits] ,
	[semester] ,
	[lecturer_id] ,
	[prerequisites] ,
	[course_description] ,
	[nqf_level] ,
	[enrollment_cap] ,
	[course_fee] 
		)
  SELECT trim(course_code) as course_code
      ,trim(course_name) as course_name
      ,trim(department) as department
      ,trim(faculty) as faculty
      ,[credits]
      ,[semester]
      ,trim(lecturer_id) as lecturer_id
      ,trim(prerequisites) as prerequisites
      ,trim(course_description) as course_description
      ,[nqf_level]
      ,[enrollment_cap]
      ,[course_fee]
  FROM [University].[bronze].[course_data]
SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

--=============================================================================================================================
-- Loading silver.research_grants

        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.research_grants';
		TRUNCATE TABLE silver.research_grants;
		PRINT '>> Inserting Data Into: silver.research_grants';
		INSERT INTO silver.research_grants (
	[grant_id],
	[project_title],
	[principal_investigator],
	[department],
	[funding_agency],
	[currency],
	[grant_amount],
	[start_date],
	[end_date],
	[research_field],
	[project_status],
	[funding_type],
	[collaborating_institutions],
	[student_involvement],
	[phd_count],
	[masters_count],
	[honours_count],
	[generic_postgrad_count],
	[total],
	[ethical_clearance]
		)
SELECT trim(grant_id) as grant_id
      ,trim(project_title) as project_title
      ,trim(principal_investigator) as principal_investigator
      ,trim(department) as department
      ,trim(funding_agency) as funding_agency
      , -- Determine currency
    CASE
        WHEN grant_amount LIKE 'R %' OR grant_amount LIKE 'ZAR%' THEN 'Rands'
        WHEN grant_amount LIKE '$%' OR grant_amount LIKE 'USD%' THEN 'United States Dollars'
        ELSE 'Unknown'
    END AS currency,

    -- Clean and convert the amount safely
    CAST(
        FLOOR(
            TRY_CAST(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(grant_amount, 'ZAR', ''),
                                'R', ''),
                            '$', ''),
                        'USD', ''),
                    ',', ''),
                ' ', '') AS FLOAT
            )
        ) AS INT
    ) AS grant_amount
      ,CASE	
        WHEN start_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, start_date, 103) -- dd/mm/yyyy
        WHEN start_date LIKE '____/__/__' THEN TRY_CONVERT(DATE, start_date, 111) -- yyyy/mm/dd
        WHEN start_date LIKE '__-__-____' THEN TRY_CONVERT(DATE, start_date, 105) -- dd-mm-yyyy
        WHEN start_date LIKE '___-__-__' THEN TRY_CONVERT(DATE, start_date, 106) -- dd-Mon-yy
		WHEN start_date LIKE '__-___-____' THEN TRY_CONVERT(DATE, start_date, 106) -- dd-MMM-yyyy
        WHEN start_date LIKE '____-__-__' THEN TRY_CONVERT(DATE, start_date, 120)
		WHEN start_date LIKE '__-___-__' THEN TRY_CONVERT(DATE, start_date, 6) -- yyyy-mm-dd
        ELSE NULL
    END AS started_date
      ,CASE	
        WHEN end_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, end_date, 103) -- dd/mm/yyyy
        WHEN end_date LIKE '____/__/__' THEN TRY_CONVERT(DATE, end_date, 111) -- yyyy/mm/dd
        WHEN end_date LIKE '__-__-____' THEN TRY_CONVERT(DATE, end_date, 105) -- dd-mm-yyyy
        WHEN end_date LIKE '___-__-__' THEN TRY_CONVERT(DATE, end_date, 106) -- dd-Mon-yy
		WHEN end_date LIKE '__-___-____' THEN TRY_CONVERT(DATE, end_date, 106) -- dd-MMM-yyyy
        WHEN end_date LIKE '____-__-__' THEN TRY_CONVERT(DATE, end_date, 120)
		WHEN end_date LIKE '__-___-__' THEN TRY_CONVERT(DATE, end_date, 6) -- yyyy-mm-dd
        ELSE NULL
    END AS end_date
      ,trim(research_field) as research_field
      ,trim(project_status) as project_status
      ,[funding_type]
	  ,[collaborating_institutions]
      ,student_involvement
	  ,
    -- Extract number of PhD students
    ISNULL(TRY_CAST(
        LEFT(
            LTRIM(
                SUBSTRING(student_involvement, 
                          CHARINDEX('PhD', student_involvement) - 3, 
                          3)
            ), 1
        ) AS INT), 0) AS phd_count,

    -- Extract number of Masters students
    ISNULL(TRY_CAST(
        LEFT(
            LTRIM(
                SUBSTRING(student_involvement, 
                          CHARINDEX('Masters', student_involvement) - 3, 
                          3)
            ), 1
        ) AS INT), 0) AS masters_count,

    -- Extract number of Honours students
    ISNULL(TRY_CAST(
        LEFT(
            LTRIM(
                SUBSTRING(student_involvement, 
                          CHARINDEX('Honours', student_involvement) - 3, 
                          3)
            ), 1
        ) AS INT), 0) AS honours_count,

    -- Extract number of Postgraduates (if no PhD or Masters mentioned)
    CASE 
        WHEN student_involvement LIKE '%Postgraduate%' 
             AND student_involvement NOT LIKE '%PhD%' 
             AND student_involvement NOT LIKE '%Masters%' 
        THEN 
            ISNULL(TRY_CAST(
                LEFT(student_involvement, CHARINDEX(' ', student_involvement) - 1) 
            AS INT), 0)
        ELSE 0
    END AS generic_postgrad_count,

    -- Total number of students
    (
        ISNULL(TRY_CAST(
            LEFT(
                LTRIM(
                    SUBSTRING(student_involvement, 
                              CHARINDEX('PhD', student_involvement) - 3, 
                              3)
                ), 1
            ) AS INT), 0)
        +
        ISNULL(TRY_CAST(
            LEFT(
                LTRIM(
                    SUBSTRING(student_involvement, 
                              CHARINDEX('Masters', student_involvement) - 3, 
                              3)
                ), 1
            ) AS INT), 0)
        +
        ISNULL(TRY_CAST(
            LEFT(
                LTRIM(
                    SUBSTRING(student_involvement, 
                              CHARINDEX('Honours', student_involvement) - 3, 
                              3)
                ), 1
            ) AS INT), 0)
        +
        CASE 
            WHEN student_involvement LIKE '%Postgraduate%' 
                 AND student_involvement NOT LIKE '%PhD%' 
                 AND student_involvement NOT LIKE '%Masters%' 
            THEN 
                ISNULL(TRY_CAST(
                    LEFT(student_involvement, CHARINDEX(' ', student_involvement) - 1) 
                AS INT), 0)
            ELSE 0
        END
    ) AS total_students
      ,trim(ethical_clearance) as ethical_clearance
  FROM [University].[bronze].[research_grants]
  SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

  --=============================================================================================================================
-- Loading silver.[student_registrations]

SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.[student_registrations]';
		TRUNCATE TABLE silver.[student_registrations];
		PRINT '>> Inserting Data Into: silver.[student_registrations]';
		INSERT INTO silver.[student_registrations] (
	[student_id] ,
	[first_name] ,
	[surname] ,
	[id_number] ,
	[date_of_birth] ,
	[gender] ,
	[email] ,
	[cellphone] ,
	[registration_date] ,
	[enrollment_status] ,
	[nationality] ,
	[province] ,
	[program_code] ,
	[study_year] ,
	[campus] 
		)
  SELECT 'STU-' + RIGHT('000000' + 
        REPLACE(REPLACE(REPLACE(trim(student_id), 'STU-', ''), 'STU', ''), '-', ''), 
    6) AS student_id
      ,trim([first_name]) as first_name
      ,trim([surname]) as surname
      ,CAST([id_number] as bigint) as id_number
      ,CONVERT(VARCHAR(10), 
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
      ,CASE WHEN gender in ('F','Female') then 'Female'
     ELSE 'Male'
     END as gender
      ,CASE 
        WHEN email IS NULL OR CHARINDEX(' ', email) > 0 OR CHARINDEX('@', email) = 0 
        THEN NULL 
        ELSE email 
        END AS email
      ,CASE 
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
      ,CONVERT(VARCHAR(10), 
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
      ,CASE WHEN enrollment_status IN ('ACTIVE','Active','active') THEN 'Active'
     WHEN enrollment_status IN ('INACTIVE','Inactive','inactive') THEN 'Inactive'
     WHEN enrollment_status IN ('PENDING','Pending','pending') THEN 'Pending'
	 WHEN enrollment_status IN ('SUSPENDED','Suspended','suspended') THEN 'Suspended'
 ELSE enrollment_status
 END AS enrollment_status
      ,CASE WHEN nationality IN ('SA','South Africa','South African','ZA') THEN 'South African'
     WHEN nationality IN ('Zimbabwe') THEN 'Zimbabwean'
	 ELSE nationality
END AS nationality
      ,trim(province) as province
      ,trim(program_code) as program_code
      ,CAST([study_year] as INT) as study_year
      ,CASE WHEN campus IN ('Main','Main Campus') THEN 'Main Campus'
ELSE campus
END AS campus
  FROM [University].[bronze].[student_registrations]
SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
  --=============================================================================================================================
