--quality check for loading into silver for research_grants
--==========================================================================================================================
-- grant_id column

select grant_id
from bronze.research_grants
where grant_id != trim(grant_id)

---Result : Nothing, confirm no leading/trailing spaces
--- All values are distinct

--============================================================================================================================
---project_title

select project_title
from bronze.research_grants
where project_title != trim(project_title)

---Result : Nothing, confirm no leading/trailing spaces

--============================================================================================================================
---principal_investigator

select principal_investigator
from bronze.research_grants
where principal_investigator != trim(principal_investigator)

---Result : Nothing, confirm no leading/trailing spaces

--============================================================================================================================
---department

select department
from bronze.research_grants
where department != trim(department)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--funding_agency

select funding_agency
from bronze.research_grants
where funding_agency != trim(funding_agency)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--grant_amount

select grant_amount
from bronze.research_grants
where grant_amount != trim(grant_amount)

---Result : Nothing, confirm no leading/trailing spaces


SELECT    
     -- Determine currency
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

FROM bronze.research_grants;

--==============================================================================================================================
--start_date

select start_date,
CASE	
        WHEN start_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, start_date, 103) -- dd/mm/yyyy
        WHEN start_date LIKE '____/__/__' THEN TRY_CONVERT(DATE, start_date, 111) -- yyyy/mm/dd
        WHEN start_date LIKE '__-__-____' THEN TRY_CONVERT(DATE, start_date, 105) -- dd-mm-yyyy
        WHEN start_date LIKE '___-__-__' THEN TRY_CONVERT(DATE, start_date, 106) -- dd-Mon-yy
		WHEN start_date LIKE '__-___-____' THEN TRY_CONVERT(DATE, start_date, 106) -- dd-MMM-yyyy
        WHEN start_date LIKE '____-__-__' THEN TRY_CONVERT(DATE, start_date, 120)
		WHEN start_date LIKE '__-___-__' THEN TRY_CONVERT(DATE, start_date, 6) -- yyyy-mm-dd
        ELSE NULL
    END AS started_date
from bronze.research_grants

--Standardise all dates to yyyy-mm-dd

--==============================================================================================================================
select end_date,
CASE	
        WHEN end_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, end_date, 103) -- dd/mm/yyyy
        WHEN end_date LIKE '____/__/__' THEN TRY_CONVERT(DATE, end_date, 111) -- yyyy/mm/dd
        WHEN end_date LIKE '__-__-____' THEN TRY_CONVERT(DATE, end_date, 105) -- dd-mm-yyyy
        WHEN end_date LIKE '___-__-__' THEN TRY_CONVERT(DATE, end_date, 106) -- dd-Mon-yy
		WHEN end_date LIKE '__-___-____' THEN TRY_CONVERT(DATE, end_date, 106) -- dd-MMM-yyyy
        WHEN end_date LIKE '____-__-__' THEN TRY_CONVERT(DATE, end_date, 120)
		WHEN end_date LIKE '__-___-__' THEN TRY_CONVERT(DATE, end_date, 6) -- yyyy-mm-dd
        ELSE NULL
    END AS end_date
from bronze.research_grants

--Standardise all dates to yyyy-mm-dd
--==============================================================================================================================
--research_field

select research_field
from bronze.research_grants
where research_field != trim(research_field)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--project_status

select project_status
from bronze.research_grants
where project_status != trim(project_status)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================

--funding_type

select funding_type
from bronze.research_grants
where funding_type != trim(funding_type)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================

--collaborating_institutions

select collaborating_institutions
from bronze.research_grants
where collaborating_institutions != trim(collaborating_institutions)

---Result : Nothing, confirm no leading/trailing spaces

SELECT 
    student_involvement,

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

FROM [bronze].[research_grants];

--==============================================================================================================================

--ethical_clearance

select ethical_clearance
from bronze.research_grants
where ethical_clearance != trim(ethical_clearance)

---Result : Nothing, confirm no leading/trailing spaces