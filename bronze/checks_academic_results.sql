--quality check for loading into silver for academic_results
--==========================================================================================================================
-- staff_id column

select student_id
from bronze.academic_results
where student_id != trim(student_id)

---Result : Nothing, confirm no leading/trailing spaces

SELECT 
    'STU-' + RIGHT('000000' + 
        REPLACE(REPLACE(REPLACE(student_id, 'STU-', ''), 'STU', ''), '-', ''), 
    6) AS standardized_id
FROM bronze.academic_results;

-- student_id column contains various formats such as 'STU-100123','100124','STU100125' which should all be standardised to 'STU-######'  

--============================================================================================================================
---course_code

select course_code
from bronze.academic_results
where course_code != trim(course_code)

---Result : Nothing, confirm no leading/trailing spaces
---No NULL values

--============================================================================================================================
---assessment_type
select assessment_type
from bronze.academic_results
where assessment_type != trim(assessment_type)
---Result : Nothing, confirm no leading/trailing spaces

select distinct assessment_type
from bronze.academic_results

-- low cardinality and properly named

--============================================================================================================================
---mark

select mark
from bronze.academic_results
where mark is NULL
--No null record for mark

select mark
from bronze.academic_results
where mark < 0 or mark > 100
---Result : ERROR in both GRADE and STATUD column, returns value of ERROR when values are negative or greater than 100

--==============================================================================================================================
--grade

select grade
from bronze.academic_results
where grade != trim(grade)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--status

select status
from bronze.academic_results
where status != trim(status)

---Result : Nothing, confirm no leading/trailing spaces
--==============================================================================================================================
--submission_date

select submission_date
from bronze.academic_results

--submission_date column contains with DATETIME where the time element is all zeros. We would want to only return the date

 SELECT CAST(submission_date AS DATE) AS submission_date
FROM bronze.academic_results;


--==============================================================================================================================
--examiner

select examiner
from bronze.academic_results
where status != trim(status)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--moderator

select moderator
from bronze.academic_results
where moderator != trim(moderator)

---Result : Nothing, confirm no leading/trailing spaces


