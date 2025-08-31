--quality check for loading into silver for course_data
--==========================================================================================================================
-- course_code

select course_code
from bronze.course_data
where course_code != trim(course_code)

---Result : Nothing, confirm no leading/trailing spaces
--- All values are distinct

--============================================================================================================================
--course_name

select course_name
from bronze.course_data
where course_name != trim(course_name)

---Result : Nothing, confirm no leading/trailing spaces
--- All values are distinct
--============================================================================================================================
--department

select department
from bronze.course_data
where department != trim(department)

---Result : Nothing, confirm no leading/trailing spaces
--- All values are distinct

--============================================================================================================================
--faculty

select faculty
from bronze.course_data
where faculty != trim(faculty)

---Result : Nothing, confirm no leading/trailing spaces
--- All values are distinct

--==============================================================================================================================
--credits

select credits
from bronze.course_data

---Result : All looks good

--==============================================================================================================================
--semester

select semester
from bronze.course_data

---Result : All looks good
--==============================================================================================================================
--lecturer_id

select lecturer_id
from bronze.course_data
where lecturer_id != trim(lecturer_id)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--prerequisites

select prerequisites
from bronze.course_data
where prerequisites != trim(prerequisites)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--course_description

select course_description
from bronze.course_data
where course_description != trim(course_description)

---Result : Nothing, confirm no leading/trailing spaces
---Low cardinality column

--==============================================================================================================================
--nqf_level

select nqf_level
from bronze.course_data

---Result : All looks good

--==============================================================================================================================
--enrollment_cap

select enrollment_cap
from bronze.course_data

---Result : All looks good

--==============================================================================================================================
--course_fee

select course_fee
from bronze.course_data

---Result : All looks good


