--quality check for loading into silver for staff_information
--==========================================================================================================================
-- staff_id column

select staff_id
from bronze.staff_information
where staff_id != trim(staff_id)

---Result : Nothing, confirm no trailing spaces

select distinct staff_id
from bronze.staff_information

-- 39 rows returned which is equal to count of all records in the table confirming that all staff-ids are distinct 

--============================================================================================================================
---first name column

select *
from bronze.staff_information
where first_name is NULL
--one of the rows has first_name NULL for staff_id STAFF-6078
-- this can be filled in latr during data integration if any of the other tables provides the missing information

select first_name
from bronze.staff_information
where first_name != trim(first_name)
---Result : Nothing, confirm no trailing spaces

--============================================================================================================================
---last name column

select *
from bronze.staff_information
where last_name is NULL
--No null record for last_name

select first_name
from bronze.staff_information
where last_name != trim(last_name)
---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--date_of_birth

select date_of_birth
from bronze.staff_information

--date_of_birth of column contains with various formats such as '12-Aug-75','10/01/1980' which can be standardised using the query below
 select   CASE
        WHEN TRY_CAST(date_of_birth AS DATE) IS NOT NULL THEN TRY_CAST(date_of_birth AS DATE) -- ISO format
        WHEN TRY_CONVERT(DATE, date_of_birth, 103) IS NOT NULL THEN TRY_CONVERT(DATE, date_of_birth, 103) -- dd/mm/yyyy
        WHEN TRY_CONVERT(DATE, date_of_birth, 105) IS NOT NULL THEN TRY_CONVERT(DATE, date_of_birth, 105) -- dd-mm-yyyy
        WHEN TRY_CONVERT(DATE, date_of_birth, 106) IS NOT NULL THEN TRY_CONVERT(DATE, date_of_birth, 106) -- dd-MMM-yy (e.g., 15-Jul-79)
        ELSE NULL
    END
	from bronze.staff_information

--==============================================================================================================================
--gender

select distinct gender
from bronze.staff_information

--gender
--F
--Female
--M
--Male
--results show 4 different values for 2 options and need to be standardised to be just Male and Female

select 
CASE WHEN gender in ('F','Female') then 'Female'
     ELSE 'Male'
END as gender
from bronze.staff_information

--==============================================================================================================================
--email
select email
from bronze.staff_information
where email != trim(email)
---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--contact_number

select contact_number
from bronze.staff_information

--- phone numbers are in different formats and need to be standardised to for example '+27 82 111 2222' 

select
    CASE 
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
	from bronze.staff_information;

--==============================================================================================================================
--department

select department
from bronze.staff_information
where department != trim(department)
---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--faculty

select faculty
from bronze.staff_information
where faculty != trim(faculty)
---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--position

select position
from bronze.staff_information
where position != trim(position)
---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--employment_type

select employment_type
from bronze.staff_information
where employment_type != trim(employment_type)
---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--start_date

-- start_date of column contains with various formats such as '12-Aug-75','10/01/1980' which can be standardised using the query below
 select   CASE
        WHEN TRY_CAST(start_date AS DATE) IS NOT NULL THEN TRY_CAST(start_date AS DATE) -- ISO format
        WHEN TRY_CONVERT(DATE, start_date, 103) IS NOT NULL THEN TRY_CONVERT(DATE, start_date, 103) -- dd/mm/yyyy
        WHEN TRY_CONVERT(DATE, start_date, 105) IS NOT NULL THEN TRY_CONVERT(DATE, start_date, 105) -- dd-mm-yyyy
        WHEN TRY_CONVERT(DATE, start_date, 106) IS NOT NULL THEN TRY_CONVERT(DATE, start_date, 106) -- dd-MMM-yy (e.g., 15-Jul-79)
        ELSE NULL
    END as started_date
	from bronze.staff_information

--==============================================================================================================================
--end_date

-- end_date of column contains with various formats such as '12-Aug-75','10/01/1980' which can be standardised using the query below
 select   CASE
        WHEN TRY_CAST(end_date AS DATE) IS NOT NULL THEN TRY_CAST(end_date AS DATE) -- ISO format
        WHEN TRY_CONVERT(DATE, end_date, 103) IS NOT NULL THEN TRY_CONVERT(DATE, end_date, 103) -- dd/mm/yyyy
        WHEN TRY_CONVERT(DATE, end_date, 105) IS NOT NULL THEN TRY_CONVERT(DATE, end_date, 105) -- dd-mm-yyyy
        WHEN TRY_CONVERT(DATE, end_date, 106) IS NOT NULL THEN TRY_CONVERT(DATE, end_date, 106) -- dd-MMM-yy (e.g., 15-Jul-79)
        ELSE NULL
    END as end_date
	from bronze.staff_information

-- end_date of NULL means the person is still part of the staff

--==============================================================================================================================
--qualification

select qualification
from bronze.staff_information
where qualification != trim(qualification)
---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--office_location

select  office_location
from bronze.staff_information
where office_location != trim(office_location)
---Result : Nothing, confirm no leading/trailing spaces






