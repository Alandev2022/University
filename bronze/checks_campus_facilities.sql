--quality check for loading into silver for campus_facilities
--==========================================================================================================================
-- facility_id column

select facility_id
from bronze.campus_facilities
where facility_id != trim(facility_id)

---Result : Nothing, confirm no leading/trailing spaces
--- All values are distinct

--============================================================================================================================
---facility_name

select facility_name
from bronze.campus_facilities
where facility_name != trim(facility_name)

---Result : Nothing, confirm no leading/trailing spaces
---No NULL values

--============================================================================================================================
---building
select building
from bronze.campus_facilities
where building != trim(building)

---Result : Nothing, confirm no leading/trailing spaces

--============================================================================================================================
---campus

select campus
from bronze.campus_facilities
where campus != trim(campus)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--capacity

select capacity
from bronze.campus_facilities

---Result : All looks good

--==============================================================================================================================
--facility_type

select facility_type
from bronze.campus_facilities
where facility_type != trim(facility_type)

---Result : Nothing, confirm no leading/trailing spaces
--==============================================================================================================================
--availability

select availability
from bronze.campus_facilities
where availability != trim(availability)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--booking_contact

select booking_contact
from bronze.campus_facilities
where booking_contact != trim(booking_contact)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--maintenance_status

select maintenance_status
from bronze.campus_facilities
where maintenance_status != trim(maintenance_status)

---Result : Nothing, confirm no leading/trailing spaces
---Low cardinality column

--==============================================================================================================================
--last_service_date

SELECT
last_service_date,
CASE	
        WHEN last_service_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, last_service_date, 103) -- dd/mm/yyyy
        WHEN last_service_date LIKE '____/__/__' THEN TRY_CONVERT(DATE, last_service_date, 111) -- yyyy/mm/dd
        WHEN last_service_date LIKE '__-__-____' THEN TRY_CONVERT(DATE, last_service_date, 105) -- dd-mm-yyyy
        WHEN last_service_date LIKE '___-__-__' THEN TRY_CONVERT(DATE, last_service_date, 106) -- dd-Mon-yy
		WHEN last_service_date LIKE '__-___-____' THEN TRY_CONVERT(DATE, last_service_date, 106) -- dd-MMM-yyyy
        WHEN last_service_date LIKE '____-__-__' THEN TRY_CONVERT(DATE, last_service_date, 120)
		WHEN last_service_date LIKE '__-___-__' THEN TRY_CONVERT(DATE, last_service_date, 6) -- yyyy-mm-dd
        ELSE NULL
    END AS last_service_date
FROM bronze.campus_facilities

--==============================================================================================================================
--accessibility

select accessibility
from bronze.campus_facilities
where accessibility != trim(accessibility)

---Result : Nothing, confirm no leading/trailing spaces
---Low cardinality column

--==============================================================================================================================
--equipment

select equipment
from bronze.campus_facilities
where equipment != trim(equipment)

---Result : Nothing, confirm no leading/trailing spaces
---Low cardinality column

--==============================================================================================================================
--operating_hours

select 
operating_hours
from bronze.campus_facilities


SELECT
    -- Extract Start Time, handle '24 hours' case
    CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN '00:00'
        ELSE LTRIM(RTRIM(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1)))
    END AS start_time,

    -- Extract End Time, handle '24 hours' case
    CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN '00:00'
        ELSE LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours))))
    END AS end_time

FROM bronze.campus_facilities;

SELECT
    -- Extract Start Time, handle '24 hours' case
    CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN '00:00'
        ELSE LTRIM(RTRIM(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1)))
    END AS start_time,

    -- Extract End Time, handle '24 hours' case
    CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN '00:00'
        ELSE LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours))))
    END AS end_time,

    -- Calculate duration in minutes
    CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN 1440  -- 24 hours = 1440 minutes
        ELSE 
            CASE 
                -- Handle overnight ranges where end_time < start_time (e.g., 23:00-06:00)
                WHEN TRY_CAST(LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours)))) AS TIME) 
                     < TRY_CAST(LTRIM(RTRIM(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1))) AS TIME)
                THEN 
                    1440 - DATEDIFF(
                        MINUTE,
                        TRY_CAST(LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours)))) AS TIME),
                        TRY_CAST(LTRIM(RTRIM(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1))) AS TIME)
                    )
                ELSE 
                    DATEDIFF(
                        MINUTE,
                        TRY_CAST(LTRIM(RTRIM(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1))) AS TIME),
                        TRY_CAST(LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours)))) AS TIME)
                    )
            END
    END AS duration_minutes

FROM bronze.campus_facilities;




SELECT
    -- Extract Start Time, handle '24 hours' case
    CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN '00:00'
        ELSE LTRIM(RTRIM(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1)))
    END AS start_time,

    -- Extract End Time, handle '24 hours' and '24:00' cases
    CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN '00:00'
        ELSE 
            CASE 
                WHEN LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours)))) = '24:00' 
                THEN '00:00'
                ELSE LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours))))
            END
    END AS end_time,

    -- Calculate duration in minutes, handle '24 hours' and overnight cases
    CASE 
        WHEN LOWER(LTRIM(RTRIM(operating_hours))) = '24 hours' THEN 1440  -- 24 hours = 1440 minutes
        ELSE 
            CASE 
                -- Handle overnight ranges where end_time < start_time (e.g., 23:00-06:00)
                WHEN TRY_CAST(LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours)))) AS TIME) 
                     < TRY_CAST(LTRIM(RTRIM(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1))) AS TIME)
                THEN 
                    -- Calculate overnight range duration
                    1440 - DATEDIFF(
                        MINUTE,
                        TRY_CAST(LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours)))) AS TIME),
                        TRY_CAST(LTRIM(RTRIM(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1))) AS TIME)
                    )
                ELSE 
                    -- Regular same-day range
                    DATEDIFF(
                        MINUTE,
                        TRY_CAST(LTRIM(RTRIM(LEFT(operating_hours, CHARINDEX('-', operating_hours) - 1))) AS TIME),
                        TRY_CAST(LTRIM(RTRIM(RIGHT(operating_hours, LEN(operating_hours) - CHARINDEX('-', operating_hours)))) AS TIME)
                    )
            END
    END AS duration_minutes

FROM bronze.campus_facilities;


