--quality check for loading into silver for financial_transactions
--==========================================================================================================================
-- transaction_id column

select transaction_id
from bronze.financial_transactions
where transaction_id != trim(transaction_id)

---Result : Nothing, confirm no leading/trailing spaces
--- All values are distinct
  
--============================================================================================================================
---student_id

select student_id
from bronze.financial_transactions
where student_id != trim(student_id)

---Result : Nothing, confirm no leading/trailing spaces
---No NULL values

SELECT 
    'STU-' + RIGHT('000000' + 
        REPLACE(REPLACE(REPLACE(student_id, 'STU-', ''), 'STU', ''), '-', ''), 
    6) AS student_id
FROM bronze.financial_transactions;

-- student_id column contains various formats such as 'STU-100123','100124','STU100125' which should all be standardised to 'STU-######'


--============================================================================================================================
---transaction_date

select 
transaction_date,
CASE	
        WHEN transaction_date LIKE '__/__/____' THEN TRY_CONVERT(DATE, transaction_date, 103) -- dd/mm/yyyy
        WHEN transaction_date LIKE '____/__/__' THEN TRY_CONVERT(DATE, transaction_date, 111) -- yyyy/mm/dd
        WHEN transaction_date LIKE '__-__-____' THEN TRY_CONVERT(DATE, transaction_date, 105) -- dd-mm-yyyy
        WHEN transaction_date LIKE '___-__-__' THEN TRY_CONVERT(DATE, transaction_date, 106) -- dd-Mon-yy
		WHEN transaction_date LIKE '__-___-____' THEN TRY_CONVERT(DATE, transaction_date, 106) -- dd-MMM-yyyy
        WHEN transaction_date LIKE '____-__-__' THEN TRY_CONVERT(DATE, transaction_date, 120)
		WHEN transaction_date LIKE '__-___-__' THEN TRY_CONVERT(DATE, transaction_date, 6) -- yyyy-mm-dd
        ELSE NULL
    END AS transaction_date
from bronze.financial_transactions

--Standardise all dates

--============================================================================================================================
---transaction_type

select transaction_type
from bronze.financial_transactions
where transaction_type != trim(transaction_type)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--amount

SELECT
    -- Remove 'R ', commas and format as decimal with 2 decimal places
    CAST(
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
from bronze.financial_transactions

-- Standardise all the amounts and remove the 'R' all values should be like 1234.00

--==============================================================================================================================
--payment_method

select payment_method
from bronze.financial_transactions
where payment_method != trim(payment_method)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--fee_type

select fee_type
from bronze.financial_transactions
where fee_type != trim(fee_type)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--academic_year

select academic_year
from bronze.financial_transactions

---All looks good

--==============================================================================================================================
--receipt_number

select receipt_number
from bronze.financial_transactions
where receipt_number != trim(receipt_number)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--processed_by

select processed_by
from bronze.financial_transactions
where processed_by != trim(processed_by)

---Result : Nothing, confirm no leading/trailing spaces

--==============================================================================================================================
--status

select status
from bronze.financial_transactions
where status != trim(status)

---Result : Nothing, confirm no leading/trailing spaces
---Low cardinality column

--==============================================================================================================================

