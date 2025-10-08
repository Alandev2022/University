
IF OBJECT_ID('silver.academic_results', 'U') IS NOT NULL
    DROP TABLE silver.academic_results;
GO
CREATE TABLE [silver].[academic_results](
	[student_id] [nvarchar](50) NULL,
	[course_code] [nvarchar](50) NULL,
	[academic_year] [int] NULL,
	[semester] [nvarchar](50) NULL,
	[assessment_type] [nvarchar](50) NULL,
	[mark] [float] NULL,
	[grade] [nvarchar](50) NULL,
	[status] [nvarchar](50) NULL,
	[submission_date] [date] NULL,
	[examiner] [nvarchar](50) NULL,
	[moderator] [nvarchar](50) NULL,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.campus_facilities', 'U') IS NOT NULL
    DROP TABLE silver.campus_facilities;
GO

CREATE TABLE [silver].[campus_facilities](
	[facility_id] [nvarchar](50) NULL,
	[facility_name] [nvarchar](50) NULL,
	[building] [nvarchar](50) NULL,
	[campus] [nvarchar](50) NULL,
	[capacity] [int] NULL,
	[facility_type] [nvarchar](50) NULL,
	[availability] [nvarchar](50) NULL,
	[booking_contact] [nvarchar](50) NULL,
	[maintenance_status] [nvarchar](50) NULL,
	[last_service_date] [date] NULL,
	[accessibility] [nvarchar](50) NULL,
	[equipment] [nvarchar](100) NULL,
	[operating_hours] [nvarchar](50) NULL,
	[operating_hours_start_time] [time] NULL,
	[operating_hours_end_time] [time] NULL,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO


IF OBJECT_ID('silver.course_data', 'U') IS NOT NULL
    DROP TABLE silver.course_data;
GO

CREATE TABLE [silver].[course_data](
	[course_code] [nvarchar](50) NULL,
	[course_name] [nvarchar](50) NULL,
	[department] [nvarchar](50) NULL,
	[faculty] [nvarchar](50) NULL,
	[credits] [int] NULL,
	[semester] [nvarchar](50) NULL,
	[lecturer_id] [nvarchar](50) NULL,
	[prerequisites] [nvarchar](50) NULL,
	[course_description] [nvarchar](100) NULL,
	[nqf_level] [int] NULL,
	[enrollment_cap] [int] NULL,
	[course_fee] [float] NULL,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.financial_transactions', 'U') IS NOT NULL
    DROP TABLE silver.financial_transactions;
GO

CREATE TABLE [silver].[financial_transactions](
	[transaction_id] [nvarchar](50) NULL,
	[student_id] [nvarchar](50) NULL,
	[transaction_date] [date] NULL,
	[transaction_type] [nvarchar](50) NULL,
	[amount] [decimal](18,2) NULL,
	[payment_method] [nvarchar](50) NULL,
	[fee_type] [nvarchar](50) NULL,
	[academic_year] [int] NULL,
	[receipt_number] [nvarchar](50) NULL,
	[processed_by] [nvarchar](50) NULL,
	[status] [nvarchar](50) NULL,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.research_grants', 'U') IS NOT NULL
    DROP TABLE silver.research_grants;
GO

CREATE TABLE [silver].[research_grants](
	[grant_id] [nvarchar](50) NULL,
	[project_title] [nvarchar](100) NULL,
	[principal_investigator] [nvarchar](50) NULL,
	[department] [nvarchar](50) NULL,
	[funding_agency] [nvarchar](100) NULL,
	[currency] [nvarchar](50) NULL,
	[grant_amount] [int] NULL,
	[start_date] [date] NULL,
	[end_date] [date] NULL,
	[research_field] [nvarchar](50) NULL,
	[project_status] [nvarchar](50) NULL,
	[funding_type] [nvarchar](50) NULL,
	[collaborating_institutions] [nvarchar](100) NULL,
	[student_involvement] [nvarchar](50) NULL,
	[phd_count] [int] NULL,
	[masters_count] [int] NULL,
	[honours_count] [int] NULL,
	[generic_postgrad_count] [int] NULL,
	[total] [int] NULL,
	[ethical_clearance] [nvarchar](50) NULL,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
); 
GO

IF OBJECT_ID('silver.staff_information', 'U') IS NOT NULL
    DROP TABLE silver.staff_information;
GO

CREATE TABLE [silver].[staff_information](
	[staff_id] [nvarchar](50) NULL,
	[first_name] [nvarchar](50) NULL,
	[last_name] [nvarchar](50) NULL,
	[id_number] [bigint] NULL,
	[date_of_birth] [date] NULL,
	[gender] [nvarchar](50) NULL,
	[email] [nvarchar](50) NULL,
	[contact_number] [nvarchar](50) NULL,
	[department] [nvarchar](50) NULL,
	[faculty] [nvarchar](50) NULL,
	[position] [nvarchar](50) NULL,
	[employment_type] [nvarchar](50) NULL,
	[start_date] [date] NULL,
	[end_date] [date] NULL,
	[qualification] [nvarchar](50) NULL,
	[office_location] [nvarchar](100) NULL,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.student_registrations', 'U') IS NOT NULL
    DROP TABLE silver.student_registrations;
GO

CREATE TABLE [silver].[student_registrations](
	[student_id] [nvarchar](50) NULL,
	[first_name] [nvarchar](50) NULL,
	[surname] [nvarchar](50) NULL,
	[id_number] [bigint] NULL,
	[date_of_birth] [date] NULL,
	[gender] [nvarchar](50) NULL,
	[email] [nvarchar](50) NULL,
	[cellphone] [nvarchar](50) NULL,
	[registration_date] [date] NULL,
	[enrollment_status] [nvarchar](50) NULL,
	[nationality] [nvarchar](50) NULL,
	[province] [nvarchar](50) NULL,
	[program_code] [nvarchar](50) NULL,
	[study_year] [int] NULL,
	[campus] [nvarchar](50) NULL,
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO