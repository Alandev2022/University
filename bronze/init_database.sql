/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'University' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'University' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'University' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'University')
BEGIN
    ALTER DATABASE University SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE University;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE University;
GO

USE University;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
