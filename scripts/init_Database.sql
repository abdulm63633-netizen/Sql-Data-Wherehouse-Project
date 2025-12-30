/*
===============================================================================
Create Database and Schemas
===============================================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking 
    if it already exists. If the database exists, it is dropped and recreated. 
    Additionally, the script sets up three schemas within the database: 
    'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
===============================================================================
*/

USE master;
GO

-- 1. Drop and Recreate the Database
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- 2. Create Medallion Schemas
-- Bronze: Raw data from ERP and CRM CSV files.
CREATE SCHEMA bronze;
GO

-- Silver: Cleaned and standardized data (Resolving Data Quality issues).
CREATE SCHEMA silver;
GO

-- Gold: Optimized for BI reporting (Sales Trends, Customer Behavior).
CREATE SCHEMA gold;
GO
