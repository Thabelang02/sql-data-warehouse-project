/*
============================================================
Create Database and Schemas
============================================================
Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if already exists.
	If the database exists, it is dropped and recretaed. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution and ensure have proper backups running this scripts.
*/

use master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database

create database DataWarehouse
GO

use DataWarehouse;
GO

-- Create the schemas

create schema bronze;
GO

create schema silver
GO

create schema gold;
