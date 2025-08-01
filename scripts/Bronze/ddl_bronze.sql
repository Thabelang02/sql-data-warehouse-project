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
GO

-- Creating tables in the bronze Layer
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info (
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date 
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	drop table bronze.crm_prd_info;
create table bronze.crm_prd_info (
	prd_id int,
	prd_key nvarchar(50),
	prd_nm varchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt date,
	prd_end_dt date
);

IF OBJECT_ID ('bronze.crm_sales_info', 'U') IS NOT NULL
	drop table bronze.crm_sales_info;
create table bronze.crm_sales_info (
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
	cid nvarchar(50),
	cntry nvarchar(50)
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12 (
	cid nvarchar(50),
	bdate date,
	gen nvarchar(50)
);

IF OBJECT_ID ('bronze.erp_epx_catg1v2', 'U') IS NOT NULL
	drop table bronze.erp_epx_catg1v2;
create table bronze.erp_epx_catg1v2 (
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50)
);

/*
=================================================
Now we want to insert the data from our .csv files into the tables we have created. 
We can do this in a few line of queries using the BULK INSERT
=================================================
*/

declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
begin try
	
	set @batch_start_time = getdate();
	print '=========================================';
	print 'Loading Bronze Layer';
	print '=========================================';

	print '-----------------------------------------';
	print 'Loading the CRM Tables';
	print '-----------------------------------------';

	set @start_time = getdate();
	print '>> Truncating Table: bronze.crm_cust_info';
	truncate table bronze.crm_cust_info;

	print '>> Inserting Data Into: bronze.crm_cust_info';
	bulk insert bronze.crm_cust_info
	from 'C:\Users\ThabelangNcube\OneDrive - Maksure Risk Solutions\Desktop\SQL Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n', -- helps SQL Server correctly identify the end of each row in your CSV file during the BULK INSERT operation.
		tablock
	);
	set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	print '>>-----------';

	set @start_time = getdate();
	print '>> Truncating Table: bronze.crm_prd_info';
	truncate table bronze.crm_prd_info;

	print '>> Inserting Data Into: bronze.crm_prd_info';
	bulk insert bronze.crm_prd_info
	from 'C:\Users\ThabelangNcube\OneDrive - Maksure Risk Solutions\Desktop\SQL Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n', -- helps SQL Server correctly identify the end of each row in your CSV file during the BULK INSERT operation.
		tablock
	);
	set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	print '>>-----------';

	set @start_time = getdate();
	print '>> Truncating Table: bronze.crm_sales_info';
	truncate table bronze.crm_sales_info;

	print '>> Inserting Data Into: bronze.crm_sales_info';
	bulk insert bronze.crm_sales_info
	from 'C:\Users\ThabelangNcube\OneDrive - Maksure Risk Solutions\Desktop\SQL Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n', -- helps SQL Server correctly identify the end of each row in your CSV file during the BULK INSERT operation.
		tablock
	);
	set @end_time = getdate();
print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '>>-----------';

	print '-----------------------------------------';
	print 'Loading the ERP Tables';
	print '-----------------------------------------';

	set @start_time = getdate();
	print '>> Truncating Table: bronze.erp_loc_a101';
	truncate table bronze.erp_loc_a101;

	print '>> Inserting Data Into: bronze.erp_loc_a101';
	bulk insert bronze.erp_loc_a101
	from 'C:\Users\ThabelangNcube\OneDrive - Maksure Risk Solutions\Desktop\SQL Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n', -- helps SQL Server correctly identify the end of each row in your CSV file during the BULK INSERT operation.
		tablock
	);
	set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	print '>>-----------';

	set @start_time = getdate();
	print '>> Truncating Table: bronze.erp_cust_az12';
	truncate table bronze.erp_cust_az12;

	print '>> Inserting Data Into: bronze.erp_cust_az12';
	bulk insert bronze.erp_cust_az12
	from 'C:\Users\ThabelangNcube\OneDrive - Maksure Risk Solutions\Desktop\SQL Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n', -- helps SQL Server correctly identify the end of each row in your CSV file during the BULK INSERT operation.
		tablock
	);
	set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	print '>>-----------';

	set @start_time = getdate();
	print '>> Truncating Table: bronze.erp_epx_catg1v2';
	truncate table bronze.erp_epx_catg1v2;

	print '>> Inserting Data Into: bronze.erp_epx_catg1v2';
	bulk insert bronze.erp_epx_catg1v2
	from 'C:\Users\ThabelangNcube\OneDrive - Maksure Risk Solutions\Desktop\SQL Data Warehouse Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with (
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n', -- helps SQL Server correctly identify the end of each row in your CSV file during the BULK INSERT operation.
		tablock
	);
	set @end_time = getdate();
	print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	print '>>-----------';

	set @batch_end_time = getdate();
	print '==============================================';
	print 'Loading Bronze Layer is Complete'
	print ' -Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
	print '==============================================';
end try

begin catch
	print '==============================================';
	print 'ERROR OCCURED DURING LOADING THE BRONZE LAYER'
	print 'Error Message' + error_message();
	print 'Error Message' + cast (error_number() as nvarchar);
	print 'Error Message' + cast(error_state() as nvarchar);
	print '==============================================';
end catch
