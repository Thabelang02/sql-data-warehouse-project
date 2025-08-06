/*
============================================================================================
Stored procedure: Load Silver Layer (Bronze -> Silver)
============================================================================================

Script Purpose:
	The stored procedure performs the ETL (Extract, Transform, Load) process to the
	populate the 'silver' schema tables from the 'bronze' schema.

Action Performed:
	-Truncate Silver tables
	-Insert transformed and cleaned data from Bronze into Silver tables.

Parameters:
	None.
	This stored procedure does not accept any parameters ir eturn any values.


	Usage Example:
		EXEC Silver.load_silver
============================================================================================
*/
exec silver.load_silver

create or alter procedure silver.load_silver as
begin

	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		
		set @batch_start_time = getdate();
		print '=========================================';
		print 'Loading Silver Layer';
		print '=========================================';

		print '-----------------------------------------';
		print 'Loading the CRM Tables';
		print '-----------------------------------------';

		--Loading silver.crm_cust_info
		set @start_time = getdate();
		print 'Truncate Table: silver.crm_cust_info';
		truncate table silver.crm_cust_info;
		print '>> Inserting Data Into: silver.crm_cust_info';
		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr ,
			cst_create_date) 

		select
		cst_id,
		cst_key,

		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,


		case when upper(trim(cst_marital_status)) = 'M' then 'Married' -- Normalize ,arital status values to readable format
			when upper(trim(cst_marital_status)) = 'S' then 'Single'
			else 'n/a'
		end cst_marital_status,

 
		case when upper(trim(cst_gndr)) = 'F' then 'Female' --Normalise gender values to readable format
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end cst_gndr,

		cst_create_date
		from(
			select 
			*,
			row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info 
			where cst_id is not null
			)t
			where flag_last = 1 --Select the most recent record per customer
		
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>-----------';
		--------------------------------------------------------------------------------------------------------------------------------------------------------------
																			/*
																			=====================
																			CRM_PRD_INFO
																			=====================
																			*/

		--NB: We have to do this transformation for all the collumns by: removing duplicates
																		 --trimming unnecessary space.
																		 --normalising data into meaningfull formats
																		 --checking for nulls and negative costs amounts
																		 --Invalid Dates
		set @start_time = getdate();
		print 'Truncate Table: silver.crm_prd_info';
		truncate table silver.crm_prd_info;
		print '>> Inserting Data Into: silver.crm_prd_info';
		insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			--dwh_create_date datetime2 default getdate()
		)

		select
		prd_id,
		replace(substring(prd_key, 1, 5),'-','_' ) as cat_id, 
		substring(prd_key, 7, len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost, 0) as prd_cost,

		case upper(trim(prd_line)) --Normalise gender values to readable format
			when 'S' then 'Other Sales' 
			when  'R' then 'Road'
			when  'M' then 'Mountain'
			when  'T' then 'Touring'
			else 'n/a'
		end as prd_line,

		cast(prd_start_dt as date) as prd_start_dt,
		cast(dateadd(day, -1, lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) as date) as prd_end_dt

		from bronze.crm_prd_info
		--where 
		--substring(prd_key, 7, len(prd_key)) not in

		--(select sls_prd_key from bronze.crm_sales_info)

		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>-----------';


		--------------------------------------------------------------------------------------------------------------------------------------------------------------
		--------------------------------------------------------------------------------------------------------------------------------------------------------------
																			/*
																			=====================
																			CRM_SALES_INFO
																			=====================
																			*/

		--NB: We have to do this transformation for all the collumns by: removing duplicates
																		 --trimming unnecessary space.
																		 --normalising data into meaningfull formats
																		 --checking for nulls and negative costs amounts
																		 --Invalid Dates
																		 --Check Data consistency: Between Sales, Quantity and Price
																		--NB: Total sales = Qnty * Price
																		--NO: Negative amounts, zeros, null are not allowed

		set @start_time = getdate();
		print 'Truncate Table: silver.crm_sales_info';
		truncate table silver.crm_sales_info;
		print '>> Inserting Data Into: silver.crm_sales_info';
		insert into silver.crm_sales_info(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price 
			)
		select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id ,

		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			else cast(cast(sls_order_dt as varchar) as date) -- double casting
		end as sls_order_dt,

		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
			else cast(cast(sls_ship_dt as varchar) as date) -- double casting
		end as sls_ship_dt, 

		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
			else cast(cast(sls_due_dt as varchar) as date) -- double casting
		end as sls_due_dt, 

		case when sls_sales is null or sls_sales != sls_quantity*sls_price *abs(sls_price)
				then sls_quantity*abs(sls_price)
			else sls_sales
		end as sls_sales,
		sls_quantity,

		case when sls_price is null or sls_price <=0 
				then sls_sales / nullif(sls_quantity,0)
			else sls_price
		end as sls_price
		from bronze.crm_sales_info
		--where
		--sls_ord_num != trim(sls_ord_num)
		--sls_prd_key not in (select prd_key from silver.crm_prd_info)
		--sls_prd_key not in (select cst_id from silver.crm_cust_info)
	
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>-----------';
		--------------------------------------------------------------------------------------------------------------------------------------------------------
																			/*
																			=====================
																			erp_cust_az12
																			=====================
																			*/
		--NB: We have to do this transformation for all the collumns by: --trimming unnecessary space.
																		 --normalising data into meaningfull formats
																		 --checking for nulls 
																		 --Invalid Dates
																 
		print '-----------------------------------------';
		print 'Loading the ERP Tables';
		print '-----------------------------------------';

		set @start_time = getdate();
		print 'Truncate Table: silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;
		print '>> Inserting Data Into: silver.erp_cust_az12';
		insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		select

		case when cid like 'NAS%' then substring(cid,4, len(cid))
			else cid
		end as cid,

		case when bdate > getdate() then null
			else bdate
		end as bdate,

		case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
			when upper(trim(gen)) in ('M', 'MALE') then 'Male'
			else 'n/a'
		end as gen

		from bronze.erp_cust_az12
		--where 
		--case when cid like 'NAS%' then substring(cid,4, len(cid))
		--	else cid
		--	end not in(select distinct cst_key from silver.crm_cust_info)

		--select * from [silver].[crm_cust_info]
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>-----------';

		--------------------------------------------------------------------------------------------------------------------------------------------------------
																			/*
																			=====================
																			erp_cust_az12
																			=====================
																			*/
		--NB: We have to do this transformation for all the collumns by: --trimming unnecessary space.
																		 --normalize and handle missing or blank country codes
		set @start_time = getdate();
		print 'Truncate Table: silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;
		print '>> Inserting Data Into: silver.erp_loc_a101';
		insert into silver.erp_loc_a101(
		cid,
		cntry
		)
		Select 
		replace(cid, '-', '') cid,

		case when trim(cntry) = 'DE' then 'Germany'
			when trim(cntry) = 'US' then 'United States'
			when trim(cntry) = '' or cntry is null then 'n/a'
			else trim(cntry)
		end as cntry
		from bronze.erp_loc_a101 --where replace(cid, '-', '') not in

		--(select cst_key from silver.crm_cust_info)
		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>-----------';
		--------------------------------------------------------------------------------------------------------------------------------------------------------
																			/*
																			=====================
																			bronze.erp_epx_catg1v2
																			=====================
																			*/
		--NB: We have to do this transformation for all the collumns by: --trimming unnecessary space.
																		 --normalize and handle missing or blank country codes
		set @start_time = getdate();
		print 'Truncate Table: silver.erp_epx_catg1v2';
		truncate table silver.erp_epx_catg1v2;
		print '>> Inserting Data Into: silver.erp_epx_catg1v2';
		insert into silver.erp_epx_catg1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		select
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_epx_catg1v2

		set @end_time = getdate();
		print '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>-----------';
	
		set @batch_end_time = getdate();
		print '==============================================';
		print 'Loading Silver Layer is Complete'
		print ' -Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print '==============================================';
	end try

	begin catch
		print '==============================================';
		print 'ERROR OCCURED DURING LOADING THE SILVER LAYER'
		print 'Error Message' + error_message();
		print 'Error Message' + cast (error_number() as nvarchar);
		print 'Error Message' + cast(error_state() as nvarchar);
		print '==============================================';
	end catch
end
