/*
============================================================================================
Quality Checks
============================================================================================

Script Purpose:
	This script performs various quality checks for data consistency, accuracy,
	and standardization across the ''silver' schema. It includes checks for:
	- Null or duplicate primary keys.
	-unwanted spaces in string fields.
	-Data Standardization & Concistency.
	-Invalid date ranges and ordeers.
	Data consistency between related fields.

	Usage Notes:
		-run these checks after data loading Silver Layer.
		-investigate and resolve any discrepancies found during the checks.
============================================================================================
*/

-------------------------------------------------------------------------------------------------------------------
                      /*
											====================
											CRM_CUST_INFO TABLE
											====================
											*/
--check for nulls or duplicates in Primary keys, as they a unique and should not be null
--expectation: no result
select 
cst_id,
count(*) 
from bronze.crm_cust_info 
group by cst_id
having count(*) > 1 or cst_id is null
GO


--Check for unwanted Spaces
--Expectation: No results
--!! If the original value is not equal to the same value after trimming, it means there are spaces.

select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname) -- we can keep checking all the values in different columns for spaces.

--Check the consistency of values in low cardinality columns (marital status and gender)
select distinct cst_marital_status
from bronze.crm_cust_info
GO

---------------------------------------------------------------------------------------------------------------------------------------

											/*
											====================
											CRM_PRD_INFO TABLE
											====================
											*/

select 
prd_key
from silver.crm_prd_info
group by prd_key
having count(*) > 1 or prd_key is null
GO

--Check for unwanted spaces
--Expectation: No Results

Select prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm)
GO

--Check if we have null or negative numbers
--Expectation: No Results

Select prd_cost
from silver.crm_prd_info
where prd_cost <0 or prd_cost is null 
GO

--Data standardization & Consistency
select distinct prd_line
from silver.crm_prd_info
GO

--Check for Invalid Date Orders
select 
* 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt --End date must not be earlier than the start date

--select * from silver.crm_prd_info

---------------------------------------------------------------------------------------------------------------------------------------

											/*
											====================
											CRM_SLS_INFO TABLE
											====================
											*/

select
sls_ord_num
from bronze.crm_sales_info
where
sls_ord_num != trim(sls_ord_num)
GO

select
sls_prd_key
from bronze.crm_sales_info
where
sls_prd_key not in (select prd_key from silver.crm_prd_info)
GO

select
sls_cust_id 
from bronze.crm_sales_info
where
sls_prd_key not in (select cst_id from silver.crm_cust_info)
GO

--Check for Invalid Dates
-- negative numbers cxan not be cast to a date
select
nullif(sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_info
where sls_ship_dt <= 0 or len(sls_ship_dt) != 8 or sls_ship_dt >20500010 --checking for outliers by validating the boundaries of the date range
GO

--Check for Invalid date Orders
select
*
from bronze.crm_sales_info
where
sls_order_dt > sls_ship_dt or
sls_order_dt > sls_due_dt
GO

--Check Data consistency: Between Sales, Quantity and Price
--NB: Total sales = Qnty * Price
--NO: Negative amounts, zeros, null are not allowed

select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_info
where 
sls_sales != sls_quantity*sls_price or
sls_sales is null or
sls_quantity is null or
sls_price is null or
sls_sales <=0 or
sls_quantity <=0 or
sls_price <=0
order by sls_sales, sls_quantity, sls_price
GO
--RULES
-- If Sales is -ve, zero, or null, derive it using the Quantity and Price.
-- If Price is zero or null, calculate it using Sales and Quantity
-- If Price is -ve, convert it to a +ve value
----------------------------------------------------------------------------------------------------------------------------------------------------

											/*
											====================
											erp_cust_az12
											====================
											*/

--Identify out-of-range dates
select distinct
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()

-- Data Standardization & Concistency
select distinct
gen
--case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
--	when upper(trim(gen)) in ('M', 'MALE') then 'Male'
--	else 'n/a'
--end as gen
from bronze.erp_cust_az12
GO

----------------------------------------------------------------------------------------------------------------------------------------------------

											/*
											====================
											erp_loc_a101
											====================
											*/

--Data Standardization & Concistency
select distinct 
cntry
from bronze.erp_loc_a101
order by cntry
GO

----------------------------------------------------------------------------------------------------------------------------------------------------

											/*
											====================
											erp_px_cat_g1v2
											====================
											*/

--Check for unwanted Spaces 
select 
* 
from bronze.erp_epx_catg1v2
where cat!= trim(cat) or subcat!= trim(subcat) or maintenance!= trim(maintenance)
GO

-- Data Standardization & Concistency
select distinct 
maintenance
from bronze.erp_epx_catg1v2
--order by cntry
GO
