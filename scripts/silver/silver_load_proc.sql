/*
This script cleans the data from the bronze layer and inserts it into silver laver
*/

create or alter procedure silver.load_silver as
begin
	print 'truncating table and inserting data'
	truncate table silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date
	)

	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_material_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM (
		SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1



	print 'truncating table and inserting data'
	truncate table silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
	SELECT
	prd_id,
	REPLACE( SUBSTRING(prd_key,1,5) , '-', '_') as cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) as prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M'  THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R'  THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S'  THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T'  THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt
	FROM bronze.crm_prd_info


	print 'truncating table crm_sales_details and inserting data'
	truncate table silver.crm_sales_details;

	insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_quantity,
	sls_sales,
	sls_price
	)

	SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,

	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR ) AS DATE )
	end AS sls_order_dt,

	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR ) AS DATE )
	end AS sls_ship_dt,

	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR ) AS DATE )
	end AS sls_due_dt,
	sls_quantity,
	case when sls_sales != sls_quantity * abs(sls_price) or sls_sales is null or sls_sales <= 0
			then sls_quantity * abs(sls_price)
		else sls_sales
	end as sls_sales,

	case when sls_price is null or sls_price<=0
			then sls_sales / nullif(sls_quantity, 0)
		else sls_price
	end as sls_price
	FROM bronze.crm_sales_details




	print 'truncating table erp_cust_az12 and inserting data'
	truncate table silver.erp_cust_az12;
	insert into silver.erp_cust_az12(
	cid,
	date_of_birth,
	gender
	)
	select
	case when cid like 'NAS%' then substring(cid,4,len(cid))
		 else cid
	end as cid,
	case when date_of_birth > getdate() then NULL
		else date_of_birth
	end as date_of_birth,
	case when upper(trim(gender)) in ('F', 'Female') then 'Female'
		 when upper(trim(gender)) in ('M', 'Male') then 'Male'
		 else 'n/a'
	end as gender
	from bronze.erp_cust_az12 


	print 'truncating table erp_loc_a101 and inserting data'
	truncate table silver.erp_loc_a101;
	insert into silver.erp_loc_a101(
	cid,
	country
	)
	select 
	replace (cid, '-', '') cid,
	case when trim(country) = 'DE' then 'Germany'
		 when trim(country) in ('US', 'USA') then 'United States'
		 when trim(country) = '' or country is null then 'n/a'
		 else trim(country)
	end as country
	from bronze.erp_loc_a101



	print 'truncating table erp_px_cat_g1v2 and inserting data'
	truncate table silver.erp_px_cat_g1v2;
	insert into silver.erp_px_cat_g1v2(
	id,
	category,
	sub_category,
	maintenance
	)
	select
	id,
	category,
	sub_category,
	maintenance
	from bronze.erp_px_cat_g1v2
end
