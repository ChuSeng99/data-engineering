/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables. (Windows OS)
		- Migrate the datasets folder to a specific directory (e.g., C:/pgsql_io)
    	- Grant PostgreSQL service account ( DEFAULT: NETWORK SERVICE ) access to the directory
		- Folder > Properties > Security > Edit > Add > Enter Name

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze;
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze ()
LANGUAGE plpgsql
AS $$
BEGIN
	TRUNCATE TABLE bronze.crm_cust_info;
	COPY bronze.crm_cust_info
	FROM 'C:/pgsql_io/data-warehouse/datasets/source_crm/cust_info.csv'
	WITH (
		FORMAT csv, 
		HEADER true, 
		DELIMITER ',', 
		ENCODING 'UTF8',
		NULL ''
	);
	
	TRUNCATE TABLE bronze.crm_prod_info;
	COPY bronze.crm_prod_info
	FROM 'C:/pgsql_io/data-warehouse/datasets/source_crm/prd_info.csv'
	WITH (
		FORMAT csv, 
		HEADER true, 
		DELIMITER ',', 
		ENCODING 'UTF8',
		NULL ''
	);
	
	TRUNCATE TABLE bronze.crm_sales_details;
	COPY bronze.crm_sales_details
	FROM 'C:/pgsql_io/data-warehouse/datasets/source_crm/sales_details.csv'
	WITH (
		FORMAT csv, 
		HEADER true, 
		DELIMITER ',', 
		ENCODING 'UTF8',
		NULL ''
	);
	
	TRUNCATE TABLE bronze.erp_cust_az12;
	COPY bronze.erp_cust_az12
	FROM 'C:/pgsql_io/data-warehouse/datasets/source_erp/CUST_AZ12.csv'
	WITH (
		FORMAT csv, 
		HEADER true, 
		DELIMITER ',', 
		ENCODING 'UTF8',
		NULL ''
	);
	
	TRUNCATE TABLE bronze.erp_loc_a101;
	COPY bronze.erp_loc_a101
	FROM 'C:/pgsql_io/data-warehouse/datasets/source_erp/LOC_A101.csv'
	WITH (
		FORMAT csv, 
		HEADER true, 
		DELIMITER ',', 
		ENCODING 'UTF8',
		NULL ''
	);
	
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	COPY bronze.erp_px_cat_g1v2
	FROM 'C:/pgsql_io/data-warehouse/datasets/source_erp/PX_CAT_G1V2.csv'
	WITH (
		FORMAT csv, 
		HEADER true, 
		DELIMITER ',', 
		ENCODING 'UTF8',
		NULL ''
	);
END;
$$;

-- CALL bronze.load_bronze();