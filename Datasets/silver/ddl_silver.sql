
/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist. 
    Run this script to re-define the DDL structure of 'silver' Tables.
===============================================================================
*/


-- Check if the table exists and drop it before creating a new one
IF OBJECT_ID('silver.crm_customer_info', 'U') IS NOT NULL 
    DROP TABLE silver.crm_customer_info;

-- Create the table structure
CREATE TABLE silver.crm_customer_info (
    cst_id             INT,             -- Unique internal ID
    cst_key            NVARCHAR(50),    -- Natural key (often from source ERP)
    cst_firstname      NVARCHAR(50),    -- Customer first name
    cst_lastname       NVARCHAR(50),    -- Customer last name
    cst_marital_status NVARCHAR(50),    -- Single, Married, etc.
    cst_gndr           NVARCHAR(50),    -- Gender (Male, Female, etc.)
    cst_create_date    DATE     ,        -- Date record was created in source

    -- metadata 
    Record_source    VARCHAR(255),
    
    load_timestamp DATETIME DEFAULT GETDATE()
);




IF OBJECT_ID('silver.crm_product_info', 'U') IS NOT NULL 
    DROP TABLE silver.crm_product_info;
CREATE TABLE silver.crm_product_info (
    prd_id       INT,             -- Internal unique product ID
    prd_key      NVARCHAR(50),
    cat_id       NVARCHAR(50),-- Natural key (often used to join category data)
    prd_nm       NVARCHAR(100),   -- Product name
    prd_cost     INT,             -- Product cost (standardized to 0 if NULL during silver)
    prd_line     NVARCHAR(50),    -- Product line classification (e.g., Mountain, Road)
    prd_start_dt DATE,            -- Start date of the product record
    prd_end_dt   DATE ,            -- End date (NULL usually indicates current record)  ,

     -- metadata 
    Record_source    VARCHAR(255),
    
    load_timestamp DATETIME DEFAULT GETDATE()

);

-- Check if the table exists and drop it before creating a new one
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL 
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),   -- Sales Order Number (Primary key of transaction)
    sls_prd_key  NVARCHAR(50),   -- Natural key for products (links to product info)
    sls_cust_id  INT,            -- Customer ID (links to customer info)
    sls_order_dt DATE,            -- Order Date (stored as INT/YYYYMMDD for raw layer)
    sls_ship_dt  DATE,            -- Shipping Date (stored as INT/YYYYMMDD)
    sls_due_dt   DATE,            -- Due Date (stored as INT/YYYYMMDD)
    sls_sales    INT,            -- Total Sales Amount (raw value)
    sls_quantity INT,            -- Quantity of items sold
    sls_price    INT  ,           -- Unit price of the item ,

     -- metadata 
    Record_source    VARCHAR(255),
    
    load_timestamp DATETIME DEFAULT GETDATE()

);




-- 1. Location Data: Links internal IDs to countries
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL 
    DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid   NVARCHAR(50),  -- Customer ID (links to customer tables)
    cntry NVARCHAR(50) ,  -- Raw Country Name or Code ,

     -- metadata 
    Record_source    VARCHAR(255),
    
    load_timestamp DATETIME DEFAULT GETDATE()

);

-- 2. ERP Customer Data: Provides birthdates and gender info
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL 
    DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid   NVARCHAR(50),  -- Customer ID (often formatted as 'NAS' + ID)
    bdate DATE,          -- Birthdate
    gen   NVARCHAR(50),   -- Gender (Raw values: 'M', 'F', or '0') ,

     -- metadata 
    Record_source    VARCHAR(255),
    
    load_timestamp DATETIME DEFAULT GETDATE()

);

-- 3. Product Category Data: Hierarchical structure for products
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL 
    DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id          NVARCHAR(50),  -- Category/Subcategory ID
    cat         NVARCHAR(50),  -- Parent Category Name
    subcat      NVARCHAR(50),  -- Subcategory Name
    maintenance NVARCHAR(50) ,  -- Maintenance flag or status ,

     -- metadata 
    Record_source    VARCHAR(255),
    
    load_timestamp DATETIME DEFAULT GETDATE()

);
