/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


-- start building bronze layer
-- create DDL for tables

-- crm 

-- crm_cust_info
if object_id('bronze.crm_cust_info', 'U') is not null
    drop table bronze.crm_cust_info;
go

create table bronze.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
go

-- crm_prd_info
if object_id('bronze.crm_prd_info', 'U') is not null
    drop table bronze.crm_prd_info;
go

create table bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(100),
    prd_nm       NVARCHAR(255),
    prd_cost     NVARCHAR(50),
    prd_line     NVARCHAR(10),
    prd_start_dt DATE,
    prd_end_dt   DATE
);
go

-- crm_sales_details
if object_id('bronze.crm_sales_details', 'U') is not null
    drop table bronze.crm_sales_details;
go

create table bronze.crm_sales_details (
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(100),
    sls_cust_id   INT,
    sls_order_dt  NVARCHAR(20),
    sls_ship_dt   NVARCHAR(20),
    sls_due_dt    NVARCHAR(20),
    sls_sales     NVARCHAR(50),
    sls_quantity  NVARCHAR(50),
    sls_price     NVARCHAR(50)
);
go

-- erp

-- erp_cust_az12
if object_id('bronze.erp_cust_az12', 'U') is not null
    drop table bronze.erp_cust_az12;
go

create table bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  NVARCHAR(20),
    gen    NVARCHAR(10)
);
go

-- erp_loc_a101
if object_id('bronze.erp_loc_a101', 'U') is not null
    drop table bronze.erp_loc_a101;
go

create table bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
go

-- erp_px_cat_g1v2
if object_id('bronze.erp_px_cat_g1v2', 'U') is not null
    drop table bronze.erp_px_cat_g1v2;
go

create table bronze.erp_px_cat_g1v2 (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(100),
    maintenance NVARCHAR(10)
);
go

