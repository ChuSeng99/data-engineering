# Data Pipeline Portfolio Project

## Overview

This project demonstrates a complete data pipeline built using PostgreSQL to process data through a multi-layered architecture: Bronze, Silver, and Gold. The pipeline ingests raw data from CSV files, transforms and cleanses it, and produces business-ready datasets for analytics and reporting. The project showcases skills in database design, ETL (Extract, Transform, Load) processes, and data warehousing concepts like the Star Schema.

## Project Structure

The pipeline is divided into three layers:

* **Bronze Layer:** Stores raw, unprocessed data loaded from external CSV files.
* **Silver Layer:** Contains cleansed and transformed data derived from the Bronze layer.
* **Gold Layer:** Provides business-ready views (dimension and fact tables) in a Star Schema for analytics and reporting.

## Scripts and Components

The project includes the following SQL scripts and stored procedures:

### 1. Bronze Layer

* **DDL Script: `Create Bronze Tables`**
    * **Purpose:** Defines the schema and creates tables in the `bronze` schema, dropping existing tables if they exist.
    * **Usage:** Run this script to set up or reset the Bronze layer table structure.

* **Stored Procedure: `bronze.load_bronze`**
    * **Purpose:** Loads raw data from CSV files into the Bronze tables.
    * **Actions:**
        * Truncates existing data in Bronze tables.
        * Uses the PostgreSQL `COPY` command to load data from CSV files.
    * **Prerequisites (Windows OS):**
        * Place the CSV files in a directory (e.g., `C:/pgsql_io`).
        * Grant the PostgreSQL service account (default: `NETWORK SERVICE`) access to the directory: `Folder > Properties > Security > Edit > Add > Enter NETWORK SERVICE`.

    * **Usage Example:**
        ```sql
        CALL bronze.load_bronze;
        ```

### 2. Silver Layer

* **DDL Script: `Create Silver Tables`**
    * **Purpose:** Defines the schema and creates tables in the `silver` schema, dropping existing tables if they exist.
    * **Usage:** Run this script to set up or reset the Silver layer table structure.

* **Stored Procedure: `silver.load_silver`**
    * **Purpose:** Performs ETL operations to transform and load data from the Bronze layer into the Silver layer.
    * **Actions:**
        * Truncates Silver tables.
        * Inserts cleansed and transformed data from Bronze tables.
    * **Usage Example:**

        ```sql
        CALL silver.load_silver;
        ```

### 3. Gold Layer

* **DDL Script: `Create Gold Views`**
    * **Purpose:** Creates views in the Gold layer representing dimension and fact tables in a Star Schema.
    * **Details:**
        * Views combine and transform data from the Silver layer.
        * Produces enriched, business-ready datasets for analytics and reporting.
    * **Usage:** Query the views directly for reporting or analytics purposes.

## Prerequisites

* **Database:** PostgreSQL (version 13 or higher recommended).
* **Directory Setup:** Create a directory for CSV files (e.g., `C:/pgsql_io`).
* **File Access:** Ensure the PostgreSQL service account has read access to this directory.
* **CSV Files:** Place the input CSV files in the designated directory.
* **Permissions:** Grant the PostgreSQL service account (`NETWORK SERVICE` by default) access to the CSV directory.

## Setup Instructions

1.  **Run the Database and Schema Setup Script:**
    * Save the `S0_init_database.sql` script from the repository.
    * Run it using `psql` with a superuser account (e.g., `postgres`):

        ```bash
        psql -U postgres -f S0_init_database.sql
        ```
    * Alternatively, execute it in pgAdmin's Query Tool with a superuser account.
    * This script creates the `DataWarehouse` database and the `bronze`, `silver`, and `gold` schemas, skipping creation if they already exist.

2.  **Install PostgreSQL (if not already installed):**
    * Download and install PostgreSQL from [postgresql.org](https://www.postgresql.org/).
    * Ensure the PostgreSQL service is running.

3.  **Set Up CSV Directory:**
    * Create a directory (e.g., `C:/pgsql_io`).
    * Copy the input CSV files to this directory.

4.  **Grant NETWORK SERVICE read access to the directory (Windows):**
    * Right-click folder > `Properties` > `Security` > `Edit` > `Add` > Enter `NETWORK SERVICE` > Grant `Read` permissions.

5.  **Run DDL Scripts:**
    * Execute the Bronze and Silver DDL scripts to create the tables.
    * **Example:**
        ```sql
        \i path/to/S1_bronze_ddl.sql
        \i path/to/S2_silver_ddl.sql
        ```

6.  **Run Stored Procedures:**
    * Load data into the Bronze layer:

        ```sql
        CALL bronze.load_bronze;
        ```
    * Transform and load data into the Silver layer:

        ```sql
        CALL silver.load_silver;
        ```

7.  **Create Gold Views:**
    * Run the Gold DDL script to create the views:

        ```sql
        \i path/to/S3_gold.sql
        ```

8.  **Query the Gold Layer:**
    * Query the Gold views for analytics and reporting:

        ```sql
        SELECT * FROM gold.fact_sales;
        ```

## Notes

* **OS Compatibility:** The `COPY` command instructions are tailored for Windows. For Linux/Mac, adjust file paths and permissions accordingly.

## Future Improvements

* Implement incremental loading for large datasets.
* Automate the pipeline using a scheduler (e.g., Apache Airflow).

## Credits

DataWithBaraa - https://www.youtube.com/watch?v=9GVqKuTVANE