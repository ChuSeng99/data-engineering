/*
===============================================================================
Script: Setup Database and Schemas
Purpose: 
    Creates the database and schemas (bronze, silver, gold) for the data pipeline.
    Includes checks to avoid errors if the database or schemas already exist.
Usage:
    Run this script in a PostgreSQL client (e.g., psql) with superuser privileges.
    Example: psql -U postgres -f 00_init_database.sql
===============================================================================
*/

-- Set client encoding to UTF-8
SET client_encoding = 'UTF8';

-- Step 1: Check and create the database if it does not exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_database WHERE datname = 'DataWarehouse'
    ) THEN
        PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE DataWarehouse');
        RAISE NOTICE 'Database DataWarehouse created successfully.';
    ELSE
        RAISE NOTICE 'Database DataWarehouse already exists.';
    END IF;
END $$;

-- Step 2: Connect to the DataWarehouse database
\c DataWarehouse

-- Step 3: Check and create schemas if they do not exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_namespace WHERE nspname = 'bronze'
    ) THEN
        CREATE SCHEMA bronze;
        RAISE NOTICE 'Schema bronze created successfully.';
    ELSE
        RAISE NOTICE 'Schema bronze already exists.';
    END IF;

    IF NOT EXISTS (
        SELECT FROM pg_namespace WHERE nspname = 'silver'
    ) THEN
        CREATE SCHEMA silver;
        RAISE NOTICE 'Schema silver created successfully.';
    ELSE
        RAISE NOTICE 'Schema silver already exists.';
    END IF;

    IF NOT EXISTS (
        SELECT FROM pg_namespace WHERE nspname = 'gold'
    ) THEN
        CREATE SCHEMA gold;
        RAISE NOTICE 'Schema gold created successfully.';
    ELSE
        RAISE NOTICE 'Schema gold already exists.';
    END IF;
END $$;

-- Step 4: Grant permissions (optional, adjust as needed)
-- Grant usage on schemas to a specific role or public
GRANT USAGE ON SCHEMA bronze, silver, gold TO public;
GRANT CREATE ON SCHEMA bronze, silver, gold TO public;

-- Step 5: Confirm setup
SELECT 'Database and schemas setup completed successfully.' AS status;
