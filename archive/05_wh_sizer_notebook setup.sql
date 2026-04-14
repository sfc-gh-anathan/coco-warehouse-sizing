/*
=============================================================================
 Warehouse Sizer — Cost Optimizer  |  Setup Script
=============================================================================
 PURPOSE
   This script creates all Snowflake objects needed to run the
   "Warehouse Sizer — Cost Optimizer" Streamlit application.

 PREREQUISITES
   - ACCOUNTADMIN role (or a role with CREATE DATABASE, CREATE STREAMLIT,
     and access to SNOWFLAKE.ACCOUNT_USAGE views).
   - A running warehouse (the script uses SNOW_INTELLIGENCE_DEMO_WH by default;
     change it below if your account uses a different warehouse).

 WHAT IT CREATES
   1. Database   : COST_MANAGEMENT
   2. Schema     : COST_MANAGEMENT.PUBLIC  (default, already exists)
   3. Streamlit  : COST_MANAGEMENT.PUBLIC.WAREHOUSE_SIZER

 HOW TO USE
   1. Open a SQL Worksheet in Snowsight.
   2. Paste this entire script and run all statements (Ctrl+Shift+Enter).
   3. After the Streamlit object is created, upload the streamlit_app.py file
      from this workspace into the Streamlit's live version (see Step 4 below).
   4. Navigate to Projects > Streamlit > WAREHOUSE_SIZER to open the app.
=============================================================================
*/

-- =========================================================================
-- Step 1: Set context
-- =========================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE SNOW_INTELLIGENCE_DEMO_WH;

-- =========================================================================
-- Step 2: Create the database (if it doesn't already exist)
-- =========================================================================
CREATE DATABASE IF NOT EXISTS COST_MANAGEMENT;
USE DATABASE COST_MANAGEMENT;
USE SCHEMA PUBLIC;

-- =========================================================================
-- Step 3: Create the Streamlit application object
--   • QUERY_WAREHOUSE controls which warehouse the Streamlit app uses
--     to run its SQL queries against ACCOUNT_USAGE views.
--   • TITLE is what appears in the Snowsight Streamlit listing.
--   • The main file defaults to "streamlit_app.py".
-- =========================================================================
CREATE OR REPLACE STREAMLIT COST_MANAGEMENT.PUBLIC.WAREHOUSE_SIZER
    QUERY_WAREHOUSE = 'SNOW_INTELLIGENCE_DEMO_WH'
    TITLE = 'Warehouse Sizer — Cost Optimizer'
    COMMENT = 'Analyzes virtual warehouse utilization and generates AI-powered sizing recommendations using Snowflake Cortex';

-- =========================================================================
-- Step 4: Create a live version so we can upload the app code
--   A live version is a writable staging area for the Streamlit's source
--   files.  We copy streamlit_app.py from the workspace into it.
-- =========================================================================
ALTER STREAMLIT COST_MANAGEMENT.PUBLIC.WAREHOUSE_SIZER ADD LIVE VERSION FROM LAST;

-- =========================================================================
-- Step 5: Upload the application code from this workspace
--   This copies streamlit_app.py from the current workspace into the
--   Streamlit object's live version, replacing the default placeholder.
-- =========================================================================
COPY FILES
    INTO 'snow://streamlit/COST_MANAGEMENT.PUBLIC.WAREHOUSE_SIZER/versions/live/'
    FROM 'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/live'
    FILES=('streamlit_app.py');

-- =========================================================================
-- Step 6: Verify the upload
--   You should see streamlit_app.py with a size of ~24 KB.
-- =========================================================================
LIST 'snow://streamlit/COST_MANAGEMENT.PUBLIC.WAREHOUSE_SIZER/versions/live/';

-- =========================================================================
-- Done!  Open the app:
--   Projects > Streamlit > WAREHOUSE_SIZER
--   or run: DESCRIBE STREAMLIT COST_MANAGEMENT.PUBLIC.WAREHOUSE_SIZER;
-- =========================================================================
