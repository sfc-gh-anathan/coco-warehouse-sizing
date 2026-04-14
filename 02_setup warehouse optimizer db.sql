-- 1. Create the environment
CREATE DATABASE IF NOT EXISTS WAREHOUSE_OPTIMIZER;
CREATE SCHEMA IF NOT EXISTS WAREHOUSE_OPTIMIZER.MANAGEMENT;

-- 2. Create the Streamlit object (the "container")
-- Note: In the UI, you'd just click "Create Streamlit," 
-- but this is what happens under the hood.
CREATE OR REPLACE STREAMLIT WAREHOUSE_OPTIMIZER.MANAGEMENT.FINOPS_DASHBOARD
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = 'WAREHOUSE_OPTIMIZER_WH'
  TITLE = 'FinOps Dashboard - Warehouse Cost Optimizer'
  COMMENT = 'Warehouse right-sizing recommendations based on query history analysis';


