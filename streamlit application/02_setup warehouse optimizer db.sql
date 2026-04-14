

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

use database github;
use schema public;

CREATE OR REPLACE SECRET git_secret_042026
  TYPE = password
  USERNAME = 'sfc-gh-anathan'
  PASSWORD = '';
  show secrets;

drop secret "document_ai";

CREATE OR REPLACE API INTEGRATION git_api_integration_042026
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-anathan')
  ALLOWED_AUTHENTICATION_SECRETS = (git_secret_042026)
  ENABLED = TRUE;

  show secrets;
  show integrations;
