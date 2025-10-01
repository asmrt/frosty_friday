--    Frosty Friday 72用のDATABASE、SCHEMAの作成
CREATE OR REPLACE DATABASE FF72_DB;
USE DATABASE FF72_DB;

CREATE OR REPLACE SCHEMA SETUP;
USE SCHEMA SETUP;

CREATE OR REPLACE SECRET git_secret
  TYPE = password
  USERNAME = '<user_name>'
  PASSWORD = '<password>';

SHOW SECRETS;

CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('<git_url>')
  ALLOWED_AUTHENTICATION_SECRETS = (git_secret)
  ENABLED = TRUE;

SHOW INTEGRATIONS;
DESC INTEGRATION git_api_integration;

--GIT REPOSITORY ステージを作成
CREATE OR REPLACE GIT REPOSITORY frosty_friday_git_stage
  API_INTEGRATION = git_api_integration
  GIT_CREDENTIALS = git_secret
  ORIGIN = '<target_git_repository_url>';
