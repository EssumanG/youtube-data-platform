-- Create Metabase user
CREATE USER metabase_user WITH PASSWORD 'metabase_pass';

-- Create Metabase database
CREATE DATABASE metabase_db OWNER metabase_user;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE metabase_db TO metabase_user;
