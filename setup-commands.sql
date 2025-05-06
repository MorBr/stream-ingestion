-- Create database and warehouse
CREATE DATABASE IF NOT EXISTS STREAMING_DB;
USE DATABASE STREAMING_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- Create a warehouse for processing
CREATE OR REPLACE WAREHOUSE COMPUTE_WH WITH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Create tables for storing the data

-- Enriched stock quotes table with all the enrichments
CREATE OR REPLACE TABLE ENRICHED_STOCK_QUOTES (
  ticker STRING,
  price FLOAT,
  timestamp NUMBER,
  price_ils FLOAT,
  moving_avg FLOAT
);

-- Hourly aggregated table
CREATE OR REPLACE TABLE HOURLY_AGGREGATED_QUOTES (
  ticker STRING,
  hour_timestamp NUMBER,
  quote_count INTEGER
);


-- Example query to view enriched data
SELECT * FROM ENRICHED_STOCK_QUOTES ORDER BY timestamp DESC LIMIT 10;

-- Example query to view hourly aggregations
SELECT 
  ticker,
  TO_TIMESTAMP_LTZ(hour_timestamp, 3) AS hour,
  quote_count
FROM HOURLY_AGGREGATED_QUOTES
ORDER BY hour_timestamp DESC, ticker;
