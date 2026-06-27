-- ==============================================================================
-- Lab 3: Flink S3 Source (Parquet) → Iceberg Sink with JSON
-- ==============================================================================
-- Reads CDC Parquet files from MinIO S3 using filesystem connector
-- Writes to Iceberg with JSON stored as STRING (REST catalog 0.5.0 limitation)
-- 
-- Architecture:
--   S3 Parquet files → Flink FileSystem Connector → Iceberg (JSON as STRING)
-- ==============================================================================

-- Flink Configuration
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.storage' = 'filesystem';
SET 'execution.checkpointing.dir' = 's3a://warehouse/flink-checkpoints/';
SET 'parallelism.default' = '2';
SET 'pipeline.name' = 'lab3-s3-to-iceberg-variant';

-- ==============================================================================
-- Step 1: Create Iceberg Catalog (REST)
-- ==============================================================================
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password',
  'warehouse' = 's3://warehouse/'
);

-- Create database
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS lab3;

-- ==============================================================================
-- Step 2: FileSystem Source - Read Parquet from S3/MinIO
-- ==============================================================================
-- Switch to default catalog for filesystem connector
USE CATALOG default_catalog;

CREATE TABLE json_source (
    event_id STRING,
    operation STRING,
    table_name STRING,
    database STRING,
    payload STRING,           -- JSON string 
    event_variant STRING,     -- Complex JSON
    ts BIGINT,
    source_timestamp STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 's3a://raw-input/',              -- ✅ Reading from MinIO S3 bucket
    'format' = 'json',
    'source.monitor-interval' = '5s'          -- Check for new files every 5 seconds
);

-- ==============================================================================
-- Step 3: Iceberg Sink with VARIANT (V3 Format)
-- ==============================================================================
USE CATALOG iceberg_catalog;

-- Create Iceberg table with VARIANT type (like lab2!)
-- Using apache/iceberg-rest-fixture which supports VARIANT
CREATE TABLE IF NOT EXISTS lab3.cdc_events (
    event_id STRING,
    operation STRING,
    table_name STRING,
    database STRING,
    payload VARIANT,          -- ✅ VARIANT type for structured JSON!
    event_variant VARIANT,    -- ✅ VARIANT type for structured JSON!
    ts BIGINT,
    source_timestamp STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'format-version' = '3',   -- ✅ V3 required for VARIANT support
    'write.format.default' = 'parquet',
    'write.upsert.enabled' = 'true'
);

-- ==============================================================================
-- Step 4: Streaming INSERT with PARSE_JSON (like lab2!)
-- ==============================================================================
-- Use PARSE_JSON() to convert STRING → VARIANT
USE CATALOG default_catalog;

INSERT INTO iceberg_catalog.lab3.cdc_events
SELECT 
    event_id,
    operation,
    table_name,
    database,
    PARSE_JSON(payload),             -- ✅ STRING → VARIANT using PARSE_JSON
    PARSE_JSON(event_variant),       -- ✅ STRING → VARIANT using PARSE_JSON
    ts,
    source_timestamp
FROM json_source;

-- ==============================================================================
-- 🎯 Expected Result:
-- ==============================================================================
--   ✅ Flink continuously monitors s3://raw-input/ for new Parquet files
--   ✅ Reads CDC events with nested JSON payloads from MinIO S3
--   ✅ Stores JSON as STRING (REST catalog 0.5.0 doesn't support VARIANT)
--   ✅ Writes to Iceberg table (1:1 schema mapping)
--   
--   Query JSON fields in Flink:
--     SELECT 
--       event_id, 
--       operation,
--       JSON_VALUE(payload, '$.order_id') as order_id,
--       JSON_VALUE(payload, '$.customer.name') as customer_name,
--       JSON_VALUE(payload, '$.customer.tier') as tier
--     FROM iceberg_catalog.lab3.cdc_events
--   
--   Add more files:
--     docker exec lab3-python python producer.py
--     
--   Note: VARIANT type requires newer Iceberg REST catalog (1.5.0+)
-- ==============================================================================
