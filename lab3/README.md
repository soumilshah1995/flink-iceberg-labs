# Lab 3: S3 File Processing with Flink → Iceberg VARIANT

**Learn continuous S3 file processing with Apache Flink 2.1 and Iceberg 1.11.0**

This lab demonstrates how to build a production-ready data pipeline that:
- 📂 Continuously monitors S3/MinIO for new Parquet files
- ⚡ Processes CDC events in real-time using Flink FileSystem connector
- 🗄️ Writes to Iceberg V3 tables with native VARIANT support
- 🔄 Handles semi-structured JSON data efficiently

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐
│   Python    │      │    Flink     │      │   Iceberg    │
│  Producer   │─────▶│  FileSystem  │─────▶│   (V3)       │
│             │      │  Connector   │      │   VARIANT    │
└─────────────┘      └──────────────┘      └──────────────┘
      │                     │                      │
      ▼                     ▼                      ▼
┌────────────────────────────────────────────────────────┐
│              MinIO S3 (Object Storage)                 │
│  • raw-input/   (source Parquet files)                 │
│  • warehouse/   (Iceberg data + metadata)              │
└────────────────────────────────────────────────────────┘
```

## Stack

| Component       | Version | Purpose                              |
| --------------- | ------- | ------------------------------------ |
| Apache Flink    | 2.1     | Stream processing engine             |
| Apache Iceberg  | 1.11.0  | Table format with VARIANT support    |
| MinIO           | Latest  | S3-compatible object storage         |
| Iceberg REST    | 1.7.1   | REST catalog for metadata management |
| Python          | 3.11    | Data generator                       |

## Features

### 🚀 FileSystem Connector
- Continuous monitoring of S3 paths (`source.monitor-interval`)
- Automatic file discovery and processing
- Support for Parquet, JSON, CSV formats
- Exactly-once processing with checkpointing

### 📊 Iceberg V3 with VARIANT
- Native support for semi-structured JSON data
- Efficient storage with schema evolution
- ACID transactions with time travel
- Optimized for analytics queries

### 🔄 CDC Event Processing
- Captures INSERT, UPDATE, DELETE operations
- Preserves nested JSON structures
- Extracts common fields for easy querying
- Handles complex nested payloads

## Quick Start

### 1. Start the Stack

```bash
# Start all services
docker compose up -d --build

# Wait for services to be healthy (~30 seconds)
docker compose ps
```

### 2. Run Complete Pipeline

```bash
# Register catalog + start pipeline + generate data
chmod +x scripts/pipeline.sh
./scripts/pipeline.sh run
```

This will:
1. Create Iceberg catalog and database
2. Start the streaming pipeline
3. Generate 3 Parquet files with 50 records each
4. Upload files to MinIO S3
5. Flink automatically processes them

### 3. Verify Results

```bash
chmod +x scripts/verify.sh
./scripts/verify.sh
```

## Manual Step-by-Step

### Step 1: Register Catalog & Database

```bash
./scripts/pipeline.sh register
```

Creates:
- Iceberg REST catalog (`iceberg_catalog`)
- Database namespace (`lab3`)

### Step 2: Start Pipeline

```bash
# Start fresh (drops existing table)
./scripts/pipeline.sh start

# Or keep existing table and data
./scripts/pipeline.sh start --keep
```

This deploys the Flink SQL job that:
- Creates FileSystem source table reading from `s3://raw-input/`
- Creates Iceberg sink table with VARIANT columns
- Starts streaming INSERT job

### Step 3: Generate Data

```bash
# Generate and upload Parquet files
docker exec lab3-python python /workspace/producer.py

# Generate more files with custom settings
docker exec -e NUM_FILES=5 -e RECORDS_PER_FILE=100 lab3-python python /workspace/producer.py
```

### Step 4: Monitor Pipeline

**Flink Web UI**: http://localhost:8081
- View running jobs
- Check task metrics
- Monitor checkpoints

**MinIO Console**: http://localhost:9001 (admin/password)
- Browse S3 buckets
- View uploaded files
- Check Iceberg data files

## Pipeline Management

```bash
# Register catalog and database
./scripts/pipeline.sh register

# Start pipeline (fresh run, drops table)
./scripts/pipeline.sh start

# Start pipeline (keep existing table)
./scripts/pipeline.sh start --keep

# Pause pipeline (keeps table and state)
./scripts/pipeline.sh pause

# Stop and cleanup (drops table)
./scripts/pipeline.sh stop

# Full workflow (register + start + data)
./scripts/pipeline.sh run

# Check job status
./scripts/pipeline.sh status
```

## Data Schema

### Input Parquet Files

```sql
CREATE TABLE s3_parquet_source (
    event_id STRING,           -- Unique event identifier
    operation STRING,          -- INSERT | UPDATE | DELETE
    table_name STRING,         -- Source table name
    database STRING,           -- Source database name
    payload STRING,            -- JSON string (order details)
    event_variant STRING,      -- JSON string (complex metadata)
    ts BIGINT,                 -- Event timestamp (ms)
    source_timestamp STRING    -- ISO timestamp
)
```

### Iceberg Output Table

```sql
CREATE TABLE lab3.cdc_events (
    event_id STRING,
    operation STRING,
    table_name STRING,
    database STRING,
    payload VARIANT,           -- Nested JSON as VARIANT
    event_variant VARIANT,     -- Complex nested structure
    ts BIGINT,
    source_timestamp STRING,
    
    -- Extracted fields (Flink 2.1 limitation workaround)
    order_id STRING,
    site_id STRING,
    product_name STRING,
    order_value STRING,
    customer_name STRING,
    customer_email STRING,
    
    PRIMARY KEY (event_id) NOT ENFORCED
)
```

## Sample Data

### Payload JSON
```json
{
  "id": "uuid",
  "order_id": "uuid",
  "site_id": "siteA",
  "product_name": "widget",
  "order_value": "250",
  "priority": "HIGH",
  "order_date": "2026-01-15",
  "customer": {
    "name": "Customer_1234",
    "email": "user@example.com",
    "tier": "gold"
  },
  "shipping": {
    "address": {
      "street": "123 Main St",
      "city": "New York",
      "state": "NY",
      "zip": "10001"
    },
    "method": "express"
  }
}
```

### Event Variant JSON
```json
{
  "schema_version": 1,
  "kind": "order",
  "user": {
    "id": "uuid",
    "labels": ["label_1", "label_2"],
    "prefs": {
      "theme": "dark",
      "notifications": true
    }
  },
  "line_items": [
    {
      "sku": "SKU-456",
      "qty": 2,
      "unit_price": "125.00"
    }
  ],
  "scores": {
    "k0": 85,
    "k1": 92
  },
  "metadata": {
    "ip_address": "192.168.1.1",
    "user_agent": "Mozilla/5.0",
    "session_id": "uuid"
  },
  "at": "2026-01-15T14:30:00",
  "note": "Sample note 1234"
}
```

## Querying Data

### Flink SQL (Basic Queries)

```sql
-- Count records
SELECT COUNT(*) FROM iceberg_catalog.lab3.cdc_events;

-- Operations breakdown
SELECT operation, COUNT(*) 
FROM iceberg_catalog.lab3.cdc_events 
GROUP BY operation;

-- Query extracted fields (VARIANT columns not supported in Flink 2.1)
SELECT 
    event_id,
    operation,
    order_id,
    site_id,
    product_name,
    customer_name,
    customer_email
FROM iceberg_catalog.lab3.cdc_events
LIMIT 10;
```

### Spark SQL (VARIANT Queries)

**Note**: Use Apache Spark 4.1+ for full VARIANT support

```sql
-- Query nested paths in VARIANT columns
SELECT 
    event_id,
    variant_get(payload, '$.customer.name', 'string') as customer_name,
    variant_get(payload, '$.customer.tier', 'string') as customer_tier,
    variant_get(payload, '$.shipping.address.city', 'string') as city,
    variant_get(event_variant, '$.kind', 'string') as event_kind,
    variant_get(event_variant, '$.user.id', 'string') as user_id,
    variant_get(event_variant, '$.line_items[0].sku', 'string') as first_sku,
    variant_get(event_variant, '$.scores.k0', 'int') as score_k0
FROM iceberg_rest.lab3.cdc_events
LIMIT 10;
```

## Configuration

### Environment Variables

#### Producer (Python)
```bash
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password
S3_BUCKET=raw-input
NUM_FILES=3
RECORDS_PER_FILE=50
```

#### Flink
```bash
# Set in docker-compose.yml
execution.checkpointing.interval=10s
parallelism.default=2
state.backend=rocksdb
```

### FileSystem Connector Options

```sql
'connector' = 'filesystem'
'path' = 's3://raw-input/'              -- S3 path to monitor
'format' = 'parquet'                    -- File format
'source.monitor-interval' = '5s'        -- Check frequency
'fs.s3a.endpoint' = 'http://minio:9000'
'fs.s3a.path.style.access' = 'true'
```

## Learning Points

### 1. **FileSystem Connector**
- Monitors S3 paths for new files
- Supports various formats (Parquet, JSON, Avro, CSV)
- Integrates with Flink's checkpointing for exactly-once

### 2. **VARIANT Type**
- Stores semi-structured JSON efficiently
- No schema required upfront
- Supports nested arrays and objects
- Optimized storage with shredding (in Spark)

### 3. **JSON Parsing in Flink**
- `PARSE_JSON(string)` converts string → VARIANT
- `JSON_VALUE(json, '$.path')` extracts scalar values
- Limitation: Flink 2.1 cannot query VARIANT on read

### 4. **Iceberg V3 Features**
- Format version 3 required for VARIANT
- ACID transactions
- Time travel queries
- Schema evolution

## Troubleshooting

### Pipeline Not Processing Files

```bash
# Check Flink job status
./scripts/pipeline.sh status

# Check Flink logs
docker logs lab3-jobmanager
docker logs lab3-taskmanager

# Verify files in S3
aws s3 ls s3://raw-input/ --endpoint-url http://localhost:9000
```

### MinIO Connection Issues

```bash
# Test S3 connection
aws s3 ls --endpoint-url http://localhost:9000

# Check MinIO health
curl http://localhost:9000/minio/health/live
```

### Iceberg REST Catalog Issues

```bash
# Check REST catalog
curl http://localhost:8181/v1/config

# Verify namespace
curl http://localhost:8181/v1/namespaces
```

## Advanced Topics

### Continuous Data Generation

```bash
# Generate files every 10 seconds (in background)
while true; do
  docker exec lab3-python python /workspace/producer.py
  sleep 10
done
```

### Custom File Patterns

Modify `producer.py` to generate different:
- File sizes
- JSON structures
- CDC operations
- Table distributions

### Iceberg Maintenance

```sql
-- Expire old snapshots
CALL iceberg_catalog.system.expire_snapshots('lab3.cdc_events', TIMESTAMP '2026-01-01 00:00:00');

-- Compact data files
CALL iceberg_catalog.system.rewrite_data_files('lab3.cdc_events');
```

## Cleanup

```bash
# Stop pipeline and drop tables
./scripts/pipeline.sh stop

# Stop all services
docker compose down

# Remove all data (including MinIO volumes)
docker compose down -v
```

## Next Steps

1. **Add more file formats**: Modify pipeline to handle JSON, CSV
2. **Implement partitioning**: Partition Iceberg table by date/table_name
3. **Add Spark queries**: Create Spark job to query VARIANT columns
4. **Monitor metrics**: Add Prometheus/Grafana for observability
5. **Implement late arrival**: Handle out-of-order files

## Resources

- [Apache Flink FileSystem Connector](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/connectors/table/filesystem/)
- [Apache Iceberg VARIANT Type](https://iceberg.apache.org/docs/latest/variant/)
- [Flink SQL Reference](https://nightlies.apache.org/flink/flink-docs-release-2.1/docs/dev/table/sql/overview/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)

## License

Apache 2.0

---

**Lab 3 Complete!** 🎉

You now know how to:
- ✅ Process S3 files continuously with Flink
- ✅ Use the FileSystem connector
- ✅ Write to Iceberg with VARIANT support
- ✅ Handle semi-structured JSON data
- ✅ Build production-ready streaming pipelines
