#!/bin/bash
# ==============================================================================
# Lab 3 Verification Script
# ==============================================================================
set -e

JOBMANAGER_CONTAINER="lab3-jobmanager"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo -e "${BLUE}=============================================="
    echo -e "$1"
    echo -e "==============================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}📋 $1${NC}"
}

# Query Iceberg table
print_header "🔍 Verify Data in Iceberg"

docker exec -i $JOBMANAGER_CONTAINER /opt/flink/bin/sql-client.sh <<EOF
SET 'execution.runtime-mode' = 'batch';

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

USE CATALOG iceberg_catalog;

-- Show tables
SHOW TABLES IN lab3;

-- Count records
SELECT 'Total Events' as metric, COUNT(*) as count FROM lab3.cdc_events;

-- Operations breakdown
SELECT operation, COUNT(*) as count 
FROM lab3.cdc_events 
GROUP BY operation 
ORDER BY count DESC;

-- Tables breakdown
SELECT table_name, COUNT(*) as count 
FROM lab3.cdc_events 
GROUP BY table_name 
ORDER BY count DESC;

-- Sample records
SELECT 
    event_id,
    operation,
    table_name,
    order_id,
    site_id,
    product_name,
    order_value,
    customer_name
FROM lab3.cdc_events
LIMIT 5;

QUIT;
EOF

print_success "Verification complete!"
print_info "Note: VARIANT columns (payload, event_variant) cannot be queried in Flink 2.1"
print_info "Use Spark 4.1+ with variant_get() to query VARIANT columns"

echo ""
print_info "Next steps:"
echo "  • Add more data: docker exec lab3-python python /workspace/producer.py"
echo "  • Query with Spark: python3 spark_query.py (TODO: create this script)"
echo "  • Check Flink UI: http://localhost:8081"
echo "  • Check MinIO: http://localhost:9001 (admin/password)"
