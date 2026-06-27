#!/bin/bash
# ==============================================================================
# Lab 3 Pipeline Management Script
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SQL_FILE="$PROJECT_ROOT/sql/pipeline.sql"

JOBMANAGER_CONTAINER="lab3-jobmanager"
FLINK_SQL="/opt/flink/bin/sql-client.sh"

# Colors
RED='\033[0;31m'
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

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

check_services() {
    if ! docker ps | grep -q "$JOBMANAGER_CONTAINER"; then
        print_error "Flink JobManager not running!"
        echo "Start services with: docker compose up -d"
        exit 1
    fi
}

get_job_id() {
    docker exec $JOBMANAGER_CONTAINER curl -s http://localhost:8081/jobs | \
        python3 -c "import sys, json; jobs = json.load(sys.stdin).get('jobs', []); print(jobs[0]['id'] if jobs else '')" 2>/dev/null || echo ""
}

cancel_job() {
    local job_id=$(get_job_id)
    if [ -n "$job_id" ]; then
        print_info "Cancelling job: $job_id"
        docker exec $JOBMANAGER_CONTAINER curl -X PATCH http://localhost:8081/jobs/$job_id || true
        sleep 5
    else
        print_info "No running job found"
    fi
}

register() {
    print_header "📋 Lab 3: Register (Create Catalog & Database)"
    check_services
    
    # Just create catalog and database
    docker exec -i $JOBMANAGER_CONTAINER $FLINK_SQL <<EOF
SET 'execution.runtime-mode' = 'streaming';

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
CREATE DATABASE IF NOT EXISTS lab3;

QUIT;
EOF
    
    print_success "Catalog and database created!"
}

start() {
    print_header "🚀 Lab 3: Start Pipeline"
    check_services
    
    local keep_table=false
    if [ "$1" = "--keep" ]; then
        keep_table=true
        print_info "Keeping existing Iceberg table"
    fi
    
    # Cancel existing job
    cancel_job
    
    # Drop table if not keeping
    if [ "$keep_table" = false ]; then
        print_info "Dropping existing table..."
        docker exec -i $JOBMANAGER_CONTAINER $FLINK_SQL <<EOF
SET 'execution.runtime-mode' = 'streaming';
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
DROP TABLE IF EXISTS lab3.cdc_events;
QUIT;
EOF
    fi
    
    # Submit pipeline
    print_info "Submitting SQL pipeline..."
    docker exec -i $JOBMANAGER_CONTAINER $FLINK_SQL -f /workspace/sql/pipeline.sql
    
    sleep 5
    print_success "Pipeline started!"
    print_info "Check status: http://localhost:8081"
}

pause() {
    print_header "⏸️  Lab 3: Pause Pipeline"
    check_services
    cancel_job
    print_success "Pipeline paused (table and offsets preserved)"
}

stop() {
    print_header "🛑 Lab 3: Stop Pipeline"
    check_services
    
    # Cancel job
    cancel_job
    
    # Drop table
    print_info "Dropping Iceberg table..."
    docker exec -i $JOBMANAGER_CONTAINER $FLINK_SQL <<EOF
SET 'execution.runtime-mode' = 'streaming';
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
DROP TABLE IF EXISTS lab3.cdc_events;
QUIT;
EOF
    
    print_success "Pipeline stopped and cleaned up"
}

run() {
    print_header "🎬 Lab 3: Full Run (Register + Start + Data)"
    
    register
    echo ""
    start
    echo ""
    
    print_info "Waiting 5 seconds before generating data..."
    sleep 5
    
    print_info "Generating and uploading data files..."
    docker exec lab3-python python /workspace/producer.py
    
    print_success "Complete! Data is flowing through the pipeline"
    print_info "Monitor: http://localhost:8081"
}

status() {
    print_header "📊 Lab 3: Status"
    check_services
    
    local job_id=$(get_job_id)
    if [ -n "$job_id" ]; then
        echo -e "${GREEN}Running job: $job_id${NC}"
        docker exec $JOBMANAGER_CONTAINER curl -s http://localhost:8081/jobs/$job_id | python3 -m json.tool
    else
        echo -e "${YELLOW}No running jobs${NC}"
    fi
}

usage() {
    cat <<EOF
Lab 3 Pipeline Management

Usage: $0 <command> [options]

Commands:
  register          Create Iceberg catalog and database
  start [--keep]    Start pipeline (--keep preserves existing table)
  pause             Cancel job, keep table and state
  stop              Cancel job and drop table
  run               Full workflow: register + start + generate data
  status            Show current job status

Examples:
  $0 register
  $0 start
  $0 start --keep
  $0 run
  $0 pause
  $0 stop

Services:
  Flink UI:     http://localhost:8081
  MinIO UI:     http://localhost:9001 (admin/password)
  Iceberg REST: http://localhost:8181

EOF
}

# Main command router
case "${1:-}" in
    register)
        register
        ;;
    start)
        start "$2"
        ;;
    pause)
        pause
        ;;
    stop)
        stop
        ;;
    run)
        run
        ;;
    status)
        status
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        usage
        exit 1
        ;;
esac
