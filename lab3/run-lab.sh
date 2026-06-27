#!/bin/bash
# ==============================================================================
# Lab 3: Quick Start Script
# Complete workflow: Build → Start → Data → Verify
# ==============================================================================
set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
NC='\033[0m'

print_banner() {
    echo -e "${MAGENTA}"
    echo "=============================================="
    echo "🚀 Lab 3: S3 File Processing with Flink"
    echo "=============================================="
    echo -e "${NC}"
}

print_section() {
    echo -e "${BLUE}$1${NC}"
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

# Main workflow
print_banner

print_section "📋 Step 1: Starting Docker Stack..."
docker compose up -d --build
echo ""

print_info "Waiting 30 seconds for services to be healthy..."
sleep 30
print_success "Services are ready!"
echo ""

print_section "📋 Step 2: Register Catalog & Database..."
./scripts/pipeline.sh register
echo ""

print_section "🚀 Step 3: Start Streaming Pipeline..."
./scripts/pipeline.sh start
echo ""

print_info "Waiting 10 seconds for pipeline to initialize..."
sleep 10
echo ""

print_section "📝 Step 4: Generate & Upload Data Files..."
docker exec lab3-python python /workspace/producer.py
echo ""

print_info "Waiting 15 seconds for data processing..."
sleep 15
echo ""

print_section "🔍 Step 5: Verify Results..."
./scripts/verify.sh
echo ""

print_banner
print_success "Lab 3 Complete!"
echo ""
echo -e "${GREEN}Services:${NC}"
echo "  • Flink UI:     http://localhost:8081"
echo "  • MinIO UI:     http://localhost:9001 (admin/password)"
echo "  • Iceberg REST: http://localhost:8181"
echo ""
echo -e "${GREEN}Next Steps:${NC}"
echo "  • Add more data:    docker exec lab3-python python /workspace/producer.py"
echo "  • Query with Spark: python3 spark_query.py"
echo "  • Verify data:      ./scripts/verify.sh"
echo "  • Pause pipeline:   ./scripts/pipeline.sh pause"
echo "  • Stop pipeline:    ./scripts/pipeline.sh stop"
echo ""
print_info "Check README.md for detailed documentation"
