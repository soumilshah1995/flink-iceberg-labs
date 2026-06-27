#!/usr/bin/env python3
"""
Lab 3: S3 File Producer
Generates JSON files with CDC events and uploads to MinIO S3
"""
import json
import os
import time
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import boto3
from botocore.client import Config

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'password')
S3_BUCKET = os.getenv('S3_BUCKET', 'raw-input')

# Auto-detect if running in container or on host
if Path('/workspace').exists() and os.access('/workspace', os.W_OK):
    LOCAL_OUTPUT_DIR = Path('/workspace/data/input')  # Container path
else:
    LOCAL_OUTPUT_DIR = Path(__file__).parent / 'data' / 'input'  # Host path

NUM_FILES = int(os.getenv('NUM_FILES', '3'))
RECORDS_PER_FILE = int(os.getenv('RECORDS_PER_FILE', '50'))

# Sample data
OPERATIONS = ['INSERT', 'UPDATE', 'DELETE']
TABLES = ['orders', 'customers', 'products']
DATABASES = ['ecommerce', 'analytics']
SITES = ['siteA', 'siteB', 'siteC']
PRODUCTS = ['widget', 'gadget', 'gizmo', 'doohickey']
PRIORITIES = ['LOW', 'MEDIUM', 'HIGH', 'URGENT']


def create_s3_client():
    """Create boto3 S3 client for MinIO"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def generate_event_variant():
    """Generate a complex nested JSON object for VARIANT column"""
    return {
        "schema_version": random.randint(1, 3),
        "kind": random.choice(["order", "shipment", "refund"]),
        "user": {
            "id": str(uuid.uuid4()),
            "labels": [f"label_{i}" for i in range(random.randint(1, 4))],
            "prefs": {
                "theme": random.choice(["light", "dark"]),
                "notifications": random.choice([True, False])
            }
        },
        "line_items": [
            {
                "sku": f"SKU-{random.randint(100, 999)}",
                "qty": random.randint(1, 10),
                "unit_price": f"{random.uniform(10, 500):.2f}"
            }
            for _ in range(random.randint(1, 3))
        ],
        "scores": {
            f"k{i}": random.randint(0, 100)
            for i in range(random.randint(2, 5))
        },
        "metadata": {
            "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "user_agent": "Mozilla/5.0",
            "session_id": str(uuid.uuid4())
        },
        "at": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
        "note": f"Sample note {random.randint(1000, 9999)}"
    }


def generate_cdc_event():
    """Generate a single CDC event record"""
    operation = random.choice(OPERATIONS)
    table_name = random.choice(TABLES)
    
    # Create nested payload
    payload = {
        "id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "site_id": random.choice(SITES),
        "product_name": random.choice(PRODUCTS),
        "order_value": str(random.randint(10, 1000)),
        "priority": random.choice(PRIORITIES),
        "order_date": (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
        "customer": {
            "name": f"Customer_{random.randint(1000, 9999)}",
            "email": f"user{random.randint(1, 1000)}@example.com",
            "tier": random.choice(["bronze", "silver", "gold", "platinum"])
        },
        "shipping": {
            "address": {
                "street": f"{random.randint(1, 9999)} Main St",
                "city": random.choice(["New York", "Los Angeles", "Chicago", "Houston"]),
                "state": random.choice(["NY", "CA", "IL", "TX"]),
                "zip": f"{random.randint(10000, 99999)}"
            },
            "method": random.choice(["standard", "express", "overnight"])
        }
    }
    
    return {
        "event_id": str(uuid.uuid4()),
        "operation": operation,
        "table_name": table_name,
        "database": random.choice(DATABASES),
        "payload": json.dumps(payload),  # JSON string for Parquet
        "event_variant": json.dumps(generate_event_variant()),  # Complex nested JSON
        "ts": int(time.time() * 1000),  # milliseconds
        "source_timestamp": datetime.now().isoformat()
    }


def generate_json_file(file_num: int) -> Path:
    """Generate a JSON file with CDC events (one JSON object per line)"""
    timestamp = int(time.time())
    filename = f"cdc_events_{file_num:03d}_{timestamp}.json"
    filepath = LOCAL_OUTPUT_DIR / filename
    
    # Generate events
    events = [generate_cdc_event() for _ in range(RECORDS_PER_FILE)]
    
    # Write to JSON (newline-delimited JSON)
    with open(filepath, 'w') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    # Print stats
    df = pd.DataFrame(events)
    op_counts = df['operation'].value_counts().to_dict()
    table_counts = df['table_name'].value_counts().to_dict()
    
    print(f"📝 Generated file {file_num + 1}/{NUM_FILES}: {filename}")
    print(f"   Operations: {op_counts}")
    print(f"   Tables: {table_counts}")
    print(f"   ✅ Written: {filepath}")
    
    return filepath


def upload_to_s3(filepath: Path, s3_client):
    """Upload JSON file to MinIO S3"""
    key = filepath.name
    try:
        s3_client.upload_file(
            str(filepath),
            S3_BUCKET,
            key
        )
        print(f"   📤 Uploaded to s3://{S3_BUCKET}/{key}")
        return True
    except Exception as e:
        print(f"   ❌ Upload failed: {e}")
        return False


def main():
    """Main execution"""
    print("=" * 70)
    print("🚀 Lab3: S3 File Producer")
    print("=" * 70)
    print(f"MinIO Endpoint: {MINIO_ENDPOINT}")
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Files to generate: {NUM_FILES}")
    print(f"Records per file: {RECORDS_PER_FILE}")
    print()
    
    # Create output directory
    LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create S3 client
    try:
        s3_client = create_s3_client()
        # Test connection
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"✅ Connected to MinIO bucket: {S3_BUCKET}\n")
    except Exception as e:
        print(f"❌ Failed to connect to MinIO: {e}")
        print("Make sure MinIO is running and bucket exists!")
        return
    
    # Generate and upload files
    successful_uploads = 0
    for i in range(NUM_FILES):
        filepath = generate_json_file(i)
        if upload_to_s3(filepath, s3_client):
            successful_uploads += 1
        print()
        
        # Delay between files (optional)
        if i < NUM_FILES - 1:
            time.sleep(2)
    
    print("=" * 70)
    print(f"✅ Completed: {successful_uploads}/{NUM_FILES} files uploaded")
    print("=" * 70)
    print()
    print("Next steps:")
    print("  1. Check Flink UI: http://localhost:8081")
    print("  2. Check MinIO UI: http://localhost:9001 (admin/password)")
    print(f"  3. Verify files: aws s3 ls s3://{S3_BUCKET}/ --endpoint-url {MINIO_ENDPOINT}")
    print()


if __name__ == '__main__':
    main()
