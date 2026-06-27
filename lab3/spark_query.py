#!/usr/bin/env python3
"""
Lab 3: Spark VARIANT Query Script
Query Iceberg VARIANT columns using PySpark 4.0+ (same as lab2!)

⚠️  REQUIRES Apache Spark 4.0+ for VARIANT type support!

Install Spark 4.0:
```bash
# Download and extract Spark 4.0
curl -O https://archive.apache.org/dist/spark/spark-4.0.0/spark-4.0.0-bin-hadoop3.tgz
tar -xzf spark-4.0.0-bin-hadoop3.tgz
export SPARK_HOME=/path/to/spark-4.0.0-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
```

Run locally:
```bash
cd /Users/sshah/IdeaProjects/study-learn/flink/lab3
export PACKAGES="org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
python3 spark_query.py
```
"""
import os
import sys

# --- Spark / Iceberg setup (before pyspark import) ---
os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DEFAULT_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"  # ✅ Spark 4.0 for VARIANT!
    "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
)
PACKAGES = os.environ.get("PACKAGES", DEFAULT_PACKAGES)
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {PACKAGES} pyspark-shell"

from pyspark.sql import SparkSession

# Configuration (same as lab2)
CATALOG = "iceberg_rest"
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'admin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'password')
ICEBERG_REST_URI = os.getenv('ICEBERG_REST_URI', 'http://localhost:8181')
ICEBERG_WAREHOUSE = os.getenv('ICEBERG_WAREHOUSE', 's3://warehouse/')

def create_spark_session():
    """Create Spark session with Iceberg and S3 support (same config as lab2)"""
    spark = (SparkSession.builder
        .appName("Lab3-VARIANT-Query")
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")  # ✅ Use 'type' not 'catalog-impl'!
        .config(f"spark.sql.catalog.{CATALOG}.uri", ICEBERG_REST_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.client.region", "us-east-1")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", MINIO_ACCESS_KEY)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.defaultCatalog", CATALOG)
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    """Main query execution"""
    spark = create_spark_session()
    
    print("\n" + "=" * 80)
    print("🔍 Lab 3: Query Iceberg VARIANT with Spark")
    print("=" * 80)
    print(f"Spark version: {spark.version}")
    print(f"Iceberg REST: {ICEBERG_REST_URI}")
    print(f"MinIO: {MINIO_ENDPOINT}")
    print(f"Warehouse: {ICEBERG_WAREHOUSE}")
    
    # Check Spark version for VARIANT support
    spark_major = int(spark.version.split('.')[0])
    if spark_major < 4:
        print("\n" + "⚠️ " * 40)
        print("❌ ERROR: VARIANT type requires Apache Spark 4.0+")
        print(f"   Current version: {spark.version}")
        print("   This script uses variant_get() which is only available in Spark 4.0+")
        print("\n📥 To install Spark 4.0:")
        print("   1. Download: https://archive.apache.org/dist/spark/spark-4.0.0/")
        print("   2. Extract and set SPARK_HOME")
        print("   3. Reinstall pyspark: pip install pyspark==4.0.0")
        print("⚠️ " * 40)
        spark.stop()
        sys.exit(1)
    
    # Show available tables
    print(f"\n📚 Available tables in lab3 namespace:")
    spark.sql(f"SHOW TABLES IN {CATALOG}.lab3").show(truncate=False)
    
    # Show schema
    print(f"\n📋 Table Schema:")
    spark.table(f"{CATALOG}.lab3.cdc_events").printSchema()
    
    try:
        # Query 1: Basic count
        print("\n" + "=" * 80)
        print("📊 Query 1: Record Count")
        print("=" * 80)
        
        df_count = spark.sql(f"""
            SELECT 
                'Total Events' as metric,
                COUNT(*) as count 
            FROM {CATALOG}.lab3.cdc_events
        """)
        df_count.show()
        
        # Query 2: Operations breakdown
        print("\n" + "=" * 80)
        print("📊 Query 2: Operations Breakdown")
        print("=" * 80)
        
        df_ops = spark.sql(f"""
            SELECT 
                operation,
                COUNT(*) as count
            FROM {CATALOG}.lab3.cdc_events
            GROUP BY operation
            ORDER BY count DESC
        """)
        df_ops.show()
        
        # Query 3: VARIANT queries
        print("\n" + "=" * 80)
        print("📊 Query 3: VARIANT Column Queries")
        print("=" * 80)
        print("Extracting nested paths from VARIANT columns...")
        
        df_variant = spark.sql(f"""
            SELECT 
                event_id,
                operation,
                
                -- Extract from payload VARIANT
                variant_get(payload, '$.order_id', 'string') as order_id,
                variant_get(payload, '$.site_id', 'string') as site_id,
                variant_get(payload, '$.product_name', 'string') as product,
                variant_get(payload, '$.order_value', 'string') as value,
                variant_get(payload, '$.customer.name', 'string') as customer,
                variant_get(payload, '$.customer.tier', 'string') as tier,
                variant_get(payload, '$.shipping.address.city', 'string') as city,
                
                -- Extract from event_variant VARIANT
                variant_get(event_variant, '$.kind', 'string') as event_kind,
                variant_get(event_variant, '$.user.id', 'string') as user_id,
                variant_get(event_variant, '$.line_items[0].sku', 'string') as first_sku,
                variant_get(event_variant, '$.scores.k0', 'int') as score_k0,
                try_variant_get(event_variant, '$.note', 'string') as note
                
            FROM {CATALOG}.lab3.cdc_events
            ORDER BY event_id
            LIMIT 10
        """)
        df_variant.show(truncate=50)
        
        # Query 4: Complex aggregations
        print("\n" + "=" * 80)
        print("📊 Query 4: Aggregations by Site and Product")
        print("=" * 80)
        
        df_agg = spark.sql(f"""
            SELECT 
                variant_get(payload, '$.site_id', 'string') as site,
                variant_get(payload, '$.product_name', 'string') as product,
                COUNT(*) as order_count,
                AVG(CAST(variant_get(payload, '$.order_value', 'string') AS INT)) as avg_value
            FROM {CATALOG}.lab3.cdc_events
            WHERE operation = 'INSERT'
            GROUP BY site, product
            ORDER BY order_count DESC
            LIMIT 10
        """)
        df_agg.show()
        
        # Query 5: Customer tier analysis
        print("\n" + "=" * 80)
        print("📊 Query 5: Customer Tier Distribution")
        print("=" * 80)
        
        df_tier = spark.sql(f"""
            SELECT 
                variant_get(payload, '$.customer.tier', 'string') as tier,
                COUNT(*) as count,
                ROUND(AVG(CAST(variant_get(payload, '$.order_value', 'string') AS INT)), 2) as avg_order_value
            FROM {CATALOG}.lab3.cdc_events
            GROUP BY tier
            ORDER BY count DESC
        """)
        df_tier.show()
        
        # Query 6: Event variant analysis
        print("\n" + "=" * 80)
        print("📊 Query 6: Event Variant Analysis")
        print("=" * 80)
        
        df_event = spark.sql(f"""
            SELECT 
                variant_get(event_variant, '$.kind', 'string') as event_kind,
                variant_get(event_variant, '$.schema_version', 'int') as schema_version,
                COUNT(*) as count
            FROM {CATALOG}.lab3.cdc_events
            GROUP BY event_kind, schema_version
            ORDER BY count DESC
        """)
        df_event.show()
        
        print("\n" + "=" * 80)
        print("✅ Lab 3: VARIANT Queries Completed Successfully!")
        print("=" * 80)
        print("\n🎯 Key Achievements:")
        print("   1. ✅ Flink writes JSON files → Iceberg with PARSE_JSON()")
        print("   2. ✅ VARIANT columns store semi-structured JSON efficiently")
        print("   3. ✅ Spark queries VARIANT data with variant_get()")
        print("   4. ✅ Nested path extraction: payload.customer.tier")
        print("   5. ✅ Array access: event_variant.line_items[0].sku")
        print("   6. ✅ try_variant_get() handles missing paths gracefully")
        print("\n📝 Notes:")
        print("   • Flink 2.1 writes VARIANT (using PARSE_JSON)")
        print("   • Spark 4.0+ reads VARIANT (using variant_get)")
        print("   • Iceberg format V3 required for VARIANT support")
        print("   • apache/iceberg-rest-fixture supports VARIANT")
        print()
        
    except Exception as e:
        print(f"\n❌ Error executing queries: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
