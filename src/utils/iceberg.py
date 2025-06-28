import os
from pyiceberg.catalog import load_catalog

HIVE_CATALOG_URI = os.getenv("HIVE_CATALOG_URI", "thrift://hive-metastore.hive-metastore:9083")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://weather/warehouse")

def get_iceberg_catalog():
    """Initialize and return Iceberg catalog"""
    catalog_properties = {
        "uri": HIVE_CATALOG_URI,
        "warehouse": ICEBERG_WAREHOUSE,
        "type": "hive"
    }
    
    return load_catalog("iceberg", **catalog_properties) 