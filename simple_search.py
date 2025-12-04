from elasticsearch import Elasticsearch
import pandas as pd
import math

print("🚀 Starting ElasticSearch 9.2.1 Demo with Authentication...")

# Connect to Elasticsearch
try:
    es = Elasticsearch(
        "http://localhost:9200",
        basic_auth=('elastic', 'QRvSjVCcpXs9OfofSkRh'),
        verify_certs=False
    )
    
    if es.ping():
        print("✅ Authentication successful! Connected to ElasticSearch 9.2.1")
        
        info = es.info()
        print(f"📊 Cluster: {info['cluster_name']}")
        print(f"🔄 Version: {info['version']['number']}")
        print(f"🏷️ Node Name: {info['name']}")
    else:
        print("❌ Authentication failed")
        exit(1)

except Exception as e:
    print(f"❌ Connection error: {e}")
    exit(1)


# ===============================
# CLEANING FUNCTIONS (IMPORTANT!)
# ===============================

def clean_value(value):
    """Clean a single value to make it JSON-safe."""
    if value is None:
        return None

    # Handle real float NaN or Infinity
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None

    # Handle string "NaN", "nan", "NULL", etc.
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ["nan", "null", "none", "n/a", "na", ""]:
            return None

    return value


def clean_document(doc):
    """Clean an entire CSV row dictionary."""
    cleaned = {}
    for k, v in doc.items():
        cleaned[k] = clean_value(v)
    return cleaned


# ===============================
# LOAD AND CLEAN CSV
# ===============================

index_name = "csv_documents"

df = pd.read_csv("posts.csv", dtype=str)  # IMPORTANT: load all columns as string
df = df.where(pd.notnull(df), None)       # Replace actual NaN with None

print(f"📄 Loaded {len(df)} rows from CSV")

# ===============================
# RECREATE INDEX
# ===============================

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print("🗑️ Deleted existing index")

es.indices.create(index=index_name)
print(f"📁 Created index: {index_name}")

# ===============================
# INDEX DOCUMENTS SAFELY
# ===============================

print("\n📥 Indexing CSV rows...")

for idx, row in df.iterrows():
    raw_doc = row.to_dict()
    doc = clean_document(raw_doc)

    try:
        es.index(index=index_name, id=idx+1, document=doc)
        print(f"   ✅ Inserted row {idx+1}")
    except Exception as e:
        print("\n❌ ERROR ON ROW:", idx+1)
        print("Bad document content:")
        print(raw_doc)
        print("\nCleaned document:")
        print(doc)
        raise e   # Re-throw to show full error


# Refresh index
es.indices.refresh(index=index_name)

print("\n🎉 CSV successfully imported into Elasticsearch!")
print(f"📊 Total documents indexed: {len(df)}")
