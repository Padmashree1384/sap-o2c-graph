"""
Ingest SAP O2C JSONL files into MongoDB.
Run once: python ingest.py --data-dir /path/to/sap-o2c-data
"""
import os
import json
import asyncio
import argparse
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "sap_o2c")

COLLECTION_MAP = {
    "sales_order_headers": "sales_order_headers",
    "sales_order_items": "sales_order_items",
    "sales_order_schedule_lines": "sales_order_schedule_lines",
    "billing_document_headers": "billing_document_headers",
    "billing_document_items": "billing_document_items",
    "billing_document_cancellations": "billing_document_cancellations",
    "outbound_delivery_headers": "outbound_delivery_headers",
    "outbound_delivery_items": "outbound_delivery_items",
    "payments_accounts_receivable": "payments_accounts_receivable",
    "journal_entry_items_accounts_receivable": "journal_entry_items_accounts_receivable",
    "business_partners": "business_partners",
    "business_partner_addresses": "business_partner_addresses",
    "customer_company_assignments": "customer_company_assignments",
    "customer_sales_area_assignments": "customer_sales_area_assignments",
    "products": "products",
    "product_descriptions": "product_descriptions",
    "product_plants": "product_plants",
    "product_storage_locations": "product_storage_locations",
    "plants": "plants",
}

# Indexes to create for fast graph traversal
INDEXES = {
    "sales_order_headers": ["salesOrder", "soldToParty"],
    "sales_order_items": ["salesOrder", "material"],
    "billing_document_headers": ["billingDocument", "soldToParty", "accountingDocument"],
    "billing_document_items": ["billingDocument", "material", "referenceSdDocument"],
    "outbound_delivery_headers": ["deliveryDocument"],
    "outbound_delivery_items": ["deliveryDocument", "referenceSdDocument", "plant"],
    "payments_accounts_receivable": ["accountingDocument", "customer"],
    "journal_entry_items_accounts_receivable": ["accountingDocument", "referenceDocument", "customer"],
    "business_partners": ["businessPartner", "customer"],
    "products": ["product"],
    "plants": ["plant"],
}


def read_jsonl_dir(dir_path):
    records = []
    for fname in os.listdir(dir_path):
        if fname.endswith(".jsonl"):
            with open(os.path.join(dir_path, fname)) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
    return records


async def ingest(data_dir: str):
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]

    for folder, collection_name in COLLECTION_MAP.items():
        dir_path = os.path.join(data_dir, folder)
        if not os.path.exists(dir_path):
            print(f"  SKIP {folder} (not found)")
            continue

        records = read_jsonl_dir(dir_path)
        if not records:
            print(f"  SKIP {folder} (empty)")
            continue

        col = db[collection_name]
        await col.drop()
        # Insert in batches
        batch_size = 500
        for i in range(0, len(records), batch_size):
            await col.insert_many(records[i:i+batch_size])

        # Create indexes
        for field in INDEXES.get(collection_name, []):
            await col.create_index(field)

        print(f"  OK {collection_name}: {len(records)} records")

    print("\nIngestion complete.")
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="./sap-o2c-data")
    args = parser.parse_args()
    asyncio.run(ingest(args.data_dir))