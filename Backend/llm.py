"""
Hybrid query engine for SAP O2C:
- Direct handlers for known query patterns (fast, 100% accurate)
- LLM fallback for everything else (Groq llama-3.3-70b)
"""
import os, json, re, httpx
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = "llama-3.3-70b-versatile"
GROQ_URL     = "https://api.groq.com/openai/v1/chat/completions"

# ── Full schema ───────────────────────────────────────────────────────────────
SCHEMA = """
MongoDB database: sap_o2c

COLLECTIONS & FIELDS:
1. sales_order_headers
   salesOrder(string,PK), soldToParty(→business_partners.businessPartner),
   creationDate(ISODate), totalNetAmount(string→float), transactionCurrency,
   overallDeliveryStatus("A"=not started,"B"=partial,"C"=complete),
   overallOrdReltdBillgStatus("A"=not billed,"B"=partial,"C"=billed),
   requestedDeliveryDate, salesOrderType, salesOrganization

2. sales_order_items
   salesOrder(→sales_order_headers), salesOrderItem, material(→products.product),
   requestedQuantity(string→float), requestedQuantityUnit, netAmount(string→float),
   productionPlant, storageLocation, materialGroup

3. outbound_delivery_headers
   deliveryDocument(string,PK), creationDate(ISODate),
   overallGoodsMovementStatus("A"=not done,"C"=done),
   overallPickingStatus("C"=complete), shippingPoint,
   actualGoodsMovementDate

4. outbound_delivery_items
   deliveryDocument(→outbound_delivery_headers),
   referenceSdDocument(=salesOrder, →sales_order_headers.salesOrder),  ← CRITICAL JOIN
   deliveryDocumentItem, plant, storageLocation,
   actualDeliveryQuantity(string→float), deliveryQuantityUnit

5. billing_document_headers
   billingDocument(string,PK), billingDocumentType("F2"=invoice,"S1"=cancel),
   creationDate(ISODate), billingDocumentDate(ISODate),
   totalNetAmount(string→float), transactionCurrency,
   accountingDocument(→journal_entry_items_accounts_receivable.accountingDocument),
   soldToParty(→business_partners), companyCode,
   billingDocumentIsCancelled(bool), cancelledBillingDocument

6. billing_document_items
   billingDocument(→billing_document_headers),
   billingDocumentItem, material(→products.product),
   billingQuantity(string→float), netAmount(string→float),
   referenceSdDocument(=deliveryDocument, →outbound_delivery_headers.deliveryDocument)  ← CRITICAL JOIN

7. journal_entry_items_accounts_receivable
   accountingDocument(string,PK),
   referenceDocument(=billingDocument, →billing_document_headers.billingDocument),
   customer(→business_partners), glAccount,
   amountInTransactionCurrency(string→float), transactionCurrency,
   postingDate(ISODate), clearingDate(ISODate),
   clearingAccountingDocument, accountingDocumentType, fiscalYear, profitCenter

8. payments_accounts_receivable
   accountingDocument(string), customer(→business_partners),
   amountInTransactionCurrency(string→float), transactionCurrency,
   postingDate(ISODate), clearingDate(ISODate),
   clearingAccountingDocument, companyCode

9. business_partners
   businessPartner(string,PK), customer(=businessPartner),
   businessPartnerName, businessPartnerFullName,
   businessPartnerIsBlocked(bool), businessPartnerCategory,
   creationDate(ISODate)

10. products
    product(string,PK), productType, productOldId, productGroup,
    baseUnit, grossWeight(string→float), weightUnit, division,
    industrySector, isMarkedForDeletion(bool)

11. product_descriptions
    product(→products), language, productDescription

12. plants
    plant(string,PK)

13. billing_document_cancellations
    billingDocument, cancelledBillingDocument

FULL O2C FLOW (most important):
Customer → SalesOrder → SalesOrderItems → DeliveryItems → DeliveryHeader
       → BillingItems → BillingHeader → JournalEntry → Payment

JOIN PATHS:
sales_order_headers.soldToParty = business_partners.businessPartner
sales_order_items.salesOrder = sales_order_headers.salesOrder
outbound_delivery_items.referenceSdDocument = sales_order_headers.salesOrder  ← delivery→order
outbound_delivery_items.deliveryDocument = outbound_delivery_headers.deliveryDocument
billing_document_items.referenceSdDocument = outbound_delivery_headers.deliveryDocument  ← billing→delivery
billing_document_items.billingDocument = billing_document_headers.billingDocument
billing_document_headers.accountingDocument = journal_entry_items_accounts_receivable.accountingDocument
journal_entry_items_accounts_receivable.accountingDocument = payments_accounts_receivable.accountingDocument
journal_entry_items_accounts_receivable.referenceDocument = billing_document_headers.billingDocument

IMPORTANT NOTES:
- All amount/quantity fields are STRINGS. Use $toDouble to convert before $sum/$avg.
- Dates are ISO strings like "2025-04-03T00:00:00.000Z". Use $substr to extract "YYYY-MM" for month grouping.
- billingDocumentIsCancelled is a boolean field.
- overallDeliveryStatus "C" means fully delivered.
- Use $toString when doing string comparisons on numeric IDs.
"""

SYSTEM_PROMPT = f"""You are a MongoDB aggregation expert for a SAP Order-to-Cash system.

{SCHEMA}

YOUR JOB:
1. Classify if the question is about this SAP O2C dataset.
2. If YES: write a precise MongoDB aggregation pipeline.
3. If NO: set is_relevant=false.

PIPELINE RULES:
- ALWAYS use $toDouble when summing/averaging amount or quantity strings: {{"$toDouble": "$totalNetAmount"}}
- For date filtering by month/year: use $substr on creationDate: {{"$substr": ["$creationDate", 0, 7]}} gives "YYYY-MM"
- For "top N" queries: $group → $sort → $limit
- For cross-collection queries: use $lookup with proper localField/foreignField
- Always $project at end to clean output, remove _id with {{"_id": 0}}
- Limit results: $limit 20 for lists, $limit 1 for single items
- For "count" queries: use $count or $group with $sum:1

GUARDRAILS — set is_relevant=false for:
- General knowledge (capitals, history, science, math)
- Coding help unrelated to this system
- Creative writing, jokes, poems
- Weather, sports, news
- Anything not about orders/deliveries/billing/payments/customers/products in this dataset

Return ONLY valid JSON, no markdown:
{{
  "is_relevant": true/false,
  "refusal_message": "string (only if is_relevant=false)",
  "collection": "primary_collection_name",
  "pipeline": [...],
  "answer_hint": "what this pipeline computes"
}}

EXAMPLE PIPELINES:

Q: How many billing documents were created in May 2025?
{{
  "collection": "billing_document_headers",
  "pipeline": [
    {{"$match": {{"creationDate": {{"$gte": "2025-05-01", "$lt": "2025-06-01"}}}}}},
    {{"$count": "total"}}
  ],
  "answer_hint": "count of billing documents in May 2025"
}}

Q: Which customer has the highest total order value?
{{
  "collection": "sales_order_headers",
  "pipeline": [
    {{"$group": {{
      "_id": "$soldToParty",
      "totalValue": {{"$sum": {{"$toDouble": "$totalNetAmount"}}}}
    }}}},
    {{"$sort": {{"totalValue": -1}}}},
    {{"$limit": 5}},
    {{"$lookup": {{
      "from": "business_partners",
      "localField": "_id",
      "foreignField": "businessPartner",
      "as": "bp"
    }}}},
    {{"$project": {{
      "_id": 0,
      "customerId": "$_id",
      "customerName": {{"$arrayElemAt": ["$bp.businessPartnerName", 0]}},
      "totalOrderValue": "$totalValue"
    }}}}
  ],
  "answer_hint": "top customers by total order value in INR"
}}

Q: Which products are associated with the highest number of billing documents?
{{
  "collection": "billing_document_items",
  "pipeline": [
    {{"$group": {{"_id": "$material", "billingCount": {{"$sum": 1}}}}}},
    {{"$sort": {{"billingCount": -1}}}},
    {{"$limit": 10}},
    {{"$lookup": {{"from": "products", "localField": "_id", "foreignField": "product", "as": "p"}}}},
    {{"$project": {{
      "_id": 0,
      "material": "$_id",
      "billingCount": 1,
      "productOldId": {{"$arrayElemAt": ["$p.productOldId", 0]}}
    }}}}
  ]
}}

Q: Trace the full flow of billing document 90504248
{{
  "collection": "billing_document_headers",
  "pipeline": [
    {{"$match": {{"billingDocument": "90504248"}}}},
    {{"$lookup": {{"from": "billing_document_items", "localField": "billingDocument", "foreignField": "billingDocument", "as": "items"}}}},
    {{"$lookup": {{"from": "outbound_delivery_headers",
      "let": {{"delIds": "$items.referenceSdDocument"}},
      "pipeline": [{{"$match": {{"$expr": {{"$in": ["$deliveryDocument", "$$delIds"]}}}}}}],
      "as": "deliveries"
    }}}},
    {{"$lookup": {{"from": "journal_entry_items_accounts_receivable", "localField": "accountingDocument", "foreignField": "accountingDocument", "as": "journalEntries"}}}},
    {{"$lookup": {{"from": "payments_accounts_receivable", "localField": "accountingDocument", "foreignField": "accountingDocument", "as": "payments"}}}},
    {{"$lookup": {{"from": "business_partners", "localField": "soldToParty", "foreignField": "businessPartner", "as": "customer"}}}}
  ],
  "answer_hint": "full O2C flow: billing→delivery→journal→payment→customer"
}}

Q: Show sales orders delivered but not billed
{{
  "collection": "sales_order_headers",
  "pipeline": [
    {{"$match": {{"overallDeliveryStatus": "C"}}}},
    {{"$lookup": {{
      "from": "outbound_delivery_items",
      "localField": "salesOrder",
      "foreignField": "referenceSdDocument",
      "as": "deliveries"
    }}}},
    {{"$match": {{"deliveries.0": {{"$exists": true}}}}}},
    {{"$lookup": {{
      "from": "billing_document_items",
      "let": {{"delDocs": "$deliveries.deliveryDocument"}},
      "pipeline": [{{"$match": {{"$expr": {{"$in": ["$referenceSdDocument", "$$delDocs"]}}}}}}],
      "as": "billings"
    }}}},
    {{"$match": {{"billings": {{"$size": 0}}}}}},
    {{"$project": {{"_id": 0, "salesOrder": 1, "soldToParty": 1, "totalNetAmount": 1, "creationDate": 1}}}},
    {{"$limit": 20}}
  ],
  "answer_hint": "sales orders fully delivered but with no billing documents"
}}

Q: What is the total revenue billed in April 2025?
{{
  "collection": "billing_document_headers",
  "pipeline": [
    {{"$match": {{"creationDate": {{"$gte": "2025-04-01", "$lt": "2025-05-01"}}, "billingDocumentIsCancelled": false}}}},
    {{"$group": {{
      "_id": null,
      "totalRevenue": {{"$sum": {{"$toDouble": "$totalNetAmount"}}}},
      "count": {{"$sum": 1}}
    }}}},
    {{"$project": {{"_id": 0, "totalRevenue": 1, "count": 1}}}}
  ],
  "answer_hint": "total billed revenue in April 2025"
}}

Q: Show me all blocked customers
{{
  "collection": "business_partners",
  "pipeline": [
    {{"$match": {{"businessPartnerIsBlocked": true}}}},
    {{"$project": {{"_id": 0, "businessPartner": 1, "businessPartnerName": 1}}}},
    {{"$limit": 20}}
  ]
}}

Q: Which deliveries have no billing document?
{{
  "collection": "outbound_delivery_headers",
  "pipeline": [
    {{"$lookup": {{
      "from": "billing_document_items",
      "localField": "deliveryDocument",
      "foreignField": "referenceSdDocument",
      "as": "billings"
    }}}},
    {{"$match": {{"billings": {{"$size": 0}}}}}},
    {{"$project": {{"_id": 0, "deliveryDocument": 1, "creationDate": 1, "overallGoodsMovementStatus": 1}}}},
    {{"$limit": 20}}
  ]
}}

Q: What are the top 5 products by quantity ordered?
{{
  "collection": "sales_order_items",
  "pipeline": [
    {{"$group": {{
      "_id": "$material",
      "totalQty": {{"$sum": {{"$toDouble": "$requestedQuantity"}}}}
    }}}},
    {{"$sort": {{"totalQty": -1}}}},
    {{"$limit": 5}},
    {{"$lookup": {{"from": "products", "localField": "_id", "foreignField": "product", "as": "p"}}}},
    {{"$project": {{
      "_id": 0,
      "material": "$_id",
      "totalQuantity": "$totalQty",
      "productOldId": {{"$arrayElemAt": ["$p.productOldId", 0]}}
    }}}}
  ]
}}
"""


async def call_groq(messages: list, max_tokens: int = 2000) -> str:
    async with httpx.AsyncClient(timeout=45) as client:
        resp = await client.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": GROQ_MODEL, "messages": messages, "temperature": 0.0, "max_tokens": max_tokens},
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()


def clean_doc(doc):
    if isinstance(doc, dict):
        return {k: clean_doc(v) for k, v in doc.items() if k != "_id"}
    if isinstance(doc, list):
        return [clean_doc(i) for i in doc[:5]]  # trim nested arrays
    return doc


async def run_pipeline(db, collection: str, pipeline: list) -> list:
    results = []
    async for doc in db[collection].aggregate(pipeline):
        results.append(clean_doc(doc))
    return results[:20]


# ── Direct handlers for bullet-proof accuracy ─────────────────────────────────

async def direct_query(db, message: str):
    """
    Pattern-match common queries and run direct MongoDB code.
    Returns (results, answer_hint, highlighted) or None if no match.
    """
    msg = message.lower().strip()

    # ── COUNT billing docs by month/year ──
    m = re.search(r'how many billing\s*(documents?)?\s*(were|are)?\s*(created|issued)?\s*in\s*(january|february|march|april|may|june|july|august|september|october|november|december)?\s*(\d{4})?', msg)
    month_map = {
        'january':'01','february':'02','march':'03','april':'04',
        'may':'05','june':'06','july':'07','august':'08',
        'september':'09','october':'10','november':'11','december':'12'
    }
    if m and (m.group(4) or m.group(5)):
        month_name = m.group(4)
        year = m.group(5) or '2025'
        month_num = month_map.get(month_name, None)
        if month_num:
            prefix = f"{year}-{month_num}"
            count = await db.billing_document_headers.count_documents({
                "creationDate": {"$gte": f"{year}-{month_num}-01", "$lt": f"{year}-{str(int(month_num)+1).zfill(2)}-01"}
            })
            return [{"month": f"{month_name} {year}", "billingDocumentCount": count}], \
                   f"count of billing documents in {month_name} {year}", []

    # ── TOTAL REVENUE by month ──
    if 'total revenue' in msg or 'total amount billed' in msg or 'total billed' in msg:
        for month_name, month_num in month_map.items():
            if month_name in msg:
                year_m = re.search(r'\d{4}', msg)
                year = year_m.group() if year_m else '2025'
                next_month = str(int(month_num)+1).zfill(2)
                pipeline = [
                    {"$match": {"creationDate": {"$gte": f"{year}-{month_num}-01", "$lt": f"{year}-{next_month}-01"}, "billingDocumentIsCancelled": False}},
                    {"$group": {"_id": None, "totalRevenue": {"$sum": {"$toDouble": "$totalNetAmount"}}, "count": {"$sum": 1}}},
                    {"$project": {"_id": 0, "totalRevenue": 1, "count": 1}}
                ]
                results = await run_pipeline(db, "billing_document_headers", pipeline)
                return results, f"total billed revenue in {month_name} {year}", []

    # ── CUSTOMER with highest order value ──
    if ('customer' in msg or 'buyer' in msg) and ('highest' in msg or 'most' in msg or 'top' in msg) and ('order' in msg or 'value' in msg or 'amount' in msg):
        pipeline = [
            {"$group": {"_id": "$soldToParty", "totalValue": {"$sum": {"$toDouble": "$totalNetAmount"}}}},
            {"$sort": {"totalValue": -1}},
            {"$limit": 5},
            {"$lookup": {"from": "business_partners", "localField": "_id", "foreignField": "businessPartner", "as": "bp"}},
            {"$project": {"_id": 0, "customerId": "$_id", "customerName": {"$arrayElemAt": ["$bp.businessPartnerName", 0]}, "totalOrderValue": "$totalValue", "currency": "INR"}}
        ]
        results = await run_pipeline(db, "sales_order_headers", pipeline)
        highlighted = [r['customerId'] for r in results if r.get('customerId')]
        return results, "top customers by total order value", highlighted

    # ── TOP products by billing count ──
    if 'product' in msg and ('highest' in msg or 'most' in msg or 'top' in msg) and 'billing' in msg:
        pipeline = [
            {"$group": {"_id": "$material", "billingCount": {"$sum": 1}}},
            {"$sort": {"billingCount": -1}},
            {"$limit": 10},
            {"$lookup": {"from": "products", "localField": "_id", "foreignField": "product", "as": "p"}},
            {"$project": {"_id": 0, "material": "$_id", "billingCount": 1, "productOldId": {"$arrayElemAt": ["$p.productOldId", 0]}, "productType": {"$arrayElemAt": ["$p.productType", 0]}}}
        ]
        results = await run_pipeline(db, "billing_document_items", pipeline)
        highlighted = [r['material'] for r in results if r.get('material')]
        return results, "products ranked by number of billing documents", highlighted

    # ── TOP products by quantity ──
    if 'product' in msg and ('highest' in msg or 'most' in msg or 'top' in msg) and 'quantit' in msg:
        pipeline = [
            {"$group": {"_id": "$material", "totalQty": {"$sum": {"$toDouble": "$requestedQuantity"}}}},
            {"$sort": {"totalQty": -1}},
            {"$limit": 10},
            {"$lookup": {"from": "products", "localField": "_id", "foreignField": "product", "as": "p"}},
            {"$project": {"_id": 0, "material": "$_id", "totalQuantity": "$totalQty", "productOldId": {"$arrayElemAt": ["$p.productOldId", 0]}}}
        ]
        results = await run_pipeline(db, "sales_order_items", pipeline)
        highlighted = [r['material'] for r in results if r.get('material')]
        return results, "top products by total quantity ordered", highlighted

    # ── TRACE full flow of a billing document ──
    m = re.search(r'(?:trace|flow|full flow|complete flow|show flow).*?(?:billing\s*doc(?:ument)?\s*)?(\d{7,9})', msg)
    if m:
        bd_id = m.group(1)
        pipeline = [
            {"$match": {"billingDocument": bd_id}},
            {"$lookup": {"from": "billing_document_items", "localField": "billingDocument", "foreignField": "billingDocument", "as": "items"}},
            {"$lookup": {"from": "outbound_delivery_headers",
                "let": {"delIds": "$items.referenceSdDocument"},
                "pipeline": [{"$match": {"$expr": {"$in": ["$deliveryDocument", "$$delIds"]}}}],
                "as": "deliveries"
            }},
            {"$lookup": {"from": "outbound_delivery_items",
                "let": {"delIds": "$items.referenceSdDocument"},
                "pipeline": [{"$match": {"$expr": {"$in": ["$deliveryDocument", "$$delIds"]}}}],
                "as": "deliveryItems"
            }},
            {"$lookup": {"from": "journal_entry_items_accounts_receivable", "localField": "accountingDocument", "foreignField": "accountingDocument", "as": "journalEntries"}},
            {"$lookup": {"from": "payments_accounts_receivable", "localField": "accountingDocument", "foreignField": "accountingDocument", "as": "payments"}},
            {"$lookup": {"from": "business_partners", "localField": "soldToParty", "foreignField": "businessPartner", "as": "customer"}},
            {"$project": {
                "_id": 0,
                "billingDocument": 1, "billingDocumentType": 1, "creationDate": 1,
                "totalNetAmount": 1, "transactionCurrency": 1,
                "accountingDocument": 1, "billingDocumentIsCancelled": 1,
                "customerName": {"$arrayElemAt": ["$customer.businessPartnerName", 0]},
                "customerId": "$soldToParty",
                "deliveries": {"$map": {"input": "$deliveries", "as": "d", "in": {
                    "deliveryDocument": "$$d.deliveryDocument",
                    "creationDate": "$$d.creationDate",
                    "goodsMovementStatus": "$$d.overallGoodsMovementStatus"
                }}},
                "journalEntries": {"$map": {"input": "$journalEntries", "as": "j", "in": {
                    "accountingDocument": "$$j.accountingDocument",
                    "amount": "$$j.amountInTransactionCurrency",
                    "postingDate": "$$j.postingDate",
                    "glAccount": "$$j.glAccount"
                }}},
                "payments": {"$map": {"input": "$payments", "as": "p", "in": {
                    "accountingDocument": "$$p.accountingDocument",
                    "amount": "$$p.amountInTransactionCurrency",
                    "clearingDate": "$$p.clearingDate"
                }}},
                "items": {"$map": {"input": "$items", "as": "i", "in": {
                    "material": "$$i.material",
                    "quantity": "$$i.billingQuantity",
                    "netAmount": "$$i.netAmount",
                    "deliveryRef": "$$i.referenceSdDocument"
                }}}
            }}
        ]
        results = await run_pipeline(db, "billing_document_headers", pipeline)
        highlighted = [bd_id]
        if results:
            for d in results[0].get('deliveries', []):
                highlighted.append(d.get('deliveryDocument', ''))
            for j in results[0].get('journalEntries', []):
                highlighted.append(j.get('accountingDocument', ''))
        return results, f"full O2C flow trace for billing document {bd_id}", [h for h in highlighted if h]

    # ── DELIVERIES linked to a sales order ──
    m = re.search(r'deliver(?:ies|y)\s*(?:linked|for|of|associated)?\s*(?:to|with)?\s*(?:sales\s*order\s*)?(\d{5,7})', msg)
    if m:
        so_id = m.group(1)
        pipeline = [
            {"$match": {"referenceSdDocument": so_id}},
            {"$project": {"_id": 0, "deliveryDocument": 1, "deliveryDocumentItem": 1,
                          "plant": 1, "actualDeliveryQuantity": 1, "storageLocation": 1}}
        ]
        results = await run_pipeline(db, "outbound_delivery_items", pipeline)
        highlighted = list(set(r.get('deliveryDocument', '') for r in results if r.get('deliveryDocument')))
        return results, f"deliveries linked to sales order {so_id}", highlighted

    # ── SALES ORDERS for a customer ──
    m = re.search(r'(?:sales orders?|orders?)\s*(?:for|of|by)\s*(?:customer\s*)?(\d{8,10})', msg)
    if m:
        cust_id = m.group(1)
        pipeline = [
            {"$match": {"soldToParty": cust_id}},
            {"$lookup": {"from": "business_partners", "localField": "soldToParty", "foreignField": "businessPartner", "as": "bp"}},
            {"$project": {"_id": 0, "salesOrder": 1, "creationDate": 1, "totalNetAmount": 1,
                          "transactionCurrency": 1, "overallDeliveryStatus": 1, "overallOrdReltdBillgStatus": 1,
                          "customerName": {"$arrayElemAt": ["$bp.businessPartnerName", 0]}}},
            {"$limit": 20}
        ]
        results = await run_pipeline(db, "sales_order_headers", pipeline)
        highlighted = [r.get('salesOrder', '') for r in results if r.get('salesOrder')]
        return results, f"all sales orders for customer {cust_id}", highlighted

    # ── COMPLETE O2C FLOW for a customer ──
    m = re.search(r'(?:complete|full|entire)\s*(?:o2c|order.to.cash|flow)\s*(?:for\s*customer\s*)?(\d{8,10})', msg)
    if not m:
        m = re.search(r'(?:o2c|order.to.cash|flow)\s*(?:for|of)\s*(?:customer\s*)?(\d{8,10})', msg)
    if m:
        cust_id = m.group(1)
        pipeline = [
            {"$match": {"soldToParty": cust_id}},
            {"$lookup": {"from": "business_partners", "localField": "soldToParty", "foreignField": "businessPartner", "as": "bp"}},
            {"$lookup": {"from": "sales_order_items", "localField": "salesOrder", "foreignField": "salesOrder", "as": "items"}},
            {"$lookup": {"from": "outbound_delivery_items", "localField": "salesOrder", "foreignField": "referenceSdDocument", "as": "deliveryItems"}},
            {"$lookup": {"from": "billing_document_headers", "localField": "soldToParty", "foreignField": "soldToParty", "as": "billings"}},
            {"$limit": 3},
            {"$project": {
                "_id": 0,
                "salesOrder": 1, "totalNetAmount": 1, "creationDate": 1,
                "overallDeliveryStatus": 1, "overallOrdReltdBillgStatus": 1,
                "customerName": {"$arrayElemAt": ["$bp.businessPartnerName", 0]},
                "itemCount": {"$size": "$items"},
                "deliveryCount": {"$size": "$deliveryItems"},
                "billingCount": {"$size": "$billings"}
            }}
        ]
        results = await run_pipeline(db, "sales_order_headers", pipeline)
        highlighted = [cust_id] + [r.get('salesOrder', '') for r in results]
        return results, f"complete O2C flow for customer {cust_id}", [h for h in highlighted if h]

    # ── DELIVERED but NOT BILLED ──
    if ('deliver' in msg and 'not bill' in msg) or ('deliver' in msg and 'unbill' in msg) or ('no bill' in msg and 'deliver' in msg):
        pipeline = [
            {"$match": {"overallDeliveryStatus": "C"}},
            {"$lookup": {"from": "outbound_delivery_items", "localField": "salesOrder", "foreignField": "referenceSdDocument", "as": "deliveries"}},
            {"$match": {"deliveries.0": {"$exists": True}}},
            {"$lookup": {
                "from": "billing_document_items",
                "let": {"delDocs": "$deliveries.deliveryDocument"},
                "pipeline": [{"$match": {"$expr": {"$in": ["$referenceSdDocument", "$$delDocs"]}}}],
                "as": "billings"
            }},
            {"$match": {"billings": {"$size": 0}}},
            {"$project": {"_id": 0, "salesOrder": 1, "soldToParty": 1, "totalNetAmount": 1, "creationDate": 1}},
            {"$limit": 20}
        ]
        results = await run_pipeline(db, "sales_order_headers", pipeline)
        highlighted = [r.get('salesOrder', '') for r in results if r.get('salesOrder')]
        return results, "sales orders fully delivered but not yet billed", highlighted

    # ── CANCELLED billing documents ──
    if 'cancel' in msg and ('bill' in msg or 'invoice' in msg):
        pipeline = [
            {"$match": {"billingDocumentIsCancelled": True}},
            {"$project": {"_id": 0, "billingDocument": 1, "cancelledBillingDocument": 1,
                          "creationDate": 1, "totalNetAmount": 1, "soldToParty": 1}},
            {"$limit": 20}
        ]
        results = await run_pipeline(db, "billing_document_headers", pipeline)
        highlighted = [r.get('billingDocument', '') for r in results if r.get('billingDocument')]
        return results, "cancelled billing documents", highlighted

    # ── AVERAGE order value ──
    if 'average' in msg and 'order' in msg:
        pipeline = [
            {"$group": {"_id": None, "avgOrderValue": {"$avg": {"$toDouble": "$totalNetAmount"}}, "totalOrders": {"$sum": 1}}},
            {"$project": {"_id": 0, "avgOrderValue": 1, "totalOrders": 1}}
        ]
        results = await run_pipeline(db, "sales_order_headers", pipeline)
        return results, "average sales order value", []

    # ── TOTAL payments received ──
    if 'payment' in msg and ('total' in msg or 'sum' in msg or 'how much' in msg):
        for month_name, month_num in month_map.items():
            if month_name in msg:
                year_m = re.search(r'\d{4}', msg)
                year = year_m.group() if year_m else '2025'
                next_month = str(int(month_num)+1).zfill(2)
                pipeline = [
                    {"$match": {"postingDate": {"$gte": f"{year}-{month_num}-01", "$lt": f"{year}-{next_month}-01"}}},
                    {"$group": {"_id": None, "totalPayments": {"$sum": {"$toDouble": "$amountInTransactionCurrency"}}, "count": {"$sum": 1}}},
                    {"$project": {"_id": 0, "totalPayments": 1, "count": 1}}
                ]
                results = await run_pipeline(db, "payments_accounts_receivable", pipeline)
                return results, f"total payments in {month_name} {year}", []

    # ── SALES ORDERS count ──
    if ('how many' in msg or 'count' in msg or 'total number' in msg) and 'sales order' in msg:
        count = await db.sales_order_headers.count_documents({})
        return [{"totalSalesOrders": count}], "total number of sales orders", []

    # ── BILLING docs for a specific document ID ──
    m = re.search(r'billing\s*doc(?:ument)?\s*(\d{7,9})', msg)
    if m:
        bd_id = m.group(1)
        pipeline = [
            {"$match": {"billingDocument": bd_id}},
            {"$lookup": {"from": "billing_document_items", "localField": "billingDocument", "foreignField": "billingDocument", "as": "items"}},
            {"$project": {"_id": 0, "billingDocument": 1, "billingDocumentType": 1, "creationDate": 1,
                          "totalNetAmount": 1, "soldToParty": 1, "accountingDocument": 1,
                          "billingDocumentIsCancelled": 1, "items": 1}}
        ]
        results = await run_pipeline(db, "billing_document_headers", pipeline)
        return results, f"details of billing document {bd_id}", [bd_id]

    # ── JOURNAL ENTRY for an accounting document ──
    m = re.search(r'(?:journal\s*entry|accounting\s*doc(?:ument)?)\s*(?:for|linked|of|about)?\s*(?:#|number|no\.?)?\s*(\d{9,12})', msg)
    if m:
        acc_id = m.group(1)
        pipeline = [
            {"$match": {"accountingDocument": acc_id}},
            {"$project": {"_id": 0}},
            {"$limit": 5}
        ]
        results = await run_pipeline(db, "journal_entry_items_accounts_receivable", pipeline)
        return results, f"journal entry for accounting document {acc_id}", [acc_id]

    # ── PAYMENTS for a specific document ──
    m = re.search(r'payment[s]?\s*(?:for|of|linked|cleared)?\s*(?:accounting\s*doc(?:ument)?\s*)?(\d{9,12})', msg)
    if m:
        acc_id = m.group(1)
        pipeline = [
            {"$match": {"accountingDocument": acc_id}},
            {"$project": {"_id": 0}},
            {"$limit": 5}
        ]
        results = await run_pipeline(db, "payments_accounts_receivable", pipeline)
        return results, f"payments for accounting document {acc_id}", [acc_id]

    # ── NO DELIVERY orders ──
    if ('no deliver' in msg or 'without deliver' in msg or 'not deliver' in msg) and 'order' in msg:
        pipeline = [
            {"$lookup": {"from": "outbound_delivery_items", "localField": "salesOrder", "foreignField": "referenceSdDocument", "as": "deliveries"}},
            {"$match": {"deliveries": {"$size": 0}}},
            {"$project": {"_id": 0, "salesOrder": 1, "soldToParty": 1, "totalNetAmount": 1, "creationDate": 1}},
            {"$limit": 20}
        ]
        results = await run_pipeline(db, "sales_order_headers", pipeline)
        highlighted = [r.get('salesOrder', '') for r in results if r.get('salesOrder')]
        return results, "sales orders with no delivery", highlighted

    # ── BILLING without delivery ──
    if ('bill' in msg or 'invoice' in msg) and ('no deliver' in msg or 'without deliver' in msg):
        pipeline = [
            {"$match": {"billingDocumentIsCancelled": False}},
            {"$lookup": {"from": "billing_document_items", "localField": "billingDocument", "foreignField": "billingDocument", "as": "items"}},
            {"$match": {"items.0": {"$exists": True}}},
            {"$lookup": {
                "from": "outbound_delivery_headers",
                "let": {"delIds": "$items.referenceSdDocument"},
                "pipeline": [{"$match": {"$expr": {"$in": ["$deliveryDocument", "$$delIds"]}}}],
                "as": "deliveries"
            }},
            {"$match": {"deliveries": {"$size": 0}}},
            {"$project": {"_id": 0, "billingDocument": 1, "totalNetAmount": 1, "creationDate": 1, "soldToParty": 1}},
            {"$limit": 20}
        ]
        results = await run_pipeline(db, "billing_document_headers", pipeline)
        highlighted = [r.get('billingDocument', '') for r in results if r.get('billingDocument')]
        return results, "billing documents with no linked delivery", highlighted

    # ── MOST CUSTOMERS / top customers by order count ──
    if 'customer' in msg and ('most order' in msg or 'highest order' in msg or 'most orders' in msg):
        pipeline = [
            {"$group": {"_id": "$soldToParty", "orderCount": {"$sum": 1}, "totalValue": {"$sum": {"$toDouble": "$totalNetAmount"}}}},
            {"$sort": {"orderCount": -1}},
            {"$limit": 10},
            {"$lookup": {"from": "business_partners", "localField": "_id", "foreignField": "businessPartner", "as": "bp"}},
            {"$project": {"_id": 0, "customerId": "$_id", "customerName": {"$arrayElemAt": ["$bp.businessPartnerName", 0]}, "orderCount": 1, "totalValue": 1}}
        ]
        results = await run_pipeline(db, "sales_order_headers", pipeline)
        highlighted = [r.get('customerId', '') for r in results if r.get('customerId')]
        return results, "customers ranked by number of orders", highlighted

    # ── JOURNAL entries with negative amounts ──
    if 'negative' in msg and ('amount' in msg or 'journal' in msg):
        pipeline = [
            {"$match": {"$expr": {"$lt": [{"$toDouble": "$amountInTransactionCurrency"}, 0]}}},
            {"$project": {"_id": 0, "accountingDocument": 1, "amountInTransactionCurrency": 1,
                          "referenceDocument": 1, "postingDate": 1, "customer": 1}},
            {"$limit": 20}
        ]
        results = await run_pipeline(db, "journal_entry_items_accounts_receivable", pipeline)
        return results, "journal entries with negative amounts", []

    # ── TOTAL net amount of all sales orders ──
    if ('total' in msg or 'sum' in msg) and 'net amount' in msg and ('sales order' in msg or 'all order' in msg):
        pipeline = [
            {"$group": {"_id": None, "totalNetAmount": {"$sum": {"$toDouble": "$totalNetAmount"}}, "count": {"$sum": 1}}},
            {"$project": {"_id": 0, "totalNetAmount": 1, "orderCount": "$count"}}
        ]
        results = await run_pipeline(db, "sales_order_headers", pipeline)
        return results, "total net amount across all sales orders", []

    return None  # No direct handler matched


# ── LLM fallback ──────────────────────────────────────────────────────────────

async def llm_query(db, message: str, history: list):
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    for turn in history[-6:]:
        msgs.append({"role": turn["role"], "content": turn["content"]})
    msgs.append({"role": "user", "content": message})

    raw = await call_groq(msgs, max_tokens=2000)

    # Extract JSON
    try:
        clean = re.sub(r"```json|```", "", raw).strip()
        match = re.search(r'\{[\s\S]*\}', clean)
        parsed = json.loads(match.group() if match else clean)
    except Exception:
        return None, None, None, False

    if not parsed.get("is_relevant", True):
        return None, None, parsed.get("refusal_message"), False

    collection   = parsed.get("collection", "sales_order_headers")
    pipeline     = parsed.get("pipeline", [])
    answer_hint  = parsed.get("answer_hint", "")

    results = []
    if pipeline:
        try:
            results = await run_pipeline(db, collection, pipeline)
        except Exception as e:
            print(f"Pipeline error: {e}")
            return [], answer_hint, None, True

    highlighted = []
    for r in results[:15]:
        for key in ["salesOrder", "billingDocument", "deliveryDocument",
                    "accountingDocument", "material", "customerId", "businessPartner"]:
            val = r.get(key)
            if val and str(val) not in highlighted:
                highlighted.append(str(val))

    return results, answer_hint, None, True


# ── Summarize results ─────────────────────────────────────────────────────────

async def summarize(message: str, results: list, answer_hint: str) -> str:
    result_str = json.dumps(results[:10], indent=2, default=str) if results else "[]"
    prompt = f"""User asked: "{message}"
Context: {answer_hint}
Query returned {len(results)} result(s):
{result_str}

Write a clear, specific, data-backed answer in 2-5 sentences.
Rules:
- Use actual values from the data (names, IDs, amounts, counts)
- Format monetary values as "X.XX INR"
- If results are empty, say "No matching records found in the dataset."
- Do NOT mention MongoDB, pipelines, collections, or technical details
- Do NOT say "based on the data provided" — just answer directly
- Be concise and professional"""

    msgs = [
        {"role": "system", "content": "You are a SAP business intelligence analyst. Give precise, factual answers based only on the data provided. Never invent data."},
        {"role": "user", "content": prompt}
    ]
    try:
        return await call_groq(msgs, max_tokens=500)
    except Exception:
        if results:
            return f"Found {len(results)} result(s). First result: {json.dumps(results[0], default=str)}"
        return "No matching records found in the dataset."


# ── Guardrail check ───────────────────────────────────────────────────────────

GUARDRAIL_PATTERNS = [
    r'\b(capital|president|prime minister|population|geography|country|continent)\b',
    r'\b(poem|joke|story|write me|tell me a|creative|fiction|essay)\b',
    r'\b(weather|temperature|forecast|climate)\b',
    r'\b(sports?|cricket|football|soccer|basketball|team|player)\b',
    r'\b(stock price|cryptocurrency|bitcoin|forex|share price)\b',
    r'\b(code|python|javascript|java|html|css|programming|function|loop|algorithm)\b(?!.*(?:sap|o2c|order|billing|delivery))',
    r'\b(recipe|food|cook|restaurant|hotel)\b',
    r'\b(translate|language|grammar|spell)\b',
    r'\b(movie|film|song|music|actor|celebrity)\b',
    r'\bwhat is \d+[\+\-\*\/]\d+\b',
    r'\b(history|world war|ancient|emperor|king|queen)\b(?!.*(?:order|billing|delivery|customer))',
]

SAP_PATTERNS = [
    r'\b(sales order|billing|delivery|payment|invoice|customer|product|journal|material|plant|shipment|o2c|order.to.cash|revenue|receipt|dispatch|goods|warehouse)\b'
]

def is_off_topic(message: str) -> bool:
    msg = message.lower()
    # If clearly SAP-related, never reject
    for p in SAP_PATTERNS:
        if re.search(p, msg):
            return False
    # Check off-topic patterns
    for p in GUARDRAIL_PATTERNS:
        if re.search(p, msg):
            return True
    return False


# ── Main entry point ──────────────────────────────────────────────────────────

def compute_confidence(results: list, answer_hint: str, source: str) -> float:
    """
    Heuristic confidence score (0.0 – 1.0) based on:
    - source: 'direct' handlers are always high confidence
    - result count: empty results = lower confidence
    - answer_hint quality
    """
    if source == "direct":
        if not results:
            return 0.55   # direct handler ran but returned nothing (no matching docs)
        return 0.95       # direct handlers are deterministic MongoDB queries

    # LLM-generated pipeline
    if not results:
        return 0.40       # LLM ran but found nothing — may be a bad pipeline

    count = len(results)
    # More results = more likely the pipeline is doing something real
    base = 0.60
    if count >= 5:
        base = 0.72
    if count >= 10:
        base = 0.80

    # Boost if the answer_hint contains specific field names (structured output)
    specific_keywords = ["billing", "order", "customer", "delivery", "payment", "journal", "revenue"]
    hint_lower = (answer_hint or "").lower()
    matches = sum(1 for kw in specific_keywords if kw in hint_lower)
    base += min(matches * 0.025, 0.10)

    return round(min(base, 0.90), 2)


async def chat_with_groq(db, message: str, history: list) -> dict:
    if not GROQ_API_KEY:
        return {
            "answer": "⚠️ GROQ_API_KEY is not configured. Please add it to backend/.env and restart the server.",
            "pipeline": None, "results": [], "highlighted_nodes": [], "confidence": None
        }

    # Guardrail check
    if is_off_topic(message):
        return {
            "answer": "This system is designed to answer questions related to the SAP Order-to-Cash dataset only. I can help with sales orders, deliveries, billing documents, payments, customers, and products.",
            "pipeline": None, "results": [], "highlighted_nodes": [], "confidence": None
        }

    # Try direct handler first (deterministic — highest confidence)
    try:
        direct = await direct_query(db, message)
        if direct is not None:
            results, answer_hint, highlighted = direct
            answer = await summarize(message, results, answer_hint)
            confidence = compute_confidence(results, answer_hint, source="direct")
            return {
                "answer": answer,
                "pipeline": None,
                "results": results,
                "highlighted_nodes": highlighted,
                "confidence": confidence,
            }
    except Exception as e:
        print(f"Direct handler error: {e}")

    # LLM fallback
    results, answer_hint, refusal, is_relevant = await llm_query(db, message, history)

    if not is_relevant:
        return {
            "answer": refusal or "This system only answers questions about the SAP O2C dataset.",
            "pipeline": None, "results": [], "highlighted_nodes": [], "confidence": None
        }

    if results is None:
        return {
            "answer": "I had trouble processing that query. Try rephrasing it — for example: 'How many billing documents in April 2025?' or 'Which customer has the highest order value?'",
            "pipeline": None, "results": [], "highlighted_nodes": [], "confidence": 0.30
        }

    answer = await summarize(message, results, answer_hint or message)
    confidence = compute_confidence(results, answer_hint, source="llm")

    highlighted = []
    for r in (results or [])[:15]:
        for key in ["salesOrder", "billingDocument", "deliveryDocument",
                    "accountingDocument", "material", "customerId", "_id"]:
            val = r.get(key)
            if val and str(val) not in highlighted:
                highlighted.append(str(val))

    return {
        "answer": answer,
        "pipeline": None,
        "results": results or [],
        "highlighted_nodes": highlighted[:20],
        "confidence": confidence,
    }