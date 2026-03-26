"""
Graph construction: nodes and edges from MongoDB collections.
"""
from typing import Any

# Node type colors for frontend
NODE_COLORS = {
    "SalesOrder": "#4f9cf9",
    "BillingDocument": "#f97316",
    "Delivery": "#22c55e",
    "Payment": "#a855f7",
    "JournalEntry": "#ec4899",
    "Customer": "#facc15",
    "Product": "#06b6d4",
    "Plant": "#84cc16",
}

NODE_SIZES = {
    "SalesOrder": 20,
    "BillingDocument": 18,
    "Delivery": 18,
    "Payment": 16,
    "JournalEntry": 14,
    "Customer": 22,
    "Product": 16,
    "Plant": 14,
}


def make_node(id: str, node_type: str, label: str, data: dict) -> dict:
    # Remove mongo _id
    data.pop("_id", None)
    return {
        "id": f"{node_type}:{id}",
        "type": node_type,
        "label": label,
        "data": data,
        "color": NODE_COLORS.get(node_type, "#888"),
        "size": NODE_SIZES.get(node_type, 14),
    }


def make_edge(source: str, target: str, label: str) -> dict:
    return {
        "id": f"{source}->{target}",
        "source": source,
        "target": target,
        "label": label,
    }


async def get_graph_data(db, limit: int = 50):
    nodes = []
    edges = []
    seen_nodes = set()
    seen_edges = set()

    def add_node(n):
        if n["id"] not in seen_nodes:
            nodes.append(n)
            seen_nodes.add(n["id"])

    def add_edge(e):
        if e["id"] not in seen_edges:
            edges.append(e)
            seen_edges.add(e["id"])

    # --- Sales Orders ---
    async for so in db.sales_order_headers.find({}, limit=limit):
        so_id = so["salesOrder"]
        n = make_node(so_id, "SalesOrder", f"SO {so_id}", so)
        add_node(n)

        # Customer
        customer_id = so.get("soldToParty")
        if customer_id:
            bp = await db.business_partners.find_one({"businessPartner": customer_id})
            label = bp.get("businessPartnerName", customer_id) if bp else customer_id
            if bp:
                bp.pop("_id", None)
            cn = make_node(customer_id, "Customer", label, bp or {"customer": customer_id})
            add_node(cn)
            add_edge(make_edge(cn["id"], n["id"], "placed"))

    # --- Deliveries linked to Sales Orders ---
    async for di in db.outbound_delivery_items.find({}, limit=limit * 3):
        so_id = di.get("referenceSdDocument")
        del_id = di.get("deliveryDocument")
        if not so_id or not del_id:
            continue
        dh = await db.outbound_delivery_headers.find_one({"deliveryDocument": del_id})
        if dh:
            dh.pop("_id", None)
        dn = make_node(del_id, "Delivery", f"DEL {del_id}", dh or {"deliveryDocument": del_id})
        add_node(dn)
        so_node_id = f"SalesOrder:{so_id}"
        if so_node_id in seen_nodes:
            add_edge(make_edge(so_node_id, dn["id"], "delivered_via"))

    # --- Billing Documents ---
    async for bh in db.billing_document_headers.find({}, limit=limit):
        bd_id = bh["billingDocument"]
        bh.pop("_id", None)
        bn = make_node(bd_id, "BillingDocument", f"BD {bd_id}", bh)
        add_node(bn)

        # Link billing → delivery via billing_document_items
        async for bi in db.billing_document_items.find({"billingDocument": bd_id}):
            ref_del = bi.get("referenceSdDocument")
            if ref_del:
                del_node_id = f"Delivery:{ref_del}"
                if del_node_id in seen_nodes:
                    add_edge(make_edge(del_node_id, bn["id"], "billed_as"))

        # Journal entry
        acc_doc = bh.get("accountingDocument")
        if acc_doc:
            je = await db.journal_entry_items_accounts_receivable.find_one({"accountingDocument": acc_doc})
            if je:
                je.pop("_id", None)
                jn = make_node(acc_doc, "JournalEntry", f"JE {acc_doc}", je)
                add_node(jn)
                add_edge(make_edge(bn["id"], jn["id"], "creates_journal"))

    # --- Payments ---
    async for pmt in db.payments_accounts_receivable.find({}, limit=limit):
        acc_doc = pmt.get("accountingDocument")
        if not acc_doc:
            continue
        pmt.pop("_id", None)
        pn = make_node(acc_doc, "Payment", f"PMT {acc_doc}", pmt)
        add_node(pn)
        # Link to billing via journal
        je_node_id = f"JournalEntry:{acc_doc}"
        if je_node_id in seen_nodes:
            add_edge(make_edge(je_node_id, pn["id"], "cleared_by"))

    return {"nodes": nodes, "edges": edges}


async def get_node_neighbors(db, node_id: str, node_type: str):
    """Expand a node: return its direct neighbors."""
    nodes = []
    edges = []
    seen = set()
    seen_edges = set()

    def add_node(n):
        if n["id"] not in seen:
            nodes.append(n)
            seen.add(n["id"])

    def add_edge(e):
        if e["id"] not in seen_edges:
            edges.append(e)
            seen_edges.add(e["id"])

    parent_node_id = f"{node_type}:{node_id}"

    if node_type == "SalesOrder":
        # Items / products
        async for item in db.sales_order_items.find({"salesOrder": node_id}):
            mat = item.get("material", "")
            item.pop("_id", None)
            pn = make_node(mat, "Product", f"PRD {mat}", item)
            add_node(pn)
            add_edge(make_edge(parent_node_id, pn["id"], "contains"))

        # Deliveries
        async for di in db.outbound_delivery_items.find({"referenceSdDocument": node_id}):
            del_id = di.get("deliveryDocument")
            dh = await db.outbound_delivery_headers.find_one({"deliveryDocument": del_id})
            if dh:
                dh.pop("_id", None)
            dn = make_node(del_id, "Delivery", f"DEL {del_id}", dh or {})
            add_node(dn)
            add_edge(make_edge(parent_node_id, dn["id"], "delivered_via"))

    elif node_type == "Delivery":
        # Billing documents
        async for bi in db.billing_document_items.find({"referenceSdDocument": node_id}):
            bd_id = bi.get("billingDocument")
            bh = await db.billing_document_headers.find_one({"billingDocument": bd_id})
            if bh:
                bh.pop("_id", None)
            bn = make_node(bd_id, "BillingDocument", f"BD {bd_id}", bh or {})
            add_node(bn)
            add_edge(make_edge(parent_node_id, bn["id"], "billed_as"))

    elif node_type == "BillingDocument":
        # Journal entries
        bh = await db.billing_document_headers.find_one({"billingDocument": node_id})
        if bh:
            acc_doc = bh.get("accountingDocument")
            if acc_doc:
                async for je in db.journal_entry_items_accounts_receivable.find({"accountingDocument": acc_doc}):
                    je.pop("_id", None)
                    jn = make_node(acc_doc, "JournalEntry", f"JE {acc_doc}", je)
                    add_node(jn)
                    add_edge(make_edge(parent_node_id, jn["id"], "creates_journal"))

    elif node_type == "Customer":
        # Sales orders
        async for so in db.sales_order_headers.find({"soldToParty": node_id}, limit=10):
            so_id = so["salesOrder"]
            so.pop("_id", None)
            sn = make_node(so_id, "SalesOrder", f"SO {so_id}", so)
            add_node(sn)
            add_edge(make_edge(parent_node_id, sn["id"], "placed"))

    elif node_type == "JournalEntry":
        # Payments
        async for pmt in db.payments_accounts_receivable.find({"accountingDocument": node_id}):
            pmt.pop("_id", None)
            acc = pmt.get("accountingDocument", node_id)
            pn = make_node(acc, "Payment", f"PMT {acc}", pmt)
            add_node(pn)
            add_edge(make_edge(parent_node_id, pn["id"], "cleared_by"))

    return {"nodes": nodes, "edges": edges}