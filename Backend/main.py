from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db import db
from graph import get_graph_data, get_node_neighbors
from llm import chat_with_groq

app = FastAPI(title="SAP O2C Graph API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    message: str
    history: list = []


class ExpandRequest(BaseModel):
    node_id: str
    node_type: str


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/graph")
async def get_graph(limit: int = 60):
    return await get_graph_data(db, limit)


@app.post("/graph/expand")
async def expand_node(req: ExpandRequest):
    return await get_node_neighbors(db, req.node_id, req.node_type)


@app.post("/chat")
async def chat(req: ChatRequest):
    return await chat_with_groq(db, req.message, req.history)


@app.get("/stats")
async def stats():
    collections = [
        "sales_order_headers", "sales_order_items",
        "billing_document_headers", "billing_document_items",
        "outbound_delivery_headers", "outbound_delivery_items",
        "payments_accounts_receivable",
        "journal_entry_items_accounts_receivable",
        "business_partners", "products",
    ]
    return {col: await db[col].count_documents({}) for col in collections}
