# 🔗 SAP O2C Graph Explorer

**An AI-powered interactive graph explorer for SAP Order-to-Cash processes.**  
Chat with your data. Visualize the full O2C flow. Trace any document in seconds.

[🚀 Live Demo](https://sap-o2c-graph-frontend.onrender.com) · [🔧 Backend API](https://sap-o2c-backend-zdy0.onrender.com/docs)



## ✨ Features

- **🤖 AI Chat Interface** — Ask natural language questions about your SAP data powered by Groq LLaMA 3.3-70B
- **🕸️ Interactive Force Graph** — Real-time graph visualization of the full Order-to-Cash flow using `react-force-graph-2d`
- **🔍 Node Expansion** — Click any node to expand its neighbors and trace relationships across the entire O2C chain
- **📊 Live Data Tables** — Query results rendered as paginated, sortable tables directly in the chat
- **🎯 Node Highlighting** — Chat responses automatically highlight relevant nodes on the graph
- **⚡ Hybrid Query Engine** — Direct MongoDB handlers for common patterns + LLM fallback for complex queries
- **🛡️ Guardrail System** — Built-in relevance filtering ensures responses stay focused on O2C data
- **📈 Confidence Scores** — Every AI response includes a confidence rating

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     React Frontend                       │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ ForceGraph2D │  │  ChatPanel   │  │  NodeTooltip  │  │
│  │  (D3-based)  │  │ (AI Chat UI) │  │   (Inspector) │  │
│  └──────────────┘  └──────────────┘  └───────────────┘  │
└────────────────────────┬────────────────────────────────┘
                         │ REST API (Axios)
┌────────────────────────▼────────────────────────────────┐
│                  FastAPI Backend                          │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │  /graph      │  │  /chat       │  │  /graph/expand│  │
│  │  (graph data)│  │  (LLM+DB)    │  │  (neighbors)  │  │
│  └──────────────┘  └──────────────┘  └───────────────┘  │
│                    graph.py  llm.py                       │
└────────────────────────┬────────────────────────────────┘
                         │ Motor (Async)
┌────────────────────────▼────────────────────────────────┐
│               MongoDB Atlas (sap_o2c)                    │
│  sales_order_headers │ billing_document_headers          │
│  outbound_delivery_* │ payments_accounts_receivable      │
│  business_partners   │ journal_entry_items_*             │
│  products            │ plants                            │
└─────────────────────────────────────────────────────────┘
```

---

## 🔄 Order-to-Cash Flow

```
Customer ──► Sales Order ──► Sales Order Items
                                    │
                                    ▼
                            Outbound Delivery
                                    │
                                    ▼
                          Billing Document ──► Journal Entry ──► Payment
```

Each node in the graph represents an entity in this flow. Edges represent business relationships.

---

## 🚀 Getting Started

### Prerequisites

- Node.js 18+
- Python 3.10+
- MongoDB Atlas account
- Groq API key ([get one free](https://console.groq.com))

### 1. Clone the Repository

```bash
git clone https://github.com/Padmashree1384/sap-o2c-graph.git
cd sap-o2c-graph
```

### 2. Backend Setup

```bash
cd Backend
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # macOS/Linux

pip install -r requirements.txt
```

Create a `.env` file in the `Backend/` directory:

```env
MONGO_URI=mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/
DB_NAME=sap_o2c
GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxx
```

### 3. Ingest Data

```bash
python ingest.py --data-dir ../sap-o2c-data
```

You should see:
```
Connecting to: mongodb+srv://...
  OK sales_order_headers: 100 records
  OK billing_document_headers: 163 records
  ...
Ingestion complete.
```

### 4. Start the Backend

```bash
uvicorn main:app --reload --port 8000
```

API will be live at `http://localhost:8000`  
Swagger docs at `http://localhost:8000/docs`

### 5. Frontend Setup

```bash
cd ../Frontend
npm install
```

Create a `.env` file in the `Frontend/` directory:

```env
REACT_APP_API_URL=http://localhost:8000
```

### 6. Start the Frontend

```bash
npm start
```

Open [http://localhost:3000](http://localhost:3000) 🎉

---

## 📡 API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Health check |
| `GET` | `/graph?limit=60` | Fetch graph nodes and edges |
| `POST` | `/graph/expand` | Expand a node's neighbors |
| `POST` | `/chat` | Send a message to the AI agent |
| `GET` | `/stats` | Collection document counts |

### Example Chat Request

```json
POST /chat
{
  "message": "Which customer has the highest total order value?",
  "history": []
}
```

### Example Chat Response

```json
{
  "answer": "Customer Nelson, Fitzpatrick and Jordan (ID 320000083) has the highest total order value of 1,24,500.00 INR across 12 sales orders.",
  "results": [...],
  "highlighted_nodes": ["320000083"],
  "confidence": 0.95
}
```

---

## 🗂️ Project Structure

```
sap-o2c-graph/
├── Backend/
│   ├── main.py          # FastAPI app & routes
│   ├── db.py            # MongoDB connection
│   ├── graph.py         # Graph construction logic
│   ├── llm.py           # Groq LLM + query engine
│   ├── ingest.py        # Data ingestion script
│   └── .env             # Environment variables (not committed)
│
├── Frontend/
│   ├── src/
│   │   ├── App.js           # Main app + ForceGraph2D
│   │   ├── components/
│   │   │   ├── ChatPanel.js     # AI chat interface
│   │   │   ├── NodeTooltip.js   # Node inspector popup
│   │   │   ├── Legend.js        # Graph legend
│   │   │   └── StatsBar.js      # Collection stats
│   │   └── index.js
│   └── .env             # Environment variables (not committed)
│
└── sap-o2c-data/        # JSONL data files (not committed)
    ├── sales_order_headers/
    ├── billing_document_headers/
    └── ...
```

---

## 🧠 AI Query Engine

The chat system uses a **two-layer hybrid approach**:

1. **Direct Handlers** — Pre-built MongoDB aggregation pipelines for the 10 most common query patterns (billing traces, top customers, cancelled docs, etc.). These run instantly with 95% confidence.

2. **LLM Fallback** — For everything else, Groq's LLaMA 3.3-70B generates a MongoDB aggregation pipeline on the fly, executes it, and summarizes the results in plain English.

### Example Queries to Try

```
Which products have the most billing documents?
Trace full flow of billing document 90504248
Sales orders delivered but not billed
Customer with highest total order value
Show cancelled billing documents
```

---

## 🌐 Deployment

### Backend — Render

Set these environment variables in the Render dashboard:

| Key | Value |
|-----|-------|
| `MONGO_URI` | `mongodb+srv://...` |
| `DB_NAME` | `sap_o2c` |
| `GROQ_API_KEY` | `gsk_...` |

**Build Command:** `pip install -r requirements.txt`  
**Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`

### Frontend — Render

Set this environment variable in the Render dashboard:

| Key | Value |
|-----|-------|
| `REACT_APP_API_URL` | `https://your-backend.onrender.com` |

**Build Command:** `npm install && npm run build`  
**Publish Directory:** `build`

> ⚠️ **Important:** `REACT_APP_API_URL` is baked in at **build time**. Always set it in the Render dashboard, not just locally. After changing it, trigger a manual redeploy.

---

## 🎨 Graph Node Types

| Node | Color | Represents |
|------|-------|------------|
| 🔴 Customer | `#e74c3c` | Business partner / sold-to party |
| 🔵 SalesOrder | `#3b82f6` | SAP sales order header |
| 🟢 Delivery | `#22c55e` | Outbound delivery document |
| 🟠 BillingDocument | `#f97316` | Invoice or cancellation |
| 🩷 JournalEntry | `#ec4899` | Accounting document |
| 🟣 Payment | `#a855f7` | Accounts receivable payment |
| 🩵 Product | `#06b6d4` | Material / product master |
| 🟡 Plant | `#84cc16` | Storage / production plant |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, react-force-graph-2d, Axios |
| Backend | FastAPI, Motor (async MongoDB) |
| Database | MongoDB Atlas |
| AI | Groq Cloud (LLaMA 3.3-70B) |
| Hosting | Render (frontend + backend) |

---

## 📄 License

MIT License — feel free to use and adapt this project.

---

<div align="center">
Built with ❤️ · Powered by Groq + MongoDB Atlas
</div>
