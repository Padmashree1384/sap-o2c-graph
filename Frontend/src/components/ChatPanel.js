import React, { useState, useRef, useEffect } from 'react';
import axios from 'axios';
import './ChatPanel.css';

const EXAMPLE_QUERIES = [
  "Which products have the most billing documents?",
  "Trace full flow of billing document 90504248",
  "Sales orders delivered but not billed",
  "Customer with highest total order value",
  "Show cancelled billing documents",
];

// Fields to skip in table display
const SKIP_FIELDS = ['_id', '__v'];

// Human-readable label map
const LABEL_MAP = {
  billingDocument:            'Billing Doc',
  salesOrder:                 'Sales Order',
  deliveryDocument:           'Delivery Doc',
  accountingDocument:         'Accounting Doc',
  creationDate:               'Created',
  billingDocumentDate:        'Billing Date',
  postingDate:                'Posting Date',
  totalNetAmount:             'Net Amount',
  netAmount:                  'Net Amount',
  amountInTransactionCurrency:'Amount',
  transactionCurrency:        'Currency',
  soldToParty:                'Customer ID',
  customerName:               'Customer',
  customerId:                 'Customer ID',
  billingDocumentType:        'Doc Type',
  billingDocumentIsCancelled: 'Cancelled',
  cancelledBillingDocument:   'Cancels Doc',
  material:                   'Material',
  billingCount:               'Billing Count',
  orderCount:                 'Order Count',
  totalOrderValue:            'Total Order Value',
  totalValue:                 'Total Value',
  businessPartnerName:        'Customer Name',
  overallDeliveryStatus:      'Delivery Status',
  overallOrdReltdBillgStatus: 'Billing Status',
  count:                      'Count',
  total:                      'Total',
  companyCode:                'Company',
  plant:                      'Plant',
  storageLocation:            'Storage Loc',
  clearingDate:               'Clearing Date',
  goodsMovementStatus:        'Goods Status',
  glAccount:                  'GL Account',
  fiscalYear:                 'Fiscal Year',
  billingDocumentItem:        'Item',
  clearingAccountingDocument: 'Clearing Doc',
  amount:                     'Amount',
  quantity:                   'Quantity',
  deliveryRef:                'Delivery Ref',
  itemCount:                  'Items',
  deliveryCount:              'Deliveries',
};

const CURRENCY_FIELDS = [
  'totalNetAmount','netAmount','amountInTransactionCurrency',
  'totalValue','totalOrderValue','totalRevenue','totalPayments','avgOrderValue','amount'
];

function humanLabel(key) {
  return LABEL_MAP[key] || key.replace(/([A-Z])/g, ' $1').replace(/^./, s => s.toUpperCase()).trim();
}

/**
 * Flatten one result row: expand nested objects/arrays into readable strings.
 * This prevents [object Object] from ever reaching the table cells.
 */
function flattenRow(row) {
  const flat = {};
  for (const [key, val] of Object.entries(row)) {
    if (SKIP_FIELDS.includes(key)) continue;

    if (Array.isArray(val)) {
      if (val.length === 0) {
        flat[key] = '—';
      } else if (typeof val[0] === 'object' && val[0] !== null) {
        // Pick the most meaningful id-like field from the first item
        const first = val[0];
        const idKey = Object.keys(first).find(k =>
          k.toLowerCase().includes('document') ||
          k.toLowerCase().includes('order') ||
          k.toLowerCase().includes('material') ||
          k.toLowerCase().includes('account')
        );
        const preview = idKey ? first[idKey] : Object.values(first)[0];
        flat[key] = val.length === 1
          ? String(preview ?? '—')
          : `${val.length} items (e.g. ${preview ?? '—'})`;
      } else {
        flat[key] = val.slice(0, 3).join(', ') + (val.length > 3 ? ` +${val.length - 3}` : '');
      }
    } else if (val !== null && typeof val === 'object') {
      // Single nested object: show its most meaningful field
      const subEntries = Object.entries(val).filter(([k]) => !SKIP_FIELDS.includes(k));
      if (subEntries.length === 0) {
        flat[key] = '—';
      } else {
        const meaningful = subEntries.find(([k]) =>
          k.toLowerCase().includes('name') || k.toLowerCase().includes('id') ||
          k.toLowerCase().includes('document') || k.toLowerCase().includes('amount')
        );
        flat[key] = meaningful
          ? `${humanLabel(meaningful[0])}: ${meaningful[1]}`
          : subEntries.slice(0, 2).map(([k, v]) => `${humanLabel(k)}: ${v}`).join(' · ');
      }
    } else {
      flat[key] = val;
    }
  }
  return flat;
}

function formatValue(key, val) {
  if (val === null || val === undefined || val === '') return '—';
  if (typeof val === 'boolean') return val ? 'Yes' : 'No';
  if (typeof val === 'string') {
    if (val.includes('T') && val.includes(':')) return val.split('T')[0];
    if (CURRENCY_FIELDS.includes(key)) {
      const num = parseFloat(val);
      if (!isNaN(num)) return num.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' INR';
    }
    return val;
  }
  if (typeof val === 'number') {
    if (CURRENCY_FIELDS.includes(key))
      return val.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' INR';
    return val.toLocaleString('en-IN');
  }
  return String(val);
}

function getStatusBadge(key, val) {
  if (key === 'billingDocumentIsCancelled') {
    return val ? <span className="badge badge-red">Cancelled</span> : <span className="badge badge-green">Active</span>;
  }
  if (key === 'overallDeliveryStatus') {
    const map = { A: ['Not Started', 'badge-gray'], B: ['Partial', 'badge-yellow'], C: ['Complete', 'badge-green'] };
    const [label, cls] = map[val] || [val, 'badge-gray'];
    return <span className={`badge ${cls}`}>{label}</span>;
  }
  if (key === 'overallOrdReltdBillgStatus') {
    const map = { A: ['Not Billed', 'badge-gray'], B: ['Partial', 'badge-yellow'], C: ['Billed', 'badge-green'] };
    const [label, cls] = map[val] || [val, 'badge-gray'];
    return <span className={`badge ${cls}`}>{label}</span>;
  }
  return null;
}

const BADGE_FIELDS = ['billingDocumentIsCancelled', 'overallDeliveryStatus', 'overallOrdReltdBillgStatus'];

function ResultsTable({ results }) {
  if (!results || results.length === 0) return null;

  // Flatten rows first to eliminate [object Object]
  const flatResults = results.map(flattenRow);

  // Gather all keys from first few flattened rows
  const allKeys = [];
  const seen = new Set();
  flatResults.slice(0, 5).forEach(row => {
    Object.keys(row).forEach(k => {
      if (!seen.has(k) && !SKIP_FIELDS.includes(k)) {
        seen.add(k);
        allKeys.push(k);
      }
    });
  });

  const [page, setPage] = useState(0);
  const PAGE_SIZE = 5;
  const totalPages = Math.ceil(flatResults.length / PAGE_SIZE);
  const pageRows = flatResults.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <div className="results-table-wrap">
      <div className="results-table-scroll">
        <table className="results-table">
          <thead>
            <tr>
              {allKeys.map(k => (
                <th key={k}>{humanLabel(k)}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {pageRows.map((row, i) => (
              <tr key={i}>
                {allKeys.map(k => {
                  const val = row[k];
                  const badge = BADGE_FIELDS.includes(k) ? getStatusBadge(k, val) : null;
                  return (
                    <td key={k}>
                      {badge || formatValue(k, val)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalPages > 1 && (
        <div className="table-pagination">
          <button
            className="page-btn"
            onClick={() => setPage(p => Math.max(0, p - 1))}
            disabled={page === 0}
          >‹</button>
          <span className="page-info">{page + 1} / {totalPages}</span>
          <button
            className="page-btn"
            onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
            disabled={page === totalPages - 1}
          >›</button>
          <span className="page-total">{flatResults.length} rows</span>
        </div>
      )}
    </div>
  );
}

function ConfidenceBadge({ confidence }) {
  if (confidence === undefined || confidence === null) return null;
  let cls = 'conf-high';
  let label = 'High confidence';
  if (confidence < 0.5) { cls = 'conf-low'; label = 'Low confidence'; }
  else if (confidence < 0.8) { cls = 'conf-mid'; label = 'Medium confidence'; }
  return (
    <div className={`confidence-badge ${cls}`}>
      <span className="conf-dot" />
      <span>{label}</span>
      <span className="conf-pct">{Math.round(confidence * 100)}%</span>
    </div>
  );
}

export default function ChatPanel({ api, onHighlight }) {
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content: 'Hi! I can help you analyze the <strong>Order to Cash</strong> process.',
      isHtml: true,
    },
  ]);
  const [input, setInput]     = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef();

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  const sendMessage = async (text) => {
    const userMsg = text || input.trim();
    if (!userMsg || loading) return;
    setInput('');

    const history = messages
      .filter(m => m.role !== 'system')
      .map(m => ({ role: m.role, content: m.content }));

    setMessages(prev => [...prev, { role: 'user', content: userMsg }]);
    setLoading(true);

    try {
      const { data } = await axios.post(`${api}/chat`, { message: userMsg, history });
      setMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: data.answer,
          results: data.results,
          highlighted: data.highlighted_nodes,
          confidence: data.confidence,
        },
      ]);
      if (data.highlighted_nodes?.length > 0) onHighlight(data.highlighted_nodes);
    } catch (e) {
      setMessages(prev => [
        ...prev,
        { role: 'assistant', content: '⚠️ Cannot reach server. Check that backend is running on port 8000.' },
      ]);
    }
    setLoading(false);
  };

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
  };

  return (
    <div className="chat-panel">
      {/* Header */}
      <div className="chat-header">
        <div>
          <div className="chat-header-title">Chat with Graph</div>
          <div className="chat-header-sub">Order to Cash</div>
        </div>
      </div>

      {/* Messages */}
      <div className="chat-messages">
        <div className="agent-intro">
          <div className="agent-avatar">D</div>
          <div>
            <div className="agent-name">Dodge AI</div>
            <div className="agent-role">Graph Agent</div>
          </div>
        </div>

        {messages.map((msg, i) => (
          <div key={i} className={`msg-row ${msg.role}`}>
            {msg.role === 'assistant' && <div className="msg-agent-avatar">D</div>}
            {msg.role === 'user'      && <div className="msg-user-label">You</div>}

            <div className={`msg-bubble ${msg.role}`}>
              {msg.isHtml
                ? <p dangerouslySetInnerHTML={{ __html: msg.content }} />
                : <p>{msg.content}</p>
              }

              {/* Confidence badge */}
              {msg.role === 'assistant' && msg.confidence !== undefined && (
                <ConfidenceBadge confidence={msg.confidence} />
              )}

              {/* Mini table */}
              {msg.results?.length > 0 && (
                <ResultsTable results={msg.results} />
              )}

              {msg.highlighted?.length > 0 && (
                <div className="highlight-note">
                  📍 {msg.highlighted.length} node{msg.highlighted.length !== 1 ? 's' : ''} highlighted on graph
                </div>
              )}
            </div>

            {msg.role === 'user' && <div className="user-avatar-bubble" />}
          </div>
        ))}

        {loading && (
          <div className="msg-row assistant">
            <div className="msg-agent-avatar">D</div>
            <div className="msg-bubble assistant typing">
              <span /><span /><span />
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Examples */}
      <div className="examples-section">
        <p className="examples-label">Try asking</p>
        <div className="examples-list">
          {EXAMPLE_QUERIES.map((q, idx) => (
            <button key={idx} className="example-chip" onClick={() => sendMessage(q)}>{q}</button>
          ))}
        </div>
      </div>

      {/* Bottom */}
      <div className="chat-bottom">
        <div className="status-row">
          <span className="status-dot" />
          <span>{loading ? 'Analyzing...' : 'Dodge AI is awaiting instructions'}</span>
        </div>
        <div className="chat-input-row">
          <input
            className="chat-input"
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Analyze anything"
            disabled={loading}
          />
          <button
            className="send-btn"
            onClick={() => sendMessage()}
            disabled={loading || !input.trim()}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}