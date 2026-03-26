import React, { useEffect, useState } from 'react';
import axios from 'axios';
import './StatsBar.css';

const STAT_LABELS = {
  sales_order_headers:         { label: 'Sales Orders', icon: '🛒' },
  billing_document_headers:    { label: 'Billing Docs',  icon: '🧾' },
  outbound_delivery_headers:   { label: 'Deliveries',    icon: '🚚' },
  payments_accounts_receivable:{ label: 'Payments',      icon: '💳' },
  business_partners:           { label: 'Customers',     icon: '🏢' },
  products:                    { label: 'Products',      icon: '📦' },
};

export default function StatsBar({ api }) {
  const [stats, setStats] = useState({});
  useEffect(() => {
    axios.get(`${api}/stats`).then(r => setStats(r.data)).catch(() => {});
  }, [api]);

  return (
    <div className="stats-bar">
      {Object.entries(STAT_LABELS).map(([key, { label, icon }]) => (
        <div className="stat-chip" key={key}>
          <span className="stat-icon">{icon}</span>
          <span className="stat-count">{stats[key]?.toLocaleString() ?? '…'}</span>
          <span className="stat-label">{label}</span>
        </div>
      ))}
    </div>
  );
}