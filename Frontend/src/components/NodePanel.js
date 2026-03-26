import React from 'react';
import './NodePanel.css';

const TYPE_ICONS = {
  SalesOrder: '🛒', BillingDocument: '🧾', Delivery: '🚚',
  Payment: '💳', JournalEntry: '📒', Customer: '🏢', Product: '📦', Plant: '🏭',
};

const FIELD_LABELS = {
  salesOrder: 'Sales Order', soldToParty: 'Customer ID', totalNetAmount: 'Net Amount',
  transactionCurrency: 'Currency', creationDate: 'Created',
  overallDeliveryStatus: 'Delivery Status', overallOrdReltdBillgStatus: 'Billing Status',
  billingDocument: 'Billing Doc', billingDocumentType: 'Type',
  billingDocumentIsCancelled: 'Cancelled', accountingDocument: 'Accounting Doc',
  deliveryDocument: 'Delivery Doc', overallGoodsMovementStatus: 'Goods Status',
  overallPickingStatus: 'Picking Status', shippingPoint: 'Shipping Point',
  amountInTransactionCurrency: 'Amount', postingDate: 'Posting Date',
  clearingDate: 'Clearing Date', businessPartnerName: 'Name',
  businessPartnerFullName: 'Full Name', businessPartnerIsBlocked: 'Blocked',
  product: 'Product ID', productOldId: 'Old ID', productType: 'Type',
  productGroup: 'Group', grossWeight: 'Gross Weight', weightUnit: 'Weight Unit',
  referenceDocument: 'Reference Doc', glAccount: 'GL Account',
};

function formatValue(key, val) {
  if (val === null || val === undefined || val === '') return '—';
  if (typeof val === 'boolean') return val ? 'Yes' : 'No';
  if (typeof val === 'object' && val.hours !== undefined)
    return `${String(val.hours).padStart(2,'0')}:${String(val.minutes).padStart(2,'0')}`;
  if (key.toLowerCase().includes('date') && typeof val === 'string' && val.includes('T'))
    return val.split('T')[0];
  if (key.toLowerCase().includes('amount') && !isNaN(parseFloat(val)))
    return `₹ ${parseFloat(val).toLocaleString('en-IN', { minimumFractionDigits: 2 })}`;
  return String(val);
}

const PRIORITY_FIELDS = [
  'salesOrder','billingDocument','deliveryDocument','accountingDocument',
  'businessPartnerName','product','productOldId','soldToParty','customer',
  'totalNetAmount','amountInTransactionCurrency','transactionCurrency',
  'creationDate','postingDate','clearingDate',
  'overallDeliveryStatus','overallOrdReltdBillgStatus','overallGoodsMovementStatus',
  'billingDocumentIsCancelled','businessPartnerIsBlocked',
];

export default function NodePanel({ node, onClose }) {
  if (!node) return null;
  const data = node.data || {};
  const priority = PRIORITY_FIELDS.filter(k => k in data && data[k] !== '' && data[k] !== null);
  const rest = Object.keys(data).filter(
    k => !priority.includes(k) &&
         !['_id','creationTime','lastChangeDateTime','lastChangeDate','createdByUser'].includes(k)
  );

  const renderRow = (key) => {
    const val = data[key];
    if (val === '' || val === null || val === undefined) return null;
    const label = FIELD_LABELS[key] || key.replace(/([A-Z])/g, ' $1').trim();
    return (
      <tr key={key}>
        <td className="field-key">{label}</td>
        <td className="field-val">{formatValue(key, val)}</td>
      </tr>
    );
  };

  return (
    <div className="node-panel">
      <div className="node-panel-header">
        <span className="node-icon">{TYPE_ICONS[node.type] || '⬡'}</span>
        <div className="node-title">
          <span className="node-type">{node.type}</span>
          <span className="node-label">{node.label}</span>
        </div>
        <button className="close-btn" onClick={onClose}>✕</button>
      </div>
      <div className="node-panel-body">
        <table className="fields-table">
          <tbody>
            {priority.map(renderRow)}
            {rest.length > 0 && priority.length > 0 && (
              <tr className="divider-row"><td colSpan={2} /></tr>
            )}
            {rest.map(renderRow)}
          </tbody>
        </table>
      </div>
    </div>
  );
}