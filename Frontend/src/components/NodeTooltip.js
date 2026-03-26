import React, { useEffect, useRef, useState } from 'react';
import './NodeTooltip.css';

const SKIP_FIELDS = [
  '_id','createdByUser','lastChangeDateTime','lastChangeDate',
  'id','type','label','color','size','x','y','vx','vy',
  'index','__indexColor','fx','fy','__connections'
];

const MAX_VISIBLE = 10;

function formatVal(key, val) {
  if (val === null || val === undefined || val === '') return null;
  if (typeof val === 'boolean') return val ? 'Yes' : 'No';
  if (typeof val === 'object' && val.hours !== undefined)
    return `${String(val.hours).padStart(2,'0')}:${String(val.minutes).padStart(2,'0')}`;
  if (key.toLowerCase().includes('date') && typeof val === 'string' && val.includes('T'))
    return val.split('T')[0];
  return String(val);
}

function camelToTitle(str) {
  return str.replace(/([A-Z])/g, ' $1').replace(/^./, s => s.toUpperCase()).trim();
}

export default function NodeTooltip({ node, pos, onClose, containerRef }) {
  const ref = useRef();
  const [adjustedPos, setAdjustedPos] = useState({ x: pos.x + 16, y: pos.y - 20 });

  const data = node.data || {};
  const entries = Object.entries(data)
    .filter(([k, v]) => !SKIP_FIELDS.includes(k) && v !== null && v !== undefined && v !== '')
    .map(([k, v]) => [k, formatVal(k, v)])
    .filter(([, v]) => v !== null);

  const visible = entries.slice(0, MAX_VISIBLE);
  const hidden = entries.length - MAX_VISIBLE;
  const connections = node.__connections || 0;

  useEffect(() => {
    if (!ref.current || !containerRef?.current) return;
    const container = containerRef.current.getBoundingClientRect();
    const tt = ref.current.getBoundingClientRect();
    let x = pos.x + 16;
    let y = pos.y - 20;
    if (x + tt.width > container.width - 10)  x = pos.x - tt.width - 16;
    if (y + tt.height > container.height - 10) y = container.height - tt.height - 10;
    if (y < 10) y = 10;
    if (x < 10) x = 10;
    setAdjustedPos({ x, y });
  }, [pos, containerRef]);

  return (
    <div ref={ref} className="node-tooltip" style={{ left: adjustedPos.x, top: adjustedPos.y }}>
      <div className="tooltip-title">{node.type}</div>
      <table className="tooltip-table">
        <tbody>
          <tr>
            <td className="tt-key">Entity:</td>
            <td className="tt-val">{node.type}</td>
          </tr>
          {visible.map(([k, v]) => (
            <tr key={k}>
              <td className="tt-key">{camelToTitle(k)}:</td>
              <td className="tt-val">{v}</td>
            </tr>
          ))}
          {hidden > 0 && (
            <tr>
              <td colSpan={2} className="tt-hidden">
                Additional fields hidden for readability
              </td>
            </tr>
          )}
          {connections > 0 && (
            <tr>
              <td className="tt-key">Connections:</td>
              <td className="tt-val tt-connections">{connections}</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}