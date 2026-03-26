import React from 'react';
import './Legend.css';

export default function Legend({ nodeColors }) {
  return (
    <div className="legend">
      {Object.entries(nodeColors).map(([type, color]) => (
        <div className="legend-item" key={type}>
          <span className="legend-dot" style={{ background: color }} />
          <span className="legend-label">{type}</span>
        </div>
      ))}
      <div className="legend-hint">Click node to inspect • Click again to expand</div>
    </div>
  );
}