import React, { useState, useEffect, useRef, useCallback } from 'react';
import ForceGraph2D from 'react-force-graph-2d';
import axios from 'axios';
import ChatPanel from './components/ChatPanel';
import NodeTooltip from './components/NodeTooltip';
import Legend from './components/Legend';
import './App.css';
const API = process.env.REACT_APP_API_URL;
console.log("Using API URL:", API);

const NODE_COLORS = {
  Customer:        '#e74c3c',
  SalesOrder:      '#3b82f6',
  Delivery:        '#22c55e',
  BillingDocument: '#f97316',
  JournalEntry:    '#ec4899',
  Payment:         '#a855f7',
  Product:         '#06b6d4',
  Plant:           '#84cc16',
};

export default function App() {
  const [graphData, setGraphData]       = useState({ nodes: [], links: [] });
  const [selectedNode, setSelectedNode] = useState(null);
  const [tooltipPos, setTooltipPos]     = useState({ x: 0, y: 0 });
  const [loading, setLoading]           = useState(true);
  const [highlightedNodes, setHighlightedNodes] = useState(new Set());
  const [expandedNodes, setExpandedNodes]       = useState(new Set());
  const [granularOverlay, setGranularOverlay]   = useState(true);
  const fgRef        = useRef();
  const containerRef = useRef();

  useEffect(() => { fetchGraph(); }, []);

  const fetchGraph = async () => {
    setLoading(true);
    try {
      const { data } = await axios.get(`${API}/graph?limit=60`);
      setGraphData({
        nodes: data.nodes,
        links: data.edges.map(e => ({ ...e, source: e.source, target: e.target })),
      });
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  const expandNode = useCallback(async (node) => {
    if (expandedNodes.has(node.id)) return;
    setExpandedNodes(prev => new Set([...prev, node.id]));
    const [type, id] = node.id.split(':');
    try {
      const { data } = await axios.post(`${API}/graph/expand`, { node_id: id, node_type: type });
      setGraphData(prev => {
        const existingIds      = new Set(prev.nodes.map(n => n.id));
        const existingLinkIds  = new Set(prev.links.map(l => l.id));
        const newNodes = data.nodes.filter(n => !existingIds.has(n.id));
        const newLinks = data.edges
          .filter(e => !existingLinkIds.has(e.id))
          .map(e => ({ ...e, source: e.source, target: e.target }));
        return { nodes: [...prev.nodes, ...newNodes], links: [...prev.links, ...newLinks] };
      });
    } catch (e) { console.error(e); }
  }, [expandedNodes]);

  const handleNodeClick = useCallback((node, event) => {
    setSelectedNode(node);
    expandNode(node);
    if (containerRef.current) {
      const rect = containerRef.current.getBoundingClientRect();
      setTooltipPos({ x: event.clientX - rect.left, y: event.clientY - rect.top });
    }
  }, [expandNode]);

  const handleBackgroundClick = useCallback(() => setSelectedNode(null), []);

  const handleHighlight = useCallback((ids) => {
    setHighlightedNodes(new Set(ids));
    if (ids.length > 0 && fgRef.current) {
      const target = graphData.nodes.find(n => n.id.includes(ids[0]));
      if (target) {
        fgRef.current.centerAt(target.x, target.y, 800);
        fgRef.current.zoom(4, 800);
      }
    }
  }, [graphData.nodes]);

  const getNodeColor = (node) => {
    const base = NODE_COLORS[node.type] || '#94a3b8';
    if (highlightedNodes.size > 0) {
      const isHighlighted = [...highlightedNodes].some(id => node.id.includes(id));
      return isHighlighted ? base : base + '33';
    }
    return base;
  };

  const getNodeRadius = (node) => {
    if (selectedNode?.id === node.id) return 7;
    const big = ['Customer', 'SalesOrder'];
    return big.includes(node.type) ? 5 : 4;
  };

  return (
    <div className="app">
      <header className="topbar">
        <button className="sidebar-toggle">☰</button>
        <span className="topbar-divider"> | </span>
        <span className="breadcrumb-parent">Mapping</span>
        <span className="breadcrumb-slash"> / </span>
        <span className="breadcrumb-current">Order to Cash</span>
      </header>

      <div className="main-layout">
        {/* ── Graph ── */}
        <div className="graph-panel" ref={containerRef}>
          <div className="graph-overlay-btns">
            <button className="overlay-btn" onClick={() => fgRef.current?.zoomToFit(400, 40)}>
              ⤢ Minimize
            </button>
            <button className="overlay-btn dark" onClick={() => setGranularOverlay(g => !g)}>
              ▦ {granularOverlay ? 'Hide' : 'Show'} Granular Overlay
            </button>
          </div>

          {loading ? (
            <div className="loading-overlay">
              <div className="spinner" />
              <p>Building graph...</p>
            </div>
          ) : (
            <ForceGraph2D
              ref={fgRef}
              graphData={graphData}
              nodeId="id"
              nodeLabel={() => ''}
              nodeColor={getNodeColor}
              nodeVal={getNodeRadius}
              linkColor={() => 'rgba(147,197,253,0.45)'}
              linkWidth={0.8}
              linkDirectionalArrowLength={0}
              onNodeClick={handleNodeClick}
              onBackgroundClick={handleBackgroundClick}
              nodeCanvasObjectMode={() => 'replace'}
              nodeCanvasObject={(node, ctx) => {
                const r     = getNodeRadius(node);
                const color = getNodeColor(node);
                const isSel = selectedNode?.id === node.id;

                // Selected ring
                if (isSel) {
                  ctx.beginPath();
                  ctx.arc(node.x, node.y, r + 4, 0, 2 * Math.PI);
                  ctx.strokeStyle = color;
                  ctx.lineWidth = 2;
                  ctx.stroke();
                }

                // Fill
                ctx.beginPath();
                ctx.arc(node.x, node.y, r, 0, 2 * Math.PI);
                ctx.fillStyle = color;
                ctx.fill();

                // Thin border
                ctx.beginPath();
                ctx.arc(node.x, node.y, r, 0, 2 * Math.PI);
                ctx.strokeStyle = 'rgba(255,255,255,0.4)';
                ctx.lineWidth = 0.6;
                ctx.stroke();
              }}
              backgroundColor="#f0f2f5"
              cooldownTicks={120}
              d3AlphaDecay={0.015}
              d3VelocityDecay={0.25}
            />
          )}

          {/* Floating tooltip on graph */}
          {selectedNode && (
            <NodeTooltip
              node={selectedNode}
              pos={tooltipPos}
              onClose={() => setSelectedNode(null)}
              containerRef={containerRef}
            />
          )}

          {/* Legend bottom-left */}
          <Legend nodeColors={NODE_COLORS} />
        </div>

        {/* ── Chat ── */}
        <div className="right-panels">
          <ChatPanel api={API} onHighlight={handleHighlight} />
        </div>
      </div>
    </div>
  );
}