/**
 * Cowd Knowledge Graph - D3.js Force Layout Visualization (3C-3)
 *
 * Renders a knowledge graph from the backend's temporal knowledge graph
 * using D3.js force-directed layout. Supports:
 * - Interactive node dragging
 * - Zoom & pan
 * - Node click to view details
 * - Edge labels for predicates
 * - Layer-based node coloring
 */

const KnowledgeGraph = {
  svg: null,
  simulation: null,
  width: 0,
  height: 0,

  // Color scheme per layer
  layerColors: {
    l0: '#e94560',
    l1: '#f59e0b',
    l2: '#22c55e',
    l3: '#3b82f6',
    l4: '#8b5cf6',
    entity: '#06b6d4',
    concept: '#ec4899',
    default: '#6b7280'
  },

  /**
   * Render the knowledge graph into a container element.
   * @param {string} containerId - DOM element ID to render into
   * @param {Object} graphData - { nodes: [...], edges: [...] }
   */
  render(containerId, graphData) {
    const container = document.getElementById(containerId);
    if (!container) return;

    // Clear previous
    container.innerHTML = '';

    this.width = container.clientWidth || 800;
    this.height = container.clientHeight || 500;

    const nodes = (graphData?.nodes || []).map((n, i) => ({
      id: n.id || `node-${i}`,
      label: n.label || n.name || n.id || `Node ${i}`,
      layer: n.layer || 'default',
      type: n.type || 'entity',
      ...n
    }));

    const edges = (graphData?.edges || graphData?.triples || []).map((e, i) => ({
      source: e.source || e.subject || e.from,
      target: e.target || e.object || e.to,
      label: e.label || e.predicate || e.relation || '',
      ...e
    }));

    if (nodes.length === 0) {
      container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-dim);">暂无知识图谱数据</div>';
      return;
    }

    // Create SVG
    const svg = d3.select(`#${containerId}`)
      .append('svg')
      .attr('width', this.width)
      .attr('height', this.height)
      .style('background', 'var(--bg, #1a1a2e)')
      .style('border-radius', '8px');

    this.svg = svg;

    // Zoom support
    const g = svg.append('g');
    svg.call(d3.zoom()
      .scaleExtent([0.2, 5])
      .on('zoom', (event) => {
        g.attr('transform', event.transform);
      }));

    // Arrow marker for edges
    svg.append('defs').append('marker')
      .attr('id', 'arrowhead')
      .attr('viewBox', '0 -5 10 10')
      .attr('refX', 20)
      .attr('refY', 0)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('path')
      .attr('d', 'M0,-5L10,0L0,5')
      .attr('fill', 'var(--text-muted, #8888aa)');

    // Force simulation
    this.simulation = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(edges).id(d => d.id).distance(100))
      .force('charge', d3.forceManyBody().strength(-200))
      .force('center', d3.forceCenter(this.width / 2, this.height / 2))
      .force('collision', d3.forceCollide().radius(30));

    // Draw edges
    const link = g.append('g')
      .attr('class', 'kg-edges')
      .selectAll('line')
      .data(edges)
      .join('line')
      .attr('stroke', 'var(--text-dim, #666680)')
      .attr('stroke-width', 1.5)
      .attr('marker-end', 'url(#arrowhead)');

    // Edge labels
    const linkLabel = g.append('g')
      .attr('class', 'kg-edge-labels')
      .selectAll('text')
      .data(edges)
      .join('text')
      .attr('class', 'kg-edge-label')
      .attr('fill', 'var(--text-muted, #8888aa)')
      .attr('font-size', '10px')
      .attr('text-anchor', 'middle')
      .text(d => d.label.length > 20 ? d.label.substring(0, 20) + '...' : d.label);

    // Draw nodes
    const node = g.append('g')
      .attr('class', 'kg-nodes')
      .selectAll('g')
      .data(nodes)
      .join('g')
      .attr('class', 'kg-node')
      .call(d3.drag()
        .on('start', (event, d) => {
          if (!event.active) this.simulation.alphaTarget(0.3).restart();
          d.fx = d.x;
          d.fy = d.y;
        })
        .on('drag', (event, d) => {
          d.fx = event.x;
          d.fy = event.y;
        })
        .on('end', (event, d) => {
          if (!event.active) this.simulation.alphaTarget(0);
          d.fx = null;
          d.fy = null;
        }));

    // Node circles
    node.append('circle')
      .attr('r', d => this._nodeRadius(d))
      .attr('fill', d => this._nodeColor(d))
      .attr('stroke', 'var(--bg, #1a1a2e)')
      .attr('stroke-width', 2)
      .style('cursor', 'pointer')
      .on('click', (event, d) => this._onNodeClick(event, d))
      .on('mouseover', function() {
        d3.select(this).attr('stroke', 'var(--accent, #e94560)').attr('stroke-width', 3);
      })
      .on('mouseout', function() {
        d3.select(this).attr('stroke', 'var(--bg, #1a1a2e)').attr('stroke-width', 2);
      });

    // Node labels
    node.append('text')
      .attr('dy', d => this._nodeRadius(d) + 14)
      .attr('text-anchor', 'middle')
      .attr('fill', 'var(--text, #e8e8f0)')
      .attr('font-size', '11px')
      .text(d => d.label.length > 16 ? d.label.substring(0, 16) + '...' : d.label);

    // Simulation tick
    this.simulation.on('tick', () => {
      link
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y);

      linkLabel
        .attr('x', d => (d.source.x + d.target.x) / 2)
        .attr('y', d => (d.source.y + d.target.y) / 2 - 5);

      node.attr('transform', d => `translate(${d.x},${d.y})`);
    });

    // Legend
    this._renderLegend(g, nodes);
  },

  _nodeRadius(d) {
    const layerSize = { l0: 18, l1: 14, l2: 12, l3: 10, l4: 8, entity: 12, concept: 10 };
    return layerSize[d.layer] || layerSize[d.type] || 10;
  },

  _nodeColor(d) {
    return this.layerColors[d.layer] || this.layerColors[d.type] || this.layerColors.default;
  },

  _onNodeClick(event, d) {
    // Show node detail tooltip
    let tooltip = document.getElementById('kg-tooltip');
    if (!tooltip) {
      tooltip = document.createElement('div');
      tooltip.id = 'kg-tooltip';
      tooltip.className = 'kg-tooltip';
      document.body.appendChild(tooltip);
    }

    const edges = [];
    if (this.simulation) {
      this.simulation.force('link').links().forEach(e => {
        if (e.source.id === d.id) edges.push(`-> ${e.label} -> ${e.target.label}`);
        if (e.target.id === d.id) edges.push(`${e.source.label} -> ${e.label} ->`);
      });
    }

    tooltip.innerHTML = `
      <div class="kg-tooltip-header">
        <span class="kg-tooltip-color" style="background:${this._nodeColor(d)};"></span>
        <strong>${this._esc(d.label)}</strong>
      </div>
      <div class="kg-tooltip-meta">
        <span>Layer: ${this._esc(d.layer || 'unknown')}</span>
        <span>Type: ${this._esc(d.type || 'unknown')}</span>
      </div>
      ${edges.length > 0 ? `
        <div class="kg-tooltip-edges">
          <strong>Relations:</strong>
          ${edges.slice(0, 8).map(e => `<div class="kg-tooltip-edge">${this._esc(e)}</div>`).join('')}
          ${edges.length > 8 ? `<div class="kg-tooltip-more">+${edges.length - 8} more</div>` : ''}
        </div>
      ` : ''}
    `;

    tooltip.style.left = `${event.pageX + 12}px`;
    tooltip.style.top = `${event.pageY + 12}px`;
    tooltip.style.display = 'block';

    // Hide on click outside
    const hide = (e) => {
      if (!tooltip.contains(e.target)) {
        tooltip.style.display = 'none';
        document.removeEventListener('click', hide);
      }
    };
    setTimeout(() => document.addEventListener('click', hide), 10);
  },

  _renderLegend(g, nodes) {
    const layers = [...new Set(nodes.map(n => n.layer || 'default'))];
    const legendX = 16;
    const legendY = 16;

    const legend = g.append('g')
      .attr('class', 'kg-legend')
      .attr('transform', `translate(${legendX}, ${legendY})`);

    legend.append('rect')
      .attr('width', 120)
      .attr('height', layers.length * 22 + 12)
      .attr('fill', 'var(--bg, #1a1a2e)')
      .attr('stroke', 'var(--border, #2a2a4e)')
      .attr('rx', 4)
      .attr('opacity', 0.9);

    layers.forEach((layer, i) => {
      legend.append('circle')
        .attr('cx', 16)
        .attr('cy', 16 + i * 22)
        .attr('r', 6)
        .attr('fill', this.layerColors[layer] || this.layerColors.default);

      legend.append('text')
        .attr('x', 28)
        .attr('y', 20 + i * 22)
        .attr('fill', 'var(--text-muted, #8888aa)')
        .attr('font-size', '11px')
        .text(layer);
    });
  },

  /**
   * Destroy the current graph and clean up.
   */
  destroy() {
    if (this.simulation) {
      this.simulation.stop();
      this.simulation = null;
    }
    if (this.svg) {
      this.svg = null;
    }
  },

  _esc(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }
};

// Export
window.KnowledgeGraph = KnowledgeGraph;
