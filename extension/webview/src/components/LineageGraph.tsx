// The lineage graph (tasks 3.2-3.4), ported from the dashboard's LineageGraph and
// ColumnLineageGraph into one component for both modes. Data comes from the extension through
// the transport, the layout from dagre, colours from the VS Code theme. Clicking a node, or
// Enter, selects it and previews its file; double-click, or O, opens the file.

import { type KeyboardEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Background, Controls, type Edge, MarkerType, MiniMap, Panel, ReactFlow, type ReactFlowInstance,
} from '@xyflow/react';

import type { GraphNode } from '../../../src/protocol';
import type { ViewSettings } from '../../../src/webviewMessages';
import { edgeLabel, nodeLabel, plural } from '../labels';
import { layout } from '../graph/layout';
import { canExpand, type Filters, matches, neighbours, type ViewGraph, visible } from '../graph/model';
import ColumnNode from './ColumnNode';
import ModelNode, { handleNodeKey, type LineageFlowNode, type NodeActions } from './ModelNode';

const NODE_TYPES = { model: ModelNode, column: ColumnNode };
// Above this many nodes only what is on screen is rendered, and edges are never animated
const LARGE_GRAPH = 200;

// What screen readers are told about the keys. React Flow's defaults describe moving and
// deleting nodes, which this view does not do. It shows the "keyboardDisabled" text when
// keyboard support is on, so both are set.
const NODE_KEYS = 'Arrow keys follow the edges. Enter shows details and previews the file, O opens it, + shows more.';
const ARIA_LABELS = {
  'node.a11yDescription.default': NODE_KEYS,
  'node.a11yDescription.keyboardDisabled': NODE_KEYS,
  'edge.a11yDescription.default': 'Solid edges are declared, dashed inferred from SQL, dotted heuristic.',
};

export interface LineageGraphProps {
  view: ViewGraph;
  filters: Filters;
  settings: ViewSettings;
  selectedId: string | null;
  onSelect(node: GraphNode | null): void;
  onOpen(node: GraphNode): void;
  onExpand(node: GraphNode): void;
}

/** 'dark' for VS Code's dark and high-contrast themes, which set a class on <body>. */
export function useColorMode(): 'light' | 'dark' {
  const read = () => (document.body.classList.contains('vscode-dark')
    || document.body.classList.contains('vscode-high-contrast') ? 'dark' : 'light');
  const [mode, setMode] = useState<'light' | 'dark'>(read);
  useEffect(() => {
    const observer = new MutationObserver(() => setMode(read()));
    observer.observe(document.body, { attributes: true, attributeFilter: ['class'] });
    return () => observer.disconnect();
  }, []);
  return mode;
}

function prefersReducedMotion(): boolean {
  return typeof window.matchMedia === 'function' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

/** Move keyboard focus to a node's element in the graph. */
export function focusNode(container: HTMLElement | null, id: string): void {
  const escaped = id.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
  container?.querySelector<HTMLElement>(`.react-flow__node[data-id="${escaped}"]`)?.focus();
}

export default function LineageGraph({ view, filters, settings, selectedId, onSelect, onOpen, onExpand }: LineageGraphProps) {
  const colorMode = useColorMode();
  const instance = useRef<ReactFlowInstance<LineageFlowNode, Edge> | null>(null);
  const container = useRef<HTMLDivElement>(null);
  const horizontal = settings.layout === 'LR';
  const shown = useMemo(() => visible(view, filters.hiddenKinds), [view, filters.hiddenKinds]);
  const positions = useMemo(
    () => layout(shown.nodes, shown.edges, {
      direction: settings.layout, nodeSpacing: settings.nodeSpacing, levelSpacing: settings.levelSpacing,
    }),
    [shown, settings.layout, settings.nodeSpacing, settings.levelSpacing],
  );

  const onNavigate = useCallback((node: GraphNode, key: string) => {
    const forward = horizontal ? 'ArrowRight' : 'ArrowDown';
    const backward = horizontal ? 'ArrowLeft' : 'ArrowUp';
    const { upstream, downstream } = neighbours(view, node.id);
    let candidates: string[];
    if (key === forward) {
      candidates = downstream.map((edge) => edge.downstream);
    } else if (key === backward) {
      candidates = upstream.map((edge) => edge.upstream);
    } else {
      // Across the flow: the nearest node in the same layer, in that direction
      const here = positions.get(node.id);
      const across = horizontal ? 'y' : 'x';
      const sign = key === 'ArrowDown' || key === 'ArrowRight' ? 1 : -1;
      candidates = shown.nodes
        .filter((other) => other.id !== node.id && here && positions.get(other.id)
          && Math.abs(positions.get(other.id)![horizontal ? 'x' : 'y'] - here[horizontal ? 'x' : 'y']) < 1
          && sign * (positions.get(other.id)![across] - here[across]) > 0)
        .sort((a, b) => sign * (positions.get(a.id)![across] - positions.get(b.id)![across]))
        .map((other) => other.id);
    }
    const target = candidates.find((id) => positions.has(id));
    if (target) {
      focusNode(container.current, target);
    }
  }, [view, positions, shown, horizontal]);

  const actions: NodeActions = useMemo(
    () => ({ onSelect, onOpen, onExpand, onNavigate }), [onSelect, onOpen, onExpand, onNavigate]);

  const nodes: LineageFlowNode[] = useMemo(() => shown.nodes.map((node) => {
    const { upstream, downstream } = neighbours(view, node.id);
    return {
      id: node.id,
      type: node.kind === 'column' ? 'column' : 'model',
      position: positions.get(node.id) ?? { x: 0, y: 0 },
      selected: node.id === selectedId,
      ariaLabel: nodeLabel(node, upstream.length, downstream.length),
      data: {
        node,
        incoming: upstream[0],
        canExpand: canExpand(view, node),
        dimmed: !matches(node, filters.search),
        horizontal,
        actions,
      },
    };
  }), [shown, view, positions, selectedId, filters.search, horizontal, actions]);

  const animate = settings.animateEdges && shown.edges.length <= LARGE_GRAPH && !prefersReducedMotion();
  const edges: Edge[] = useMemo(() => shown.edges.map((edge) => ({
    id: `${edge.kind}|${edge.upstream}|${edge.downstream}`,
    source: edge.upstream,
    target: edge.downstream,
    type: 'smoothstep',
    animated: animate,
    className: `lp-edge lp-edge-${edge.confidence}`,
    label: edge.kind === 'derives' ? edgeLabel(edge) : undefined,
    ariaLabel: `${edge.upstream} to ${edge.downstream}: ${edgeLabel(edge) || edge.kind}`,
    markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 },
    focusable: false,
  })), [shown, animate]);

  // Fit the view when what is shown changes shape, not on every selection. React Flow waits
  // until the new nodes are measured; the first fit is the fitView prop's
  useEffect(() => {
    void instance.current?.fitView({ padding: 0.15, duration: 0 });
  }, [view.rootId, shown.nodes.length, settings.layout]);

  // Keyboard focus is on React Flow's wrapper around each node, so keys are handled here, for
  // whichever node has it; they would never reach a handler inside the node
  const onKeyDown = (event: KeyboardEvent) => {
    const id = (event.target as Element).closest?.('.react-flow__node')?.getAttribute('data-id');
    const node = id ? view.nodes.get(id) : undefined;
    if (node) {
      handleNodeKey(event, node, actions, canExpand(view, node));
    }
  };

  const large = shown.nodes.length > LARGE_GRAPH;
  return (
    <div className="lp-graph" data-testid="lineage-graph" ref={container} onKeyDown={onKeyDown}>
      <ReactFlow<LineageFlowNode, Edge>
        nodes={nodes}
        edges={edges}
        nodeTypes={NODE_TYPES}
        colorMode={colorMode}
        onInit={(flow) => { instance.current = flow; }}
        onNodeClick={(_event, node) => onSelect(node.data.node)}
        onNodeDoubleClick={(_event, node) => onOpen(node.data.node)}
        onPaneClick={() => onSelect(null)}
        elementsSelectable={false}
        nodesDraggable={false}
        nodesConnectable={false}
        edgesFocusable={false}
        deleteKeyCode={null}
        onlyRenderVisibleElements={large}
        minZoom={0.05}
        maxZoom={2}
        fitView
        // Double-click opens the file, so it must not zoom (d3-zoom would take it first)
        zoomOnDoubleClick={false}
        proOptions={{ hideAttribution: true }}
        ariaLabelConfig={ARIA_LABELS}
      >
        <Background gap={16} size={1} />
        {settings.showControls && <Controls showInteractive={false} />}
        {settings.showMiniMap && <MiniMap pannable zoomable ariaLabel="Overview of the lineage graph" />}
        {settings.showInfoPanel && (
          <Panel position="top-left" className="lp-panel-info">
            <div><strong>{plural(shown.nodes.length, 'node')}</strong>, {plural(shown.edges.length, 'edge')}</div>
            {view.truncated && <div className="lp-muted">There is more beyond what is shown: use + or a larger depth</div>}
            {view.unknowns.length > 0 && <div className="lp-warn">{plural(view.unknowns.length, 'unknown')}: see the details panel</div>}
            {view.nodes.size > shown.nodes.length && <div className="lp-muted">{view.nodes.size - shown.nodes.length} hidden by filters</div>}
          </Panel>
        )}
        {settings.showLegend && (
          <Panel position="bottom-right" className="lp-panel-legend" aria-label="Legend">
            <div className="lp-legend-title">Edges</div>
            <div className="lp-legend-row"><span className="lp-legend-line lp-edge-declared" /> declared</div>
            <div className="lp-legend-row"><span className="lp-legend-line lp-edge-inferred" /> inferred</div>
            <div className="lp-legend-row"><span className="lp-legend-line lp-edge-heuristic" /> heuristic</div>
          </Panel>
        )}
      </ReactFlow>
    </div>
  );
}
