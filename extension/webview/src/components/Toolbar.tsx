// The graph's controls (task 3.3): model or column mode, depth, direction, layout, filters by
// node kind, and a search that highlights matching nodes.

import { Columns3, LocateFixed, Network, RefreshCw, Search, Settings } from 'lucide-react';

import type { NodeKind } from '../../../src/protocol';
import type { Granularity } from '../../../src/webviewMessages';
import { KIND_LABELS } from '../labels';

export type Direction = 'both' | 'upstream' | 'downstream';
export const MAX_DEPTH = 10;

export interface ToolbarProps {
  mode: Granularity;
  canUseColumns: boolean;
  depth: number;
  direction: Direction;
  layout: 'LR' | 'TB';
  kinds: NodeKind[];
  hiddenKinds: Set<NodeKind>;
  search: string;
  onMode(mode: Granularity): void;
  onDepth(depth: number): void;
  onDirection(direction: Direction): void;
  onLayout(layout: 'LR' | 'TB'): void;
  onToggleKind(kind: NodeKind): void;
  onSearch(search: string): void;
  onRefresh(): void;
  onRecentre(): void;
  onSettings(): void;
}

export default function Toolbar(props: ToolbarProps) {
  const { mode, depth, direction, kinds, hiddenKinds } = props;
  return (
    <div className="lp-toolbar" role="toolbar" aria-label="Lineage graph controls">
      <div className="lp-segmented" role="group" aria-label="Granularity">
        <button type="button" aria-pressed={mode === 'model'} onClick={() => props.onMode('model')}>
          <Network size={14} aria-hidden="true" /> Models
        </button>
        <button type="button" aria-pressed={mode === 'column'} disabled={!props.canUseColumns && mode !== 'column'}
          title={props.canUseColumns ? 'Trace one column' : 'Choose a model with columns first'}
          onClick={() => props.onMode('column')}>
          <Columns3 size={14} aria-hidden="true" /> Columns
        </button>
      </div>
      <label className="lp-field">
        Depth
        <select value={depth} onChange={(event) => props.onDepth(Number(event.target.value))}>
          {Array.from({ length: MAX_DEPTH }, (_, i) => i + 1).map((value) => (
            <option key={value} value={value}>{value}</option>
          ))}
        </select>
      </label>
      <label className="lp-field">
        Direction
        <select value={direction} onChange={(event) => props.onDirection(event.target.value as Direction)}>
          <option value="both">Both</option>
          <option value="upstream">Upstream</option>
          <option value="downstream">Downstream</option>
        </select>
      </label>
      <label className="lp-field">
        Layout
        <select value={props.layout} onChange={(event) => props.onLayout(event.target.value as 'LR' | 'TB')}>
          <option value="LR">Left to right</option>
          <option value="TB">Top to bottom</option>
        </select>
      </label>
      {kinds.length > 1 && (
        <fieldset className="lp-kinds">
          <legend>Show</legend>
          {kinds.map((kind) => (
            <label key={kind}>
              <input type="checkbox" checked={!hiddenKinds.has(kind)} onChange={() => props.onToggleKind(kind)} />
              {KIND_LABELS[kind]}
            </label>
          ))}
        </fieldset>
      )}
      <label className="lp-search">
        <Search size={14} aria-hidden="true" />
        <span className="lp-visually-hidden">Highlight nodes</span>
        <input type="search" placeholder="Highlight…" value={props.search}
          onChange={(event) => props.onSearch(event.target.value)} />
      </label>
      <span className="lp-spacer" />
      <button type="button" className="lp-icon-button" onClick={() => props.onRecentre()} aria-label="Back to the focus" title="Back to the focus">
        <LocateFixed size={15} aria-hidden="true" />
      </button>
      <button type="button" className="lp-icon-button" onClick={() => props.onRefresh()} aria-label="Reload" title="Reload">
        <RefreshCw size={15} aria-hidden="true" />
      </button>
      <button type="button" className="lp-icon-button" onClick={() => props.onSettings()} aria-label="View settings" title="View settings">
        <Settings size={15} aria-hidden="true" />
      </button>
    </div>
  );
}
