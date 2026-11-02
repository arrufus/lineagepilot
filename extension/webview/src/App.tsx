// The lineage graph view (Phase 3): the toolbar, the graph, and a side panel for details,
// impact or settings. All data comes from the extension through the transport.

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Columns3, X } from 'lucide-react';

import type { GraphNode, ImpactResult, NodeKind, NodeResult } from '../../src/protocol';
import { DEFAULT_SETTINGS, type Focus, type Granularity, type ViewSettings } from '../../src/webviewMessages';
import DetailPanel from './components/DetailPanel';
import ImpactPanel from './components/ImpactPanel';
import LineageGraph from './components/LineageGraph';
import SettingsPanel from './components/SettingsPanel';
import Toolbar, { type Direction } from './components/Toolbar';
import { fromResult, kindsIn, merge, neighbours, type ViewGraph } from './graph/model';
import { KIND_LABELS, plural } from './labels';
import type { Transport } from './transport';

export const DEFAULT_DEPTH = 2;

type Side = 'details' | 'impact' | 'settings' | null;

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function displayName(node: GraphNode): string {
  if (node.kind !== 'column') {
    return node.name;
  }
  return `${(node.parent ?? node.id.split('#')[0]).split('.').pop()}.${node.name}`;
}

export default function App({ transport }: { transport: Transport }) {
  // What was shown before VS Code last hid this view, which destroys the page
  const [restored] = useState(() => transport.restoreState());
  const [project, setProject] = useState<{ name: string; root: string } | null>(null);
  const [focus, setFocus] = useState<Focus | null>(restored?.focus ?? null);
  const [settings, setSettings] = useState<ViewSettings>(DEFAULT_SETTINGS);
  const [depth, setDepth] = useState(restored?.depth ?? DEFAULT_DEPTH);
  const [direction, setDirection] = useState<Direction>(restored?.direction ?? 'both');
  const [hiddenKinds, setHiddenKinds] = useState<Set<NodeKind>>(new Set());
  const [search, setSearch] = useState('');
  const [view, setView] = useState<ViewGraph | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<GraphNode | null>(null);
  const [detail, setDetail] = useState<NodeResult | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [side, setSide] = useState<Side>(null);
  const [impact, setImpact] = useState<{ name: string; result: ImpactResult | null; error: string | null } | null>(null);
  const [picker, setPicker] = useState<NodeResult | null>(null);
  const [reload, setReload] = useState(0);
  const [announcement, setAnnouncement] = useState('');
  const graphRequest = useRef(0);
  const detailRequest = useRef(0);

  useEffect(() => {
    const stop = transport.onNotice((notice) => {
      if (notice.type === 'init') {
        setProject(notice.project);
        setSettings(notice.settings);
        // A focus asked for while the view was hidden wins over the one it kept
        if (notice.focus) {
          setFocus(notice.focus);
        }
      } else if (notice.type === 'focus') {
        setFocus(notice.focus);
        setSelected(null);
        setSide((current) => (current === 'settings' ? current : null));
      } else if (notice.ready) {
        setReload((n) => n + 1);
      }
    });
    transport.ready();
    return stop;
  }, [transport]);

  useEffect(() => {
    transport.saveState({ focus, depth, direction });
  }, [transport, focus, depth, direction]);

  // Load the graph around the focus whenever what is asked for changes
  useEffect(() => {
    if (!focus) {
      return;
    }
    const request = ++graphRequest.current;
    setLoading(true);
    setError(null);
    transport.graph({ root: focus.id, direction, depth, granularity: focus.granularity })
      .then((result) => {
        if (request === graphRequest.current) {
          setView(fromResult(result, depth));
          setAnnouncement(`Showing ${plural(result.nodes.length, 'node')} around ${result.nodes.find((n) => n.side === 'root')?.name ?? focus.id}`);
        }
      })
      .catch((failure) => {
        if (request === graphRequest.current) {
          setError(messageOf(failure));
          setView(null);
        }
      })
      .finally(() => {
        if (request === graphRequest.current) {
          setLoading(false);
        }
      });
  }, [transport, focus, depth, direction, reload]);

  // A click or Enter on a node: its details, and a preview of its file beside the graph
  const select = useCallback((node: GraphNode | null) => {
    setSelected(node);
    if (!node) {
      setSide((current) => (current === 'details' ? null : current));
      return;
    }
    if (node.location) {
      transport.openFile(node.location, true);
    }
    setSide('details');
    setDetail(null);
    setDetailError(null);
    const request = ++detailRequest.current;
    transport.node({ id: node.id })
      .then((result) => { if (request === detailRequest.current) setDetail(result); })
      .catch((failure) => { if (request === detailRequest.current) setDetailError(messageOf(failure)); });
  }, [transport]);

  const open = useCallback((node: GraphNode) => {
    if (node.location) {
      transport.openFile(node.location);
    }
  }, [transport]);

  const expand = useCallback((node: GraphNode) => {
    if (!focus || node.side === 'root') {
      return;
    }
    transport.graph({ root: node.id, direction: node.side, depth: 1, granularity: focus.granularity })
      .then((result) => {
        setView((current) => (current ? merge(current, node.id, result) : current));
        setAnnouncement(`Added ${plural(Math.max(0, result.nodes.length - 1), 'node')} ${node.side} of ${node.name}`);
      })
      .catch((failure) => setError(messageOf(failure)));
  }, [transport, focus]);

  const showImpact = useCallback((node: GraphNode) => {
    setSide('impact');
    setImpact({ name: node.name, result: null, error: null });
    transport.impact({ targets: [node.id] })
      .then((result) => setImpact({ name: node.name, result, error: null }))
      .catch((failure) => setImpact({ name: node.name, result: null, error: messageOf(failure) }));
  }, [transport]);

  const focusOn = useCallback((id: string, granularity: Granularity) => {
    setFocus({ id, granularity });
    setSelected(null);
    setSide(null);
    setPicker(null);
  }, []);

  const changeMode = useCallback((mode: Granularity) => {
    if (!focus || mode === focus.granularity) {
      return;
    }
    if (mode === 'model') {
      const root = view?.nodes.get(view.rootId);
      focusOn(root?.parent ?? focus.id.split('#')[0], 'model');
      return;
    }
    // Columns need a column to trace: offer the focused model's
    transport.node({ id: selected?.kind !== 'column' && selected ? selected.id : focus.id })
      .then((result) => setPicker(result))
      .catch((failure) => setError(messageOf(failure)));
  }, [transport, focus, view, selected, focusOn]);

  const changeSettings = useCallback((next: ViewSettings) => {
    setSettings(next);
    transport.saveSettings(next);
  }, [transport]);

  const filters = useMemo(() => ({ hiddenKinds, search }), [hiddenKinds, search]);
  const names = useMemo(
    () => new Map([...(view?.nodes.values() ?? [])].map((node) => [node.id, displayName(node)])),
    [view],
  );
  const edgesOf = selected && view ? neighbours(view, selected.id) : { upstream: [], downstream: [] };

  return (
    <div className="lp-app">
      <Toolbar
        mode={focus?.granularity ?? 'model'}
        canUseColumns={!!focus}
        depth={depth}
        direction={direction}
        layout={settings.layout}
        kinds={view ? kindsIn(view) : []}
        hiddenKinds={hiddenKinds}
        search={search}
        onMode={changeMode}
        onDepth={setDepth}
        onDirection={setDirection}
        onLayout={(layout) => changeSettings({ ...settings, layout })}
        onToggleKind={(kind) => setHiddenKinds((current) => {
          const next = new Set(current);
          if (!next.delete(kind)) next.add(kind);
          return next;
        })}
        onSearch={setSearch}
        onRefresh={() => setReload((n) => n + 1)}
        onRecentre={() => focus && focusOn(focus.id, focus.granularity)}
        onSettings={() => setSide((current) => (current === 'settings' ? null : 'settings'))}
      />
      <div className="lp-main">
        {!focus && (
          <div className="lp-empty">
            <p>{project ? `Lineage for ${project.name}` : 'Lineage'}</p>
            <p className="lp-muted">Run <strong>LineagePilot: Show Lineage Graph</strong> from a model, or from the Lineage view.</p>
          </div>
        )}
        {error && <div className="lp-error lp-banner" role="alert">{error}</div>}
        {loading && !view && <div className="lp-empty" role="status">Loading lineage…</div>}
        {view && (
          <LineageGraph view={view} filters={filters} settings={settings} selectedId={selected?.id ?? null}
            onSelect={select} onOpen={open} onExpand={expand} />
        )}
        {picker && (
          <div className="lp-picker" role="dialog" aria-label={`Choose a column of ${picker.node.name}`}>
            <header>
              <h2><Columns3 size={16} aria-hidden="true" /> Trace a column of {picker.node.name}</h2>
              <button type="button" className="lp-icon-button" aria-label="Cancel" onClick={() => setPicker(null)}>
                <X size={16} aria-hidden="true" />
              </button>
            </header>
            {picker.columns.length === 0
              ? <p className="lp-muted">This {KIND_LABELS[picker.node.kind]} has no known columns. Run dbt docs generate to read them from the warehouse.</p>
              : (
                <ul className="lp-list">
                  {picker.columns.map((c) => (
                    <li key={c.id}>
                      <button type="button" className="lp-link" onClick={() => focusOn(c.id, 'column')}>{c.name}</button>
                      <span className="lp-muted">{c.dataType}</span>
                    </li>
                  ))}
                </ul>
              )}
          </div>
        )}
        {side === 'details' && selected && (
          <DetailPanel node={selected} detail={detail} error={detailError} upstream={edgesOf.upstream}
            downstream={edgesOf.downstream} names={names} unknowns={view?.unknowns ?? []}
            onClose={() => select(null)} onOpen={open} onFocus={focusOn} onImpact={showImpact} />
        )}
        {side === 'impact' && impact && (
          <ImpactPanel name={impact.name} result={impact.result} error={impact.error}
            onClose={() => setSide(null)} onOpen={(location) => transport.openFile(location)}
            onFocus={(id) => focusOn(id, 'model')} />
        )}
        {side === 'settings' && (
          <SettingsPanel settings={settings} onChange={changeSettings} onClose={() => setSide(null)} />
        )}
      </div>
      <div className="lp-visually-hidden" aria-live="polite">{announcement}</div>
    </div>
  );
}
