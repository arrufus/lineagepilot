// A stand-in for VS Code around the production lineage view, to measure a large graph in a real
// browser (Phase 3 exit: a 500-node graph stays interactive). From extension/:
//
//   npm run package                      (or: node esbuild.mjs --production)
//   npx esbuild --servedir=. --serve=127.0.0.1:8123
//   open http://127.0.0.1:8123/webview/perf/?nodes=500
//
// It answers the view's requests with a layered graph of ?nodes= models, then drives it: the
// first draw, zooming in and out for two seconds, selecting a node, highlighting by search, and
// an arrow key. Results are shown on the page and kept in window.lpPerf.
(() => {
  'use strict';
  const params = new URLSearchParams(location.search);
  const COUNT = Number(params.get('nodes') || 500);
  const WIDTH = Number(params.get('width') || 25);
  const ROOT = 'model.big.m0';

  const perf = {
    nodes: COUNT, cspViolations: [], longTasks: [], opened: [], requests: [], results: {}, done: false,
  };
  window.lpPerf = perf;
  document.addEventListener('securitypolicyviolation', (event) => {
    perf.cspViolations.push(`${event.violatedDirective} ${event.blockedURI || ''}`.trim());
  });
  try {
    new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) perf.longTasks.push(Math.round(entry.duration));
    }).observe({ type: 'longtask', buffered: true });
  } catch {
    // Long task timing is Chromium only
  }

  const location0 = (i) => ({
    uri: `file:///project/models/m${i}.sql`, range: { start: { line: 0, character: 0 }, end: { line: 5, character: 0 } },
  });
  const nodes = Array.from({ length: COUNT }, (_, i) => ({
    id: `model.big.m${i}`, kind: i % 40 === 7 ? 'seed' : 'model', name: `m${i}`, side: i === 0 ? 'root' : 'downstream',
    depth: Math.floor(i / WIDTH), parent: null, location: location0(i), layer: i < WIDTH ? 'staging' : 'marts',
    materialization: 'table', owner: null, description: null, columnLineage: 'resolved', columnLineageReason: null, pii: i % 17 === 0,
  }));
  const edges = [];
  for (let i = WIDTH; i < COUNT; i += 1) {
    const before = (Math.floor(i / WIDTH) - 1) * WIDTH;
    for (const j of new Set([before + (i % WIDTH), before + ((i * 7) % WIDTH)])) {
      edges.push({
        kind: 'depends_on', upstream: `model.big.m${j}`, downstream: `model.big.m${i}`, relation: null,
        confidence: i % 5 === 0 ? 'inferred' : 'declared', origins: ['manifest'], transformation: null,
      });
    }
  }
  const graph = { root: ROOT, direction: 'both', granularity: 'model', nodes, edges, truncated: false, unknowns: [] };

  const send = (message) => window.postMessage(message, '*');
  window.acquireVsCodeApi = () => ({
    postMessage(message) {
      perf.requests.push(message.type);
      if (message.type === 'ready') {
        send({
          type: 'init', project: { name: 'perf', root: '/project' }, focus: { id: ROOT, granularity: 'model' }, generation: 1,
          settings: { showMiniMap: true, showControls: true, showLegend: true, showInfoPanel: true, animateEdges: false, layout: 'LR', nodeSpacing: 40, levelSpacing: 120 },
        });
      } else if (message.type === 'graph') {
        perf.graphSentAt = performance.now();
        send({ type: 'graphResult', id: message.id, result: graph });
      } else if (message.type === 'node') {
        const node = nodes.find((n) => n.id === message.params.id) || nodes[0];
        send({ type: 'nodeResult', id: message.id, result: { node, columns: [], upstream: 2, downstream: 2 } });
      } else if (message.type === 'impact') {
        send({ type: 'impactResult', id: message.id, result: { targets: message.params.targets, truncated: false, counts: {}, nodes: [], unknowns: [], columnLineage: 'done' } });
      } else if (message.type === 'openFile') {
        perf.opened.push(message.location);
      }
    },
    getState: () => undefined,
    setState: () => {},
  });

  const frame = () => new Promise((resolve) => requestAnimationFrame(() => resolve(performance.now())));
  const settle = async () => { await frame(); return frame(); };
  async function until(test, limitMs = 10000) {
    const start = performance.now();
    while (!test()) {
      if (performance.now() - start > limitMs) throw new Error('timed out');
      await frame();
    }
    return frame();
  }
  const ms = (value) => Math.round(value * 10) / 10;
  const percentile = (values, p) => [...values].sort((a, b) => a - b)[Math.min(values.length - 1, Math.floor(values.length * p))];

  function show() {
    const r = perf.results;
    const lines = [
      `${COUNT} nodes, ${edges.length} edges`,
      `first draw          ${r.firstDrawMs} ms (${r.drawnNodes} nodes on screen)`,
      `zoom, 2 s           ${r.zoom ? `${r.zoom.fps} fps, p95 frame ${r.zoom.p95FrameMs} ms, worst ${r.zoom.worstFrameMs} ms` : '…'}`,
      `select a node       ${r.selectMs ?? '…'} ms to the details panel`,
      `search highlight    ${r.searchMs ?? '…'} ms`,
      `arrow key           ${r.arrowMs ?? '…'} ms to the next node${r.arrowRevealed ? ', brought into view' : ''}`,
      `long tasks > 50 ms  ${perf.longTasks.length ? perf.longTasks.join(', ') : 'none'}`,
      `CSP violations      ${perf.cspViolations.length ? perf.cspViolations.join('; ') : 'none'}`,
    ];
    document.getElementById('lp-perf').textContent = lines.join('\n');
  }

  async function run() {
    await until(() => perf.graphSentAt !== undefined && document.querySelector('.react-flow__node[style*="visible"]'), 30000);
    await settle();
    perf.results.firstDrawMs = ms(performance.now() - perf.graphSentAt);
    perf.results.drawnNodes = document.querySelectorAll('.react-flow__node').length;
    perf.longTasks.length = 0;
    show();

    // Zoom out and back in around the middle, every frame for two seconds
    const pane = document.querySelector('.react-flow__pane');
    const box = pane.getBoundingClientRect();
    const frames = [];
    let last = await frame();
    const start = last;
    while (last - start < 2000) {
      const out = (last - start) % 1000 < 500;
      pane.dispatchEvent(new WheelEvent('wheel', {
        deltaY: out ? 60 : -60, clientX: box.left + box.width / 2, clientY: box.top + box.height / 2, bubbles: true, cancelable: true,
      }));
      const now = await frame();
      frames.push(now - last);
      last = now;
    }
    perf.results.zoom = {
      fps: Math.round(frames.length / ((last - start) / 1000)), p95FrameMs: ms(percentile(frames, 0.95)), worstFrameMs: ms(Math.max(...frames)),
      nodesDrawnAtEnd: document.querySelectorAll('.react-flow__node').length,
    };
    show();
    await settle();

    // Select a node on screen: until its details panel shows
    const target = document.querySelector('.react-flow__node[data-id="model.big.m30"]') || document.querySelector('.react-flow__node');
    let t = performance.now();
    target.dispatchEvent(new MouseEvent('click', { bubbles: true, clientX: 10, clientY: 10 }));
    await until(() => document.querySelector('.lp-side'));
    perf.results.selectMs = ms(performance.now() - t);
    show();

    // Highlight by search: until the other nodes dim
    const search = document.querySelector('input[type="search"]');
    const setValue = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
    t = performance.now();
    setValue.call(search, 'm1');
    search.dispatchEvent(new Event('input', { bubbles: true }));
    await until(() => document.querySelector('.lp-node-dimmed'));
    perf.results.searchMs = ms(performance.now() - t);
    show();

    // An arrow key on the selected node, which is on screen (the root may not be, once zoomed in):
    // until focus moves along an edge. The view brings a node that is off screen into view first.
    const fromId = target.getAttribute('data-id');
    const from = document.querySelector(`.react-flow__node[data-id="${fromId}"]`);
    const key = edges.some((edge) => edge.upstream === fromId) ? 'ArrowRight' : 'ArrowLeft';
    const drawnBefore = new Set([...document.querySelectorAll('.react-flow__node')].map((n) => n.getAttribute('data-id')));
    from.focus();
    t = performance.now();
    from.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true }));
    await until(() => document.activeElement !== from && document.activeElement?.classList.contains('react-flow__node'));
    perf.results.arrowMs = ms(performance.now() - t);
    perf.results.arrowKey = key;
    perf.results.arrowFrom = fromId;
    perf.results.arrowTo = document.activeElement.getAttribute('data-id');
    perf.results.arrowRevealed = !drawnBefore.has(perf.results.arrowTo);
    perf.done = true;
    show();
  }

  window.addEventListener('load', () => {
    run().catch((error) => {
      perf.error = String(error);
      document.getElementById('lp-perf').textContent = `failed: ${error}`;
    });
  });
})();
