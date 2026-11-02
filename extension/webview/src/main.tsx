// The webview's entry point: connect the transport to VS Code and render the view.

import { createRoot } from 'react-dom/client';
import { config } from 'zod';

import App from './App';
import { Transport, type VsCodeApi } from './transport';

declare function acquireVsCodeApi(): VsCodeApi;

// The CSP allows no eval: without this, zod probes for it with `new Function` and the webview
// reports a CSP violation, even though zod catches the error
config({ jitless: true });

const transport = new Transport(acquireVsCodeApi());
createRoot(document.getElementById('root')!).render(<App transport={transport} />);
