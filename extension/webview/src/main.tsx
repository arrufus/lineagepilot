// The webview's entry point: connect the transport to VS Code and render the view.

// First, before any module builds a zod schema
import './jitless';

import { createRoot } from 'react-dom/client';

import App from './App';
import { Transport, type VsCodeApi } from './transport';

declare function acquireVsCodeApi(): VsCodeApi;

const transport = new Transport(acquireVsCodeApi());
createRoot(document.getElementById('root')!).render(<App transport={transport} />);
