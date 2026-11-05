// The review page's entry point: connect the transport to VS Code and render the view.

// First, before any module builds a zod schema
import '../jitless';

import { createRoot } from 'react-dom/client';

import type { VsCodeApi } from '../transport';
import ReviewApp from './ReviewApp';
import { ReviewTransport } from './transport';

declare function acquireVsCodeApi(): VsCodeApi;

const transport = new ReviewTransport(acquireVsCodeApi());
createRoot(document.getElementById('root')!).render(<ReviewApp transport={transport} />);
