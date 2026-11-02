// Messages between the extension and the lineage graph webview (task 3.1, design section 9).
// Both sides validate every message against these zod schemas. The webview never talks to the
// language server: it asks the extension, which forwards lineage/graph, lineage/impact and
// lineage/node and validates the results against the shared protocol schema. Files are opened
// only by the extension, through an openFile message. Kept free of the vscode module, so the
// webview bundle can import it.

import * as z from 'zod';

import {
  Count, Direction, GraphParams, GraphResult, Id, ImpactParams, ImpactResult, Location, Name, NodeParams, NodeResult, Text,
} from './protocol';

export const Granularity = z.enum(['model', 'column']);
export type Granularity = z.infer<typeof Granularity>;

/** What the graph is centred on. */
export const Focus = z.strictObject({ id: Id, granularity: Granularity });
export type Focus = z.infer<typeof Focus>;

/** The view options the user can change, kept between sessions. */
export const ViewSettings = z.strictObject({
  showMiniMap: z.boolean(),
  showControls: z.boolean(),
  showLegend: z.boolean(),
  showInfoPanel: z.boolean(),
  animateEdges: z.boolean(),
  layout: z.enum(['LR', 'TB']),
  nodeSpacing: z.int().min(20).max(300),
  levelSpacing: z.int().min(50).max(600),
});
export type ViewSettings = z.infer<typeof ViewSettings>;

export const DEFAULT_SETTINGS: ViewSettings = {
  showMiniMap: true,
  showControls: true,
  showLegend: true,
  showInfoPanel: true,
  animateEdges: false,
  layout: 'LR',
  nodeSpacing: 40,
  levelSpacing: 120,
};

/** What the webview keeps in VS Code's webview state. Unless a panel retains its context, VS
 * Code destroys the page when it is hidden and builds it again when it is shown. */
export const ViewState = z.strictObject({
  focus: Focus.nullable(),
  depth: z.int().min(1).max(10),
  direction: Direction,
});
export type ViewState = z.infer<typeof ViewState>;

const RequestId = z.int().min(1);

/** From the webview to the extension. */
export const WebviewMessage = z.discriminatedUnion('type', [
  z.strictObject({ type: z.literal('ready') }),
  z.strictObject({ type: z.literal('graph'), id: RequestId, params: GraphParams }),
  z.strictObject({ type: z.literal('impact'), id: RequestId, params: ImpactParams }),
  z.strictObject({ type: z.literal('node'), id: RequestId, params: NodeParams }),
  // preserveFocus: a preview of the file, keeping the keyboard in the graph (a click)
  z.strictObject({ type: z.literal('openFile'), location: Location, preserveFocus: z.boolean() }),
  z.strictObject({ type: z.literal('saveSettings'), settings: ViewSettings }),
]);
export type WebviewMessage = z.infer<typeof WebviewMessage>;

/** From the extension to the webview. */
export const HostMessage = z.discriminatedUnion('type', [
  z.strictObject({
    type: z.literal('init'),
    project: z.strictObject({ name: Name, root: Text }),
    focus: Focus.nullable(),
    settings: ViewSettings,
    generation: Count,
  }),
  z.strictObject({ type: z.literal('focus'), focus: Focus }),
  // The project was re-indexed: the webview reloads what it shows
  z.strictObject({ type: z.literal('indexUpdated'), generation: Count, ready: z.boolean() }),
  z.strictObject({ type: z.literal('graphResult'), id: RequestId, result: GraphResult }),
  z.strictObject({ type: z.literal('impactResult'), id: RequestId, result: ImpactResult }),
  z.strictObject({ type: z.literal('nodeResult'), id: RequestId, result: NodeResult }),
  z.strictObject({ type: z.literal('error'), id: RequestId, message: Text, code: z.int().nullable() }),
]);
export type HostMessage = z.infer<typeof HostMessage>;
