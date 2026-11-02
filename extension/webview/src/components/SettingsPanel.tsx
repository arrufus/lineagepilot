// View settings (task 3.2), ported from the dashboard's SettingsPanel. The theme choice is gone:
// the graph follows the VS Code theme (task 3.4). Settings are kept by the extension.

import { RotateCcw, Settings, X } from 'lucide-react';

import { DEFAULT_SETTINGS, type ViewSettings } from '../../../src/webviewMessages';

export interface SettingsPanelProps {
  settings: ViewSettings;
  onChange(settings: ViewSettings): void;
  onClose(): void;
}

type Toggle = 'showMiniMap' | 'showControls' | 'showLegend' | 'showInfoPanel' | 'animateEdges';

const TOGGLES: [Toggle, string, string][] = [
  ['showMiniMap', 'Mini map', 'An overview of the whole graph'],
  ['showControls', 'Zoom controls', 'Buttons to zoom and fit the view'],
  ['showLegend', 'Legend', 'What the edge styles mean'],
  ['showInfoPanel', 'Summary', 'Node and edge counts, and what is not shown'],
  ['animateEdges', 'Animate edges', 'Off for large graphs and when reduced motion is on'],
];

export default function SettingsPanel({ settings, onChange, onClose }: SettingsPanelProps) {
  const set = (patch: Partial<ViewSettings>) => onChange({ ...settings, ...patch });
  return (
    <aside className="lp-side" aria-label="View settings">
      <header>
        <h2><Settings size={16} aria-hidden="true" /> View settings</h2>
        <button type="button" className="lp-icon-button" onClick={() => onClose()} aria-label="Close settings" title="Close">
          <X size={16} aria-hidden="true" />
        </button>
      </header>
      <section aria-label="Display">
        <h3>Display</h3>
        {TOGGLES.map(([key, label, description]) => (
          <label key={key} className="lp-toggle">
            <input type="checkbox" role="switch" checked={settings[key]} onChange={() => set({ [key]: !settings[key] })} />
            <span>
              <span>{label}</span>
              <span className="lp-muted">{description}</span>
            </span>
          </label>
        ))}
      </section>
      <section aria-label="Layout">
        <h3>Layout</h3>
        <label className="lp-slider">
          <span>Node spacing <span className="lp-muted">{settings.nodeSpacing} px</span></span>
          <input type="range" min={20} max={300} step={10} value={settings.nodeSpacing}
            onChange={(event) => set({ nodeSpacing: Number(event.target.value) })} />
        </label>
        <label className="lp-slider">
          <span>Level spacing <span className="lp-muted">{settings.levelSpacing} px</span></span>
          <input type="range" min={50} max={600} step={10} value={settings.levelSpacing}
            onChange={(event) => set({ levelSpacing: Number(event.target.value) })} />
        </label>
      </section>
      <div className="lp-actions">
        <button type="button" onClick={() => onChange(DEFAULT_SETTINGS)}>
          <RotateCcw size={14} aria-hidden="true" /> Reset to defaults
        </button>
      </div>
    </aside>
  );
}
