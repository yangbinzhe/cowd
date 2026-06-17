import type { Component } from 'vue';

export type NavId = 'chat' | 'runtime' | 'context' | 'memory' | 'skills' | 'agents' | 'tools' | 'gateway' | 'mfg' | 'audit' | 'settings';
export type CompanionTab = 'activity' | 'thinking' | 'workspace' | 'inspector';
export type Tone = 'neutral' | 'info' | 'success' | 'warn' | 'danger';

export interface NavItem {
  id: NavId;
  label: string;
  route: string;
  icon: Component;
  group: string;
}

export interface SessionSummary {
  id: string;
  title: string;
  model?: string;
  status?: string;
  updated_at?: number | string;
}

export interface ChatTurn {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  status?: 'streaming' | 'complete' | 'error';
  activity?: ActivityEvent[];
}

export interface ActivityEvent {
  id: string;
  kind: 'tool' | 'think' | 'runtime' | 'context' | 'approval' | 'error';
  title: string;
  detail?: string;
  status?: string;
  at?: string;
}

export interface WorkspaceFile {
  name: string;
  path: string;
  kind: 'dir' | 'file';
  is_dir?: boolean;
  size?: number;
  modified?: string;
}

export interface SessionAttachment {
  ref_id: string;
  kind: string;
  path: string;
  label: string;
  size: number;
  sha256: string;
  added_at_ms: number;
}

export interface Metric {
  label: string;
  value: string | number;
  delta?: string;
  tone?: Tone;
}

export interface CapabilityAction {
  label: string;
  kind: 'primary' | 'secondary' | 'danger';
  endpoint?: string;
}

export interface CapabilitySection {
  id: string;
  label: string;
  description: string;
}

export interface ChartPoint {
  name: string;
  value: number;
}

export interface CapabilitySpec {
  id: NavId;
  title: string;
  subtitle: string;
  primaryAction: string;
  metrics: Metric[];
  chartKind: 'line' | 'bar' | 'donut' | 'radar' | 'heatmap' | 'graph';
  chartTitle: string;
  chartData: ChartPoint[];
  tableTitle: string;
  rows: Array<Record<string, string | number>>;
  sections: CapabilitySection[];
  actions: CapabilityAction[];
  inspector: Array<{ label: string; value: string }>;
}
