import { describe, it, expect, beforeEach, vi } from 'vitest';
import './api.js';
import './ui.js';
import './panels.js';

describe('API module', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('has all session endpoints', () => {
    expect(typeof window.Api.listSessions).toBe('function');
    expect(typeof window.Api.createSession).toBe('function');
    expect(typeof window.Api.deleteSession).toBe('function');
    expect(typeof window.Api.sendMessage).toBe('function');
    expect(typeof window.Api.getStreamUrl).toBe('function');
    expect(typeof window.Api.compactSession).toBe('function');
    expect(typeof window.Api.getMessages).toBe('function');
  });

  it('has all memory endpoints', () => {
    expect(typeof window.Api.memoryStatus).toBe('function');
    expect(typeof window.Api.listMemoryLayers).toBe('function');
    expect(typeof window.Api.searchMemory).toBe('function');
    expect(typeof window.Api.createMemoryEntry).toBe('function');
    expect(typeof window.Api.updateMemoryEntry).toBe('function');
    expect(typeof window.Api.deleteMemoryEntry).toBe('function');
    expect(typeof window.Api.listEntities).toBe('function');
    expect(typeof window.Api.detectEntities).toBe('function');
    expect(typeof window.Api.listTriples).toBe('function');
    expect(typeof window.Api.addTriple).toBe('function');
    expect(typeof window.Api.checkFacts).toBe('function');
    expect(typeof window.Api.registerFacts).toBe('function');
  });

  it('has all skill endpoints', () => {
    expect(typeof window.Api.listSkills).toBe('function');
    expect(typeof window.Api.installSkill).toBe('function');
    expect(typeof window.Api.viewSkill).toBe('function');
    expect(typeof window.Api.uninstallSkill).toBe('function');
    expect(typeof window.Api.invokeSkill).toBe('function');
    expect(typeof window.Api.toggleSkill).toBe('function');
  });

  it('has all cron endpoints', () => {
    expect(typeof window.Api.listCrons).toBe('function');
    expect(typeof window.Api.createCron).toBe('function');
    expect(typeof window.Api.deleteCron).toBe('function');
    expect(typeof window.Api.runCron).toBe('function');
    expect(typeof window.Api.pauseCron).toBe('function');
    expect(typeof window.Api.resumeCron).toBe('function');
  });

  it('has all workspace endpoints', () => {
    expect(typeof window.Api.getWorkspace).toBe('function');
    expect(typeof window.Api.listWorkspaces).toBe('function');
    expect(typeof window.Api.listFiles).toBe('function');
    expect(typeof window.Api.createFile).toBe('function');
    expect(typeof window.Api.getRawFile).toBe('function');
  });

  it('has all approval endpoints', () => {
    expect(typeof window.Api.pendingApprovals).toBe('function');
    expect(typeof window.Api.respondApproval).toBe('function');
    expect(typeof window.Api.getApprovalConfig).toBe('function');
    expect(typeof window.Api.updateApprovalConfig).toBe('function');
    expect(typeof window.Api.toggleSolo).toBe('function');
    expect(typeof window.Api.approvalHistory).toBe('function');
  });

  it('has config and usage endpoints', () => {
    expect(typeof window.Api.getConfig).toBe('function');
    expect(typeof window.Api.updateConfig).toBe('function');
    expect(typeof window.Api.getProviders).toBe('function');
    expect(typeof window.Api.getUsage).toBe('function');
  });

  it('has auth endpoints', () => {
    expect(typeof window.Api.login).toBe('function');
    expect(typeof window.Api.verifyAuth).toBe('function');
    expect(typeof window.Api.logout).toBe('function');
  });

  it('requests throw on non-ok', async () => {
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({ ok: false, status: 500, text: () => Promise.resolve('Server error') })
    ));
    await expect(window.Api.listSessions()).rejects.toThrow('Server error');
  });

  it('listSessions transforms response', async () => {
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve([{ id: 'abc', title: 'S1', created_at: 1 }]) })
    ));
    const { sessions } = await window.Api.listSessions();
    expect(sessions[0].id).toBe('abc');
    expect(sessions[0].started_at).toBe(1);
  });

  it('createSession uses default model', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ id: 'new-s' }) })
    );
    vi.stubGlobal('fetch', mockF);
    await window.Api.createSession();
    const body = JSON.parse(mockF.mock.calls[0][1].body);
    expect(body.model).toBe('claude-sonnet-4-6');
  });

  it('platform API endpoints are defined', () => {
    expect(typeof window.Api.listPlatforms).toBe('function');
    expect(typeof window.Api.getPlatform).toBe('function');
  });

  it('memory CRUD endpoints are defined', () => {
    expect(typeof window.Api.createMemoryEntry).toBe('function');
    expect(typeof window.Api.updateMemoryEntry).toBe('function');
    expect(typeof window.Api.deleteMemoryEntry).toBe('function');
    expect(typeof window.Api.listEntities).toBe('function');
    expect(typeof window.Api.listTriples).toBe('function');
  });

  it('gateway and skills endpoints are defined', () => {
    expect(typeof window.Api.getUsage).toBe('function');
    expect(typeof window.Api.toggleSolo).toBe('function');
    expect(typeof window.Api.compactSession).toBe('function');
  });

  it('Panels module exposes all panel renderers', () => {
    expect(typeof window.Panels.renderMemory).toBe('function');
    expect(typeof window.Panels.renderSkills).toBe('function');
    expect(typeof window.Panels.renderCrons).toBe('function');
    expect(typeof window.Panels.renderAgents).toBe('function');
    expect(typeof window.Panels.renderGateway).toBe('function');
    expect(typeof window.Panels.renderTools).toBe('function');
    expect(typeof window.Panels.renderSettings).toBe('function');
    expect(typeof window.Panels.renderCCConfig).toBe('function');
    expect(typeof window.Panels.renderCCProviders).toBe('function');
    expect(typeof window.Panels.renderCCApproval).toBe('function');
    expect(typeof window.Panels.renderCCHistory).toBe('function');
    expect(typeof window.Panels.renderCCUsage).toBe('function');
  });

  it('command and fact endpoints are defined', () => {
    expect(typeof window.Api.listCommands).toBe('function');
    expect(typeof window.Api.commandHistory).toBe('function');
    expect(typeof window.Api.executeCommand).toBe('function');
    expect(typeof window.Api.auditFacts).toBe('function');
  });

  it('cron log endpoints are defined', () => {
    expect(typeof window.Api.listCronLogs).toBe('function');
    expect(typeof window.Api.listAllCronLogs).toBe('function');
  });
});
