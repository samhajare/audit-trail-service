import type { AuditEvent } from '../../src/contracts/audit-event';

export function auditEventFixture(
  overrides: Partial<AuditEvent> = {},
): AuditEvent {
  return {
    eventId: 'b3-event-1',
    schemaVersion: '1.0',
    eventType: 'USER_LOGIN',
    timestamp: '2026-10-04T00:00:00Z',
    tenantId: 'b3-tenant',
    correlationId: 'b3-flow',
    actor: { id: 'user-1' },
    resource: { type: 'session', id: 'session-1' },
    action: 'login',
    changes: { before: null, after: null },
    context: { service: 'identity' },
    metadata: {},
    ...overrides,
  };
}
