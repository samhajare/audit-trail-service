import { randomUUID } from 'node:crypto';
import type { AuditEvent } from '../../src/contracts/audit-event';
import { auditEventSchema } from '../../src/contracts/audit-event.schema';
import { EVENT_TYPES, EventType } from '../../src/contracts/event-types';
import { SCHEMA_VERSION } from '../../src/contracts/schema-version';
import { SimulatorOptions, simulatorOptionsSchema } from './options';

const details: Record<
  EventType,
  Pick<AuditEvent, 'resource' | 'action' | 'changes' | 'context'>
> = {
  USER_LOGIN: {
    resource: { type: 'session', id: 'demo-session' },
    action: 'user.login',
    changes: { before: null, after: { authenticated: true } },
    context: { service: 'demo-identity' },
  },
  USER_ROLE_CHANGED: {
    resource: { type: 'user', id: 'demo-user' },
    action: 'user.role.changed',
    changes: {
      before: { role: 'AUDIT_VIEWER' },
      after: { role: 'AUDIT_ANALYST' },
    },
    context: { service: 'demo-identity' },
  },
  DATA_EXPORTED: {
    resource: { type: 'report', id: 'demo-report' },
    action: 'data.exported',
    changes: { before: null, after: { format: 'csv', rowCount: 25 } },
    context: { service: 'demo-reporting' },
  },
  CONFIG_CHANGED: {
    resource: { type: 'configuration', id: 'demo-config' },
    action: 'config.changed',
    changes: { before: { retentionDays: 30 }, after: { retentionDays: 90 } },
    context: { service: 'demo-config' },
  },
  PAYMENT_REFUNDED: {
    resource: { type: 'payment', id: 'demo-payment' },
    action: 'payment.refunded',
    changes: {
      before: { status: 'paid' },
      after: { status: 'refunded', amount: 25, currency: 'USD' },
    },
    context: { service: 'demo-payments' },
  },
};

export function generateEvents(input: SimulatorOptions): AuditEvent[] {
  const options = simulatorOptionsSchema.parse(input);
  const count =
    options.scenario === 'correlated' ? EVENT_TYPES.length : options.count;
  const sharedCorrelation =
    options.correlation ??
    (options.scenario === 'correlated' ? randomUUID() : undefined);
  const start = Date.now();
  const events = Array.from({ length: count }, (_, index) => {
    const eventType =
      options.type === 'all'
        ? EVENT_TYPES[index % EVENT_TYPES.length]!
        : options.type;
    return auditEventSchema.parse({
      eventId: randomUUID(),
      schemaVersion: SCHEMA_VERSION,
      eventType,
      timestamp: new Date(start + index).toISOString(),
      tenantId: options.tenant,
      correlationId: sharedCorrelation ?? randomUUID(),
      actor: {
        id: 'demo-actor',
        email: 'demo@example.com',
        role: 'AUDIT_ADMIN',
      },
      ...structuredClone(details[eventType]),
      metadata: { simulator: true, severity: 'INFO' },
    });
  });
  return options.scenario === 'duplicate'
    ? [events[0]!, structuredClone(events[0]!)]
    : events;
}
