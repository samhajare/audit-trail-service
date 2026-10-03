import type { AuditEvent } from '../src/contracts/audit-event';
import { auditEventSchema } from '../src/contracts/audit-event.schema';
import { EVENT_TYPES } from '../src/contracts/event-types';
import { SCHEMA_VERSION } from '../src/contracts/schema-version';

function validEvent(): AuditEvent {
  return {
    eventId: 'event-1',
    schemaVersion: SCHEMA_VERSION,
    eventType: 'USER_ROLE_CHANGED',
    timestamp: '2026-10-03T12:30:00.000Z',
    tenantId: 'tenant-1',
    correlationId: 'flow-1',
    actor: { id: 'user-1', email: 'user@example.com', role: 'ADMIN' },
    resource: { type: 'user', id: 'user-2' },
    action: 'role.changed',
    changes: { before: { role: 'VIEWER' }, after: { role: 'ANALYST' } },
    context: { service: 'identity', nested: { values: [1, true, null] } },
    metadata: { severity: 'INFO' },
  };
}

describe('Audit event schema 1.0', () => {
  it.each(EVENT_TYPES)('accepts supported event type %s', (eventType) => {
    const event = { ...validEvent(), eventType };
    expect(auditEventSchema.parse(event)).toEqual(event);
  });

  it('accepts minimal nested objects and no state change', () => {
    const event = {
      ...validEvent(),
      actor: { id: 'system' },
      changes: { before: null, after: null },
      context: {},
      metadata: {},
    };
    expect(auditEventSchema.parse(event)).toEqual(event);
  });

  it.each(Object.keys(validEvent()))('rejects missing field %s', (field) => {
    const event: Record<string, unknown> = validEvent();
    delete event[field];
    const result = auditEventSchema.safeParse(event);
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.issues.some((issue) => issue.path[0] === field)).toBe(
        true,
      );
    }
  });

  it.each(['eventId', 'tenantId', 'correlationId', 'action'])(
    'rejects blank or non-string %s',
    (field) => {
      for (const value of ['', '  \t', null, 123]) {
        expect(
          auditEventSchema.safeParse({ ...validEvent(), [field]: value })
            .success,
        ).toBe(false);
      }
    },
  );

  it.each(['USER_DELETED', 'user_login', '', 1])(
    'rejects unsupported event type %s',
    (eventType) => {
      expect(
        auditEventSchema.safeParse({ ...validEvent(), eventType }).success,
      ).toBe(false);
    },
  );

  it.each(['2.0', '1.1', '1', 1, null])(
    'rejects unsupported version %s',
    (schemaVersion) => {
      expect(
        auditEventSchema.safeParse({ ...validEvent(), schemaVersion }).success,
      ).toBe(false);
    },
  );

  it.each([
    'not-a-date',
    '2026-02-30T12:00:00Z',
    '2025-02-29T12:00:00Z',
    '2026-10-03',
    '2026-10-03T12:00:00',
    '2026-10-03T25:00:00Z',
    1791028800000,
  ])('rejects invalid or timezone-free timestamp %s', (timestamp) => {
    expect(
      auditEventSchema.safeParse({ ...validEvent(), timestamp }).success,
    ).toBe(false);
  });

  it.each(['2024-02-29T12:00:00Z', '2026-10-03T18:00:00+05:30'])(
    'accepts valid calendar timestamp %s',
    (timestamp) => {
      expect(
        auditEventSchema.parse({ ...validEvent(), timestamp }).timestamp,
      ).toBe(timestamp);
    },
  );

  it.each([
    { actor: {} },
    { actor: { id: ' ' } },
    { actor: { id: 'user-1', email: 'invalid' } },
    { actor: { id: 'user-1', role: '' } },
    { resource: { type: 'user' } },
    { resource: { id: 'user-1' } },
    { resource: { type: '', id: 'user-1' } },
    { resource: { type: 'user', id: 1 } },
    { changes: {} },
    { changes: { before: null } },
    { changes: { before: [], after: null } },
    { context: null },
    { context: [] },
    { metadata: 'text' },
    { metadata: { value: undefined } },
    { metadata: { value: Infinity } },
    { metadata: { value: () => 'value' } },
  ])('rejects malformed nested payload %j', (invalidFields) => {
    expect(
      auditEventSchema.safeParse({ ...validEvent(), ...invalidFields }).success,
    ).toBe(false);
  });

  it.each([null, [], 'json text', 42])(
    'rejects non-object input %j',
    (input) => {
      expect(auditEventSchema.safeParse(input).success).toBe(false);
    },
  );

  it('rejects unknown envelope and structured nested fields', () => {
    expect(
      auditEventSchema.safeParse({ ...validEvent(), unexpected: true }).success,
    ).toBe(false);
    expect(
      auditEventSchema.safeParse({
        ...validEvent(),
        actor: { id: 'user-1', unexpected: true },
      }).success,
    ).toBe(false);
  });

  it('throws on invalid input when using parse', () => {
    expect(() =>
      auditEventSchema.parse({ ...validEvent(), schemaVersion: '2.0' }),
    ).toThrow();
  });

  it('does not coerce or mutate input', () => {
    const event = validEvent();
    const original = structuredClone(event);
    const parsed = auditEventSchema.parse(event);
    expect(event).toEqual(original);
    expect(parsed).toEqual(original);
    expect(parsed).not.toBe(event);
  });
});
