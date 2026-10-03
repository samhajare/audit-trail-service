import { z } from 'zod';
import { EVENT_TYPES } from './event-types';
import { SCHEMA_VERSION } from './schema-version';

// Preserve the original value; whitespace-only identifiers are not valid.
const nonBlankString = z.string().refine((value) => value.trim().length > 0, {
  message: 'Must not be blank',
});
const jsonObject = z.record(z.string(), z.json());

export const auditEventSchema = z.strictObject({
  eventId: nonBlankString,
  schemaVersion: z.literal(SCHEMA_VERSION),
  eventType: z.enum(EVENT_TYPES),
  timestamp: z.iso.datetime({ offset: true }),
  tenantId: nonBlankString,
  correlationId: nonBlankString,
  actor: z.strictObject({
    id: nonBlankString,
    email: z.email().optional(),
    role: nonBlankString.optional(),
  }),
  resource: z.strictObject({
    type: nonBlankString,
    id: nonBlankString,
  }),
  action: nonBlankString,
  changes: z.strictObject({
    before: jsonObject.nullable(),
    after: jsonObject.nullable(),
  }),
  context: jsonObject,
  metadata: jsonObject,
});
