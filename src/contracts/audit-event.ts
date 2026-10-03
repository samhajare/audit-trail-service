import type { z } from 'zod';
import type { auditEventSchema } from './audit-event.schema';

/** Validated schema 1.0 payload. Parse untrusted input before using this type. */
export type AuditEvent = z.infer<typeof auditEventSchema>;
export type AuditActor = AuditEvent['actor'];
export type AuditResource = AuditEvent['resource'];
export type AuditChanges = AuditEvent['changes'];
