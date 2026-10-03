import type { AuditEvent } from '../contracts/audit-event';
import type { EventType } from '../contracts/event-types';

export interface PersistedAuditEvent extends AuditEvent {
  id: string;
  createdAt: string;
}

export type CreateAuditEventResult =
  { status: 'created'; event: PersistedAuditEvent } | { status: 'duplicate' };

export interface AuditReadOptions {
  limit?: number;
  offset?: number;
  eventType?: EventType;
}

export interface AuditStatistics {
  total: number;
  byEventType: Record<EventType, number>;
}

/** All reads require a tenant from trusted server context, never client filtering. */
export abstract class AuditRepository {
  abstract create(payload: unknown): Promise<CreateAuditEventResult>;
  abstract findByEventId(
    tenantId: string,
    eventId: string,
  ): Promise<PersistedAuditEvent | null>;
  abstract findMany(
    tenantId: string,
    options?: AuditReadOptions,
  ): Promise<PersistedAuditEvent[]>;
  abstract findByCorrelationId(
    tenantId: string,
    correlationId: string,
    options?: AuditReadOptions,
  ): Promise<PersistedAuditEvent[]>;
  abstract getStatistics(tenantId: string): Promise<AuditStatistics>;
}
