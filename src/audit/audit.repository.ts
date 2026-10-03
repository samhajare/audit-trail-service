import type { AuditEvent } from '../contracts/audit-event';
import type { EventType } from '../contracts/event-types';
import type { AuditFilters } from './audit-filters';

export interface PersistedAuditEvent extends AuditEvent {
  id: string;
  createdAt: string;
}

export type CreateAuditEventResult =
  { status: 'created'; event: PersistedAuditEvent } | { status: 'duplicate' };

export interface AuditReadOptions extends AuditFilters {
  limit?: number;
  offset?: number;
}

export interface AuditPage {
  items: PersistedAuditEvent[];
  total: number;
}

export interface AuditStatistics {
  total: number;
  byEventType: Record<EventType, number>;
}

/** All reads require a tenant from trusted server context, never client filtering. */
export abstract class AuditRepository {
  abstract create(payload: unknown): Promise<CreateAuditEventResult>;
  abstract findById(
    tenantId: string,
    id: string,
  ): Promise<PersistedAuditEvent | null>;
  abstract findPage(
    tenantId: string,
    options?: AuditReadOptions,
    order?: 'created' | 'timeline',
  ): Promise<AuditPage>;
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
  abstract getStatistics(
    tenantId: string,
    filters?: AuditFilters,
  ): Promise<AuditStatistics>;
}
