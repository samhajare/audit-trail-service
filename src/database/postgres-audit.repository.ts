import { Injectable } from '@nestjs/common';
import type { QueryResultRow } from 'pg';
import { z } from 'zod';
import {
  AuditRepository,
  AuditReadOptions,
  AuditStatistics,
  CreateAuditEventResult,
  PersistedAuditEvent,
} from '../audit/audit.repository';
import type { AuditEvent } from '../contracts/audit-event';
import { auditEventSchema } from '../contracts/audit-event.schema';
import { EVENT_TYPES, EventType } from '../contracts/event-types';
import { assertNoCredentials } from './assert-no-credentials';
import { DatabaseService } from './database.service';

interface AuditRow extends QueryResultRow {
  id: string;
  event_id: string;
  schema_version: AuditEvent['schemaVersion'];
  event_type: EventType;
  event_timestamp: Date;
  tenant_id: string;
  correlation_id: string;
  actor_id: string;
  actor_email: string | null;
  actor_role: string | null;
  resource_type: string;
  resource_id: string;
  action: string;
  before_data: AuditEvent['changes']['before'];
  after_data: AuditEvent['changes']['after'];
  context: AuditEvent['context'];
  metadata: AuditEvent['metadata'];
  created_at: Date;
}

const identifier = z.string().refine((value) => value.trim().length > 0);
const readOptions = z.strictObject({
  limit: z.number().int().min(1).max(1000).default(100),
  offset: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER).default(0),
  eventType: z.enum(EVENT_TYPES).optional(),
});

function toEvent(row: AuditRow): PersistedAuditEvent {
  return {
    id: row.id,
    createdAt: row.created_at.toISOString(),
    eventId: row.event_id,
    schemaVersion: row.schema_version,
    eventType: row.event_type,
    timestamp: row.event_timestamp.toISOString(),
    tenantId: row.tenant_id,
    correlationId: row.correlation_id,
    actor: {
      id: row.actor_id,
      ...(row.actor_email === null ? {} : { email: row.actor_email }),
      ...(row.actor_role === null ? {} : { role: row.actor_role }),
    },
    resource: { type: row.resource_type, id: row.resource_id },
    action: row.action,
    changes: { before: row.before_data, after: row.after_data },
    context: row.context,
    metadata: row.metadata,
  };
}

@Injectable()
export class PostgresAuditRepository extends AuditRepository {
  constructor(private readonly database: DatabaseService) {
    super();
  }

  async create(payload: unknown): Promise<CreateAuditEventResult> {
    const event = auditEventSchema.parse(payload);
    assertNoCredentials(event);
    const result = await this.database.pool.query<AuditRow>(
      `
      INSERT INTO audit_events (
        event_id, schema_version, event_type, event_timestamp, tenant_id,
        correlation_id, actor_id, actor_email, actor_role, resource_type,
        resource_id, action, severity, before_data, after_data, context, metadata
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15::jsonb,$16::jsonb,$17::jsonb)
      ON CONFLICT (event_id) DO NOTHING
      RETURNING *
    `,
      [
        event.eventId,
        event.schemaVersion,
        event.eventType,
        event.timestamp,
        event.tenantId,
        event.correlationId,
        event.actor.id,
        event.actor.email ?? null,
        event.actor.role ?? null,
        event.resource.type,
        event.resource.id,
        event.action,
        typeof event.metadata.severity === 'string'
          ? event.metadata.severity
          : null,
        event.changes.before === null
          ? null
          : JSON.stringify(event.changes.before),
        event.changes.after === null
          ? null
          : JSON.stringify(event.changes.after),
        JSON.stringify(event.context),
        JSON.stringify(event.metadata),
      ],
    );
    const row = result.rows[0];
    // A duplicate never returns another tenant's original event or overwrites it.
    return row
      ? { status: 'created', event: toEvent(row) }
      : { status: 'duplicate' };
  }

  async findByEventId(
    tenantId: string,
    eventId: string,
  ): Promise<PersistedAuditEvent | null> {
    identifier.parse(tenantId);
    identifier.parse(eventId);
    const result = await this.database.pool.query<AuditRow>(
      'SELECT * FROM audit_events WHERE tenant_id = $1 AND event_id = $2',
      [tenantId, eventId],
    );
    return result.rows[0] ? toEvent(result.rows[0]) : null;
  }

  async findMany(
    tenantId: string,
    options: AuditReadOptions = {},
  ): Promise<PersistedAuditEvent[]> {
    identifier.parse(tenantId);
    const parsed = readOptions.parse(options);
    const result = await this.database.pool.query<AuditRow>(
      `
      SELECT * FROM audit_events
      WHERE tenant_id = $1 AND ($2::varchar IS NULL OR event_type = $2)
      ORDER BY created_at DESC, id DESC LIMIT $3 OFFSET $4
    `,
      [tenantId, parsed.eventType ?? null, parsed.limit, parsed.offset],
    );
    return result.rows.map(toEvent);
  }

  async findByCorrelationId(
    tenantId: string,
    correlationId: string,
    options: AuditReadOptions = {},
  ): Promise<PersistedAuditEvent[]> {
    identifier.parse(tenantId);
    identifier.parse(correlationId);
    const parsed = readOptions.parse(options);
    const result = await this.database.pool.query<AuditRow>(
      `
      SELECT * FROM audit_events
      WHERE tenant_id = $1 AND correlation_id = $2
        AND ($3::varchar IS NULL OR event_type = $3)
      ORDER BY event_timestamp ASC, id ASC LIMIT $4 OFFSET $5
    `,
      [
        tenantId,
        correlationId,
        parsed.eventType ?? null,
        parsed.limit,
        parsed.offset,
      ],
    );
    return result.rows.map(toEvent);
  }

  async getStatistics(tenantId: string): Promise<AuditStatistics> {
    identifier.parse(tenantId);
    const result = await this.database.pool.query<{
      event_type: EventType;
      count: string;
    }>(
      'SELECT event_type, COUNT(*) AS count FROM audit_events WHERE tenant_id = $1 GROUP BY event_type',
      [tenantId],
    );
    const byEventType: Record<EventType, number> = {
      USER_LOGIN: 0,
      USER_ROLE_CHANGED: 0,
      DATA_EXPORTED: 0,
      CONFIG_CHANGED: 0,
      PAYMENT_REFUNDED: 0,
    };
    let total = 0;
    for (const row of result.rows) {
      const count = Number(row.count);
      if (!Number.isSafeInteger(count))
        throw new Error('Audit count exceeds safe integer range');
      byEventType[row.event_type] = count;
      total += count;
    }
    if (!Number.isSafeInteger(total))
      throw new Error('Audit count exceeds safe integer range');
    return { total, byEventType };
  }
}
