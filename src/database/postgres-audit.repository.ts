import { Injectable } from '@nestjs/common';
import type { QueryResultRow } from 'pg';
import { z } from 'zod';
import {
  AuditRepository,
  AuditPage,
  AuditReadOptions,
  AuditStatistics,
  CreateAuditEventResult,
  PersistedAuditEvent,
} from '../audit/audit.repository';
import type { AuditEvent } from '../contracts/audit-event';
import { auditEventSchema } from '../contracts/audit-event.schema';
import { EventType } from '../contracts/event-types';
import { AuditFilters, auditFiltersSchema } from '../audit/audit-filters';
import { assertNoCredentials } from './assert-no-credentials';
import { DatabaseService } from './database.service';

interface AuditRow extends QueryResultRow {
  id: string;
  event_id: string;
  schema_version: AuditEvent['schemaVersion'];
  event_type: EventType;
  event_timestamp: Date | string;
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
  created_at: Date | string;
}

const identifier = z.string().refine((value) => value.trim().length > 0);
const readOptions = auditFiltersSchema.safeExtend({
  limit: z.number().int().min(1).max(1000).default(100),
  offset: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER).default(0),
});

function whereClause(tenantId: string, filters: AuditFilters) {
  identifier.parse(tenantId);
  const parsed = auditFiltersSchema.parse(filters);
  const values: unknown[] = [tenantId];
  const conditions = ['tenant_id = $1'];
  for (const [key, column] of [
    ['eventType', 'event_type'],
    ['resourceType', 'resource_type'],
    ['resourceId', 'resource_id'],
    ['severity', 'severity'],
    ['correlationId', 'correlation_id'],
  ] as const) {
    if (parsed[key] !== undefined)
      conditions.push(`${column} = $${values.push(parsed[key])}`);
  }
  if (parsed.actor !== undefined) {
    const parameter = values.push(parsed.actor);
    conditions.push(
      `(actor_id = $${parameter} OR actor_email = $${parameter})`,
    );
  }
  if (parsed.service !== undefined) {
    conditions.push(
      `(jsonb_typeof(context->'service') = 'string' AND context->>'service' = $${values.push(parsed.service)})`,
    );
  }
  if (parsed.from !== undefined)
    conditions.push(
      `event_timestamp >= $${values.push(parsed.from)}::timestamptz`,
    );
  if (parsed.to !== undefined)
    conditions.push(
      `event_timestamp <= $${values.push(parsed.to)}::timestamptz`,
    );
  return { sql: conditions.join(' AND '), values };
}

function timestamp(value: string | Date) {
  return typeof value === 'string'
    ? new Date(value).toISOString()
    : value.toISOString();
}

function countNumber(value: string) {
  const count = Number(value);
  if (!Number.isSafeInteger(count) || count < 0)
    throw new Error('Audit count exceeds safe integer range');
  return count;
}

function toEvent(row: AuditRow): PersistedAuditEvent {
  return {
    id: row.id,
    createdAt: timestamp(row.created_at),
    eventId: row.event_id,
    schemaVersion: row.schema_version,
    eventType: row.event_type,
    timestamp: timestamp(row.event_timestamp),
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

  async findById(
    tenantId: string,
    id: string,
  ): Promise<PersistedAuditEvent | null> {
    identifier.parse(tenantId);
    z.uuid().parse(id);
    const result = await this.database.pool.query<AuditRow>(
      'SELECT * FROM audit_events WHERE tenant_id = $1 AND id = $2::uuid',
      [tenantId, id],
    );
    return result.rows[0] ? toEvent(result.rows[0]) : null;
  }

  async findPage(
    tenantId: string,
    options: AuditReadOptions = {},
    order: 'created' | 'timeline' = 'created',
  ): Promise<AuditPage> {
    const { limit, offset, ...filters } = readOptions.parse(options);
    const where = whereClause(tenantId, filters);
    z.enum(['created', 'timeline']).parse(order);
    const ordering =
      order === 'timeline'
        ? 'event_timestamp ASC, id ASC'
        : 'created_at DESC, id DESC';
    const limitParam = where.values.push(limit);
    const offsetParam = where.values.push(offset);
    // A single PostgreSQL statement gives page and count the same snapshot,
    // including empty/out-of-range pages during concurrent ingestion.
    const result = await this.database.pool.query<{
      total: string;
      items: AuditRow[];
    }>(
      `
      WITH filtered AS MATERIALIZED (SELECT * FROM audit_events WHERE ${where.sql})
      SELECT (SELECT count(*) FROM filtered)::text AS total,
        COALESCE((SELECT jsonb_agg(page ORDER BY ${ordering}) FROM (
          SELECT * FROM filtered ORDER BY ${ordering} LIMIT $${limitParam} OFFSET $${offsetParam}
        ) page), '[]'::jsonb) AS items
    `,
      where.values,
    );
    const row = result.rows[0];
    if (!row) throw new Error('Audit page query returned no result');
    return { total: countNumber(row.total), items: row.items.map(toEvent) };
  }

  async findMany(
    tenantId: string,
    options: AuditReadOptions = {},
  ): Promise<PersistedAuditEvent[]> {
    const { limit, offset, ...filters } = readOptions.parse(options);
    const where = whereClause(tenantId, filters);
    const limitParam = where.values.push(limit);
    const offsetParam = where.values.push(offset);
    const result = await this.database.pool.query<AuditRow>(
      `
      SELECT * FROM audit_events
      WHERE ${where.sql}
      ORDER BY created_at DESC, id DESC LIMIT $${limitParam} OFFSET $${offsetParam}
    `,
      where.values,
    );
    return result.rows.map(toEvent);
  }

  async findByCorrelationId(
    tenantId: string,
    correlationId: string,
    options: AuditReadOptions = {},
  ): Promise<PersistedAuditEvent[]> {
    identifier.parse(correlationId);
    const { limit, offset, ...filters } = readOptions.parse(options);
    const where = whereClause(tenantId, { ...filters, correlationId });
    const limitParam = where.values.push(limit);
    const offsetParam = where.values.push(offset);
    const result = await this.database.pool.query<AuditRow>(
      `
      SELECT * FROM audit_events
      WHERE ${where.sql}
      ORDER BY event_timestamp ASC, id ASC LIMIT $${limitParam} OFFSET $${offsetParam}
    `,
      where.values,
    );
    return result.rows.map(toEvent);
  }

  async getStatistics(
    tenantId: string,
    filters: AuditFilters = {},
  ): Promise<AuditStatistics> {
    const where = whereClause(tenantId, filters);
    const result = await this.database.pool.query<{
      event_type: EventType;
      count: string;
    }>(
      `SELECT event_type, COUNT(*) AS count FROM audit_events WHERE ${where.sql} GROUP BY event_type`,
      where.values,
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
      const count = countNumber(row.count);
      byEventType[row.event_type] = count;
      total += count;
    }
    if (!Number.isSafeInteger(total))
      throw new Error('Audit count exceeds safe integer range');
    return { total, byEventType };
  }
}
