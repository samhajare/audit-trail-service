import { Injectable } from '@nestjs/common';
import { z } from 'zod';
import { DatabaseService } from './database.service';
import { assertNoCredentials } from './assert-no-credentials';
import { DlqRecord, DlqRepository } from '../dlq/dlq.repository';
import {
  FailureEnvelope,
  failureEnvelopeSchema,
} from '../dlq/failure-envelope';

interface Row {
  id: string;
  tenant_id: string | null;
  event_id: string | null;
  envelope: unknown;
  replay_status: DlqRecord['replayStatus'];
  created_at: Date;
}
const identifier = z
  .string()
  .min(1)
  .refine((value) => !!value.trim());
function mapRow(row: Row): DlqRecord {
  const envelope = failureEnvelopeSchema.parse(row.envelope);
  assertNoCredentials(envelope);
  const event = envelope.originalEvent as {
    tenantId?: unknown;
    eventId?: unknown;
  } | null;
  if (
    !event ||
    event.tenantId !== row.tenant_id ||
    event.eventId !== row.event_id
  )
    throw new Error('Invalid DLQ tenant context');
  return {
    id: row.id,
    tenantId: row.tenant_id,
    eventId: row.event_id,
    envelope,
    replayStatus: row.replay_status,
    createdAt: row.created_at.toISOString(),
  };
}
@Injectable()
export class PostgresDlqRepository extends DlqRepository {
  constructor(private readonly database: DatabaseService) {
    super();
  }
  async store(
    value: FailureEnvelope,
    location: { topic: string; partition: number; offset: string },
  ) {
    const envelope = failureEnvelopeSchema.parse(value);
    assertNoCredentials(envelope);
    const event = envelope.originalEvent as {
      tenantId?: unknown;
      eventId?: unknown;
    } | null;
    const tenant = identifier.safeParse(event?.tenantId);
    const id = identifier.safeParse(event?.eventId);
    await this.database.pool.query(
      `INSERT INTO audit_dlq(tenant_id,event_id,kafka_topic,kafka_partition,kafka_offset,envelope) VALUES($1,$2,$3,$4,$5,$6::jsonb) ON CONFLICT DO NOTHING`,
      [
        tenant.success ? tenant.data : null,
        id.success ? id.data : null,
        location.topic,
        location.partition,
        location.offset,
        JSON.stringify(envelope),
      ],
    );
  }
  async list(tenantId: string, limit: number, offset: number) {
    identifier.parse(tenantId);
    z.number().int().min(1).max(100).parse(limit);
    z.number().int().min(0).max(Number.MAX_SAFE_INTEGER).parse(offset);
    const client = await this.database.pool.connect();
    try {
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
      const rows = await client.query<Row>(
        'SELECT * FROM audit_dlq WHERE tenant_id=$1 AND event_id IS NOT NULL ORDER BY created_at DESC,id DESC LIMIT $2 OFFSET $3',
        [tenantId, limit, offset],
      );
      const count = await client.query<{ total: string }>(
        'SELECT count(*)::text total FROM audit_dlq WHERE tenant_id=$1 AND event_id IS NOT NULL',
        [tenantId],
      );
      const result = {
        items: rows.rows.map(mapRow),
        total: Number(count.rows[0]!.total),
      };
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  async detail(tenantId: string, eventId: string) {
    identifier.parse(tenantId);
    identifier.parse(eventId);
    const result = await this.database.pool.query<Row>(
      'SELECT * FROM audit_dlq WHERE tenant_id=$1 AND event_id=$2',
      [tenantId, eventId],
    );
    return result.rows[0] ? mapRow(result.rows[0]) : null;
  }
  async reserve(record: DlqRecord, actorId: string) {
    const client = await this.database.pool.connect();
    try {
      await client.query('BEGIN');
      const reserved = await client.query(
        "UPDATE audit_dlq SET replay_status='reserved' WHERE id=$1 AND tenant_id=$2 AND event_id=$3 AND replay_status='pending' RETURNING id",
        [record.id, record.tenantId, record.eventId],
      );
      if (!reserved.rowCount) {
        await client.query('ROLLBACK');
        return null;
      }
      const replay = await client.query<{ id: string }>(
        `INSERT INTO audit_dlq_replays(dlq_id,tenant_id,event_id,actor_id,status) VALUES($1,$2,$3,$4,'reserved') RETURNING id`,
        [record.id, record.tenantId, record.eventId, actorId],
      );
      await client.query('COMMIT');
      return replay.rows[0]!.id;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  async complete(
    recordId: string,
    replayId: string,
    status: 'published' | 'failed',
  ) {
    const client = await this.database.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        "UPDATE audit_dlq SET replay_status=$2 WHERE id=$1 AND replay_status='reserved'",
        [recordId, status],
      );
      await client.query(
        'UPDATE audit_dlq_replays SET status=$2,completed_at=CURRENT_TIMESTAMP WHERE id=$1',
        [replayId, status],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}
