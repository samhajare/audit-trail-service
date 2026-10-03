import { LAUNCHDARKLY_CLIENT } from '../src/feature-flags/feature-flag.service';
import { randomUUID } from 'node:crypto';
import { INestApplicationContext } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import { Pool } from 'pg';
import { AppModule } from '../src/app.module';
import { AuditRepository } from '../src/audit/audit.repository';
import type { AuditEvent } from '../src/contracts/audit-event';
import { databasePoolConfig } from '../src/database/database.config';
import { DatabaseService } from '../src/database/database.service';
import { runMigrations } from '../src/database/migrate';

function event(overrides: Partial<AuditEvent> = {}): AuditEvent {
  return {
    eventId: randomUUID(),
    schemaVersion: '1.0',
    eventType: 'USER_ROLE_CHANGED',
    timestamp: '2026-10-03T18:00:00+05:30',
    tenantId: 'tenant-a',
    correlationId: 'flow-1',
    actor: { id: 'actor-1', email: 'actor@example.com', role: 'ADMIN' },
    resource: { type: 'user', id: 'user-1' },
    action: 'role.changed',
    changes: { before: { role: 'VIEWER' }, after: { role: 'ANALYST' } },
    context: { service: 'identity', nested: { values: [1, true, null] } },
    metadata: { severity: 'INFO', nested: { labels: ['demo'] } },
    ...overrides,
  };
}

describe('PostgreSQL audit persistence', () => {
  const schema = `b2_test_${randomUUID().replace(/-/g, '')}`;
  let admin: Pool;
  let pool: Pool;
  let app: INestApplicationContext;
  let repository: AuditRepository;

  beforeAll(async () => {
    const config = databasePoolConfig(new ConfigService(process.env));
    admin = new Pool(config);
    await admin.query(`CREATE SCHEMA "${schema}"`);
    pool = new Pool({ ...config, options: `-c search_path=${schema}` });
    // Only this newly created schema is touched by migration/tests/cleanup.
    await runMigrations(pool);
    const module = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(LAUNCHDARKLY_CLIENT)
      .useValue(null)
      .overrideProvider(DatabaseService)
      .useValue({ pool })
      .compile();
    app = module;
    repository = app.get(AuditRepository);
  }, 30000);

  beforeEach(async () => {
    await pool.query('TRUNCATE audit_events');
  });

  afterAll(async () => {
    if (app) await app.close();
    if (pool) await pool.end();
    if (admin) {
      try {
        await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      } finally {
        await admin.end();
      }
    }
  });

  it('applies migration once, including concurrent runs', async () => {
    await Promise.all([runMigrations(pool), runMigrations(pool)]);
    const result = await pool.query(
      'SELECT id FROM schema_migrations ORDER BY id',
    );
    expect(result.rows).toEqual([
      { id: '001-create-audit-events' },
      { id: '002-create-audit-dlq' },
    ]);
  });

  it('creates required JSONB columns, timestamps, and indexes', async () => {
    const columns = await pool.query<{
      column_name: string;
      data_type: string;
    }>(
      'SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2',
      [schema, 'audit_events'],
    );
    for (const name of ['before_data', 'after_data', 'context', 'metadata']) {
      expect(columns.rows).toContainEqual({
        column_name: name,
        data_type: 'jsonb',
      });
    }
    expect(columns.rows).toContainEqual({
      column_name: 'event_timestamp',
      data_type: 'timestamp with time zone',
    });
    const indexes = await pool.query<{ indexname: string }>(
      'SELECT indexname FROM pg_indexes WHERE schemaname=$1 AND tablename=$2',
      [schema, 'audit_events'],
    );
    for (const field of [
      'tenant_id',
      'event_type',
      'created_at',
      'correlation_id',
      'actor_id',
      'resource_id',
    ]) {
      expect(indexes.rows).toContainEqual({
        indexname: `audit_events_${field}_idx`,
      });
    }
  });

  it('persists and retrieves all contract fields and JSONB snapshots', async () => {
    const payload = event();
    const result = await repository.create(payload);
    expect(result.status).toBe('created');
    if (result.status !== 'created') throw new Error('Expected inserted event');
    expect(result.event).toEqual({
      ...payload,
      timestamp: '2026-10-03T12:30:00.000Z',
      id: expect.any(String),
      createdAt: expect.any(String),
    });
    expect(result.event.id).toMatch(/^[0-9a-f-]{36}$/);
    expect(Number.isNaN(Date.parse(result.event.createdAt))).toBe(false);
    expect(await repository.findByEventId('tenant-a', payload.eventId)).toEqual(
      result.event,
    );
    expect(await repository.findByEventId('tenant-a', 'absent')).toBeNull();
    const raw = await pool.query('SELECT severity FROM audit_events');
    expect(raw.rows).toEqual([{ severity: 'INFO' }]);
  });

  it('preserves null changes and omitted optional actor fields', async () => {
    const payload = event({
      actor: { id: 'system' },
      changes: { before: null, after: null },
    });
    await repository.create(payload);
    const stored = await repository.findByEventId('tenant-a', payload.eventId);
    expect(stored?.actor).toEqual({ id: 'system' });
    expect(stored?.changes).toEqual({ before: null, after: null });
  });

  it('prevents simultaneous duplicate inserts and never overwrites', async () => {
    const payload = event();
    const results = await Promise.all(
      Array.from({ length: 10 }, () => repository.create(payload)),
    );
    expect(
      results.filter((result) => result.status === 'created'),
    ).toHaveLength(1);
    expect(
      results.filter((result) => result.status === 'duplicate'),
    ).toHaveLength(9);
    expect(
      await repository.create({ ...payload, action: 'different' }),
    ).toEqual({ status: 'duplicate' });
    expect(
      (await repository.findByEventId('tenant-a', payload.eventId))?.action,
    ).toBe(payload.action);
    expect((await repository.getStatistics('tenant-a')).total).toBe(1);
  });

  it('enforces uniqueness even when bypassing the repository', async () => {
    await repository.create(event());
    await expect(
      pool.query(`
      INSERT INTO audit_events (event_id, schema_version, event_type, event_timestamp,
        tenant_id, correlation_id, actor_id, resource_type, resource_id, action, context, metadata)
      SELECT event_id, schema_version, event_type, event_timestamp,
        tenant_id, correlation_id, actor_id, resource_type, resource_id, action, context, metadata
      FROM audit_events
    `),
    ).rejects.toMatchObject({ code: '23505' });
  });

  it('scopes every read and statistic by tenant, including shared correlations', async () => {
    const a = event();
    const b = event({ tenantId: 'tenant-b', eventType: 'DATA_EXPORTED' });
    await repository.create(a);
    await repository.create(b);
    expect(await repository.findByEventId('tenant-a', b.eventId)).toBeNull();
    expect(
      (await repository.findMany('tenant-a')).map((row) => row.eventId),
    ).toEqual([a.eventId]);
    expect(
      (await repository.findByCorrelationId('tenant-b', 'flow-1')).map(
        (row) => row.eventId,
      ),
    ).toEqual([b.eventId]);
    expect(await repository.getStatistics('tenant-a')).toEqual({
      total: 1,
      byEventType: {
        USER_LOGIN: 0,
        USER_ROLE_CHANGED: 1,
        DATA_EXPORTED: 0,
        CONFIG_CHANGED: 0,
        PAYMENT_REFUNDED: 0,
      },
    });
    expect(
      (await repository.getStatistics('tenant-b')).byEventType.DATA_EXPORTED,
    ).toBe(1);
    expect((await repository.getStatistics('empty')).total).toBe(0);
    expect(await repository.create({ ...a, tenantId: 'tenant-b' })).toEqual({
      status: 'duplicate',
    });
    expect(await repository.findByEventId('tenant-b', a.eventId)).toBeNull();
  });

  it('orders timelines by event time and supports bounded reads', async () => {
    const later = event({
      timestamp: '2026-10-03T13:00:00Z',
      eventType: 'USER_LOGIN',
    });
    const earlier = event({ timestamp: '2026-10-03T12:00:00Z' });
    await repository.create(later);
    await repository.create(earlier);
    expect(
      (await repository.findByCorrelationId('tenant-a', 'flow-1')).map(
        (row) => row.eventId,
      ),
    ).toEqual([earlier.eventId, later.eventId]);
    expect(
      (
        await repository.findByCorrelationId('tenant-a', 'flow-1', {
          limit: 1,
          offset: 1,
        })
      )[0]?.eventId,
    ).toBe(later.eventId);
    expect(
      (await repository.findMany('tenant-a', { eventType: 'USER_LOGIN' }))[0]
        ?.eventId,
    ).toBe(later.eventId);
    const all = await repository.findMany('tenant-a');
    expect(
      await repository.findMany('tenant-a', { limit: 1, offset: 1 }),
    ).toEqual([all[1]]);
  });

  it('uses parameters for identifiers and arbitrary JSON values', async () => {
    const hostile = "x' OR 1=1; DROP TABLE audit_events; --";
    const payload = event({
      tenantId: hostile,
      eventId: hostile,
      correlationId: hostile,
      metadata: { note: hostile },
    });
    await repository.create(payload);
    expect(
      (await repository.findByEventId(hostile, hostile))?.metadata,
    ).toEqual({ note: hostile });
    expect(await repository.findMany('tenant-a')).toEqual([]);
    expect(await repository.findByCorrelationId('tenant-a', hostile)).toEqual(
      [],
    );
  });

  it('rejects missing tenant context and invalid pagination', async () => {
    for (const tenant of ['', '  ']) {
      await expect(repository.findByEventId(tenant, 'id')).rejects.toThrow();
      await expect(repository.findMany(tenant)).rejects.toThrow();
      await expect(
        repository.findByCorrelationId(tenant, 'flow'),
      ).rejects.toThrow();
      await expect(repository.getStatistics(tenant)).rejects.toThrow();
    }
    for (const options of [
      { limit: 0 },
      { limit: 1001 },
      { offset: -1 },
      { limit: 1.5 },
    ]) {
      await expect(repository.findMany('tenant-a', options)).rejects.toThrow();
    }
  });

  it('validates untrusted events before writing', async () => {
    await expect(
      repository.create({ ...event(), schemaVersion: '2.0' }),
    ).rejects.toThrow();
    expect((await repository.getStatistics('tenant-a')).total).toBe(0);
  });

  it.each([
    'password',
    'access_token',
    'refreshToken',
    'SECRET',
    'api-key',
    'Authorization',
    'private_key',
  ])('rejects credential field %s before persistence', async (key) => {
    await expect(
      repository.create(
        event({
          changes: {
            before: null,
            after: { nested: [{ [key]: 'test-only-value' }] },
          },
        }),
      ),
    ).rejects.toThrow('prohibited credential fields');
    expect((await repository.getStatistics('tenant-a')).total).toBe(0);
  });

  it('rolls back all migration changes after a DDL failure', async () => {
    const failedSchema = `${schema}_failed`;
    await admin.query(`CREATE SCHEMA "${failedSchema}"`);
    const failedPool = new Pool({
      ...databasePoolConfig(new ConfigService(process.env)),
      options: `-c search_path=${failedSchema}`,
    });
    try {
      await failedPool.query('CREATE TABLE audit_events (id integer)');
      await expect(runMigrations(failedPool)).rejects.toMatchObject({
        code: '42P07',
      });
      const ledger = await failedPool.query(
        'SELECT to_regclass($1) AS table_name',
        [`${failedSchema}.schema_migrations`],
      );
      expect(ledger.rows).toEqual([{ table_name: null }]);
    } finally {
      await failedPool.end();
      await admin.query(`DROP SCHEMA "${failedSchema}" CASCADE`);
    }
  });
});
