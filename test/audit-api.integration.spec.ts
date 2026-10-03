import {
  FeatureFlagService,
  LAUNCHDARKLY_CLIENT,
} from '../src/feature-flags/feature-flag.service';
import { randomUUID } from 'node:crypto';
import { INestApplication, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import { Pool } from 'pg';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import {
  AuditRepository,
  PersistedAuditEvent,
} from '../src/audit/audit.repository';
import { AUTH0_CONFIG } from '../src/auth/auth0.config';
import { Auth0Fixture } from './fixtures/auth0.fixture';
import { databasePoolConfig } from '../src/database/database.config';
import { DatabaseService } from '../src/database/database.service';
import { runMigrations } from '../src/database/migrate';
import { AuditConsumerService } from '../src/kafka/audit-consumer.service';
import { auditEventFixture } from './fixtures/audit-event.fixture';

describe('Audit REST API with PostgreSQL', () => {
  const schema = `b5_test_${randomUUID().replace(/-/g, '')}`;
  const tenantId = 'b5-tenant-a';
  const correlationId = 'b5-flow';
  let admin: Pool;
  let pool: Pool;
  let app: INestApplication;
  let repository: AuditRepository;
  const stored: PersistedAuditEvent[] = [];
  const auth = new Auth0Fixture();
  const api = () => ({
    get: (path: string) =>
      request(app.getHttpServer())
        .get(path)
        .set('Authorization', 'Bearer ' + auth.token(tenantId)),
  });

  beforeAll(async () => {
    await auth.start();
    admin = new Pool(databasePoolConfig(new ConfigService(process.env)));
    await admin.query(`CREATE SCHEMA "${schema}"`);
    pool = new Pool({
      ...databasePoolConfig(new ConfigService(process.env)),
      options: `-c search_path=${schema}`,
    });
    await runMigrations(pool);
    const module = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(DatabaseService)
      .useValue({ pool })
      .overrideProvider(AuditConsumerService)
      .useValue({})
      .overrideProvider(LAUNCHDARKLY_CLIENT)
      .useValue(null)
      .overrideProvider(FeatureFlagService)
      .useValue({ isEnabled: jest.fn().mockResolvedValue(true) })
      .overrideProvider(AUTH0_CONFIG)
      .useValue(auth.config)
      .compile();
    app = module.createNestApplication();
    await app.init();
    repository = app.get(AuditRepository);
    const common = {
      tenantId,
      correlationId,
      actor: { id: 'user-1', email: 'user@example.com' },
    };
    const events = [
      auditEventFixture({
        ...common,
        eventId: 'b5-login',
        timestamp: '2026-10-04T00:00:00Z',
        resource: { type: 'session', id: 'session-1' },
        metadata: { severity: 'INFO' },
      }),
      auditEventFixture({
        ...common,
        eventId: 'b5-role',
        timestamp: '2026-10-04T02:00:00Z',
        eventType: 'USER_ROLE_CHANGED',
        resource: { type: 'user', id: 'user-1' },
        metadata: { severity: 'WARN' },
      }),
      auditEventFixture({
        ...common,
        actor: { id: 'analyst' },
        eventId: 'b5-export',
        timestamp: '2026-10-04T01:00:00Z',
        eventType: 'DATA_EXPORTED',
        resource: { type: 'report', id: 'report-1' },
        context: { service: 'reporting' },
        metadata: { severity: 'INFO' },
      }),
      auditEventFixture({
        ...common,
        actor: { id: 'admin' },
        correlationId: 'other-flow',
        eventId: 'b5-config',
        timestamp: '2026-10-04T03:00:00Z',
        eventType: 'CONFIG_CHANGED',
        resource: { type: 'config', id: 'config-1' },
        context: { service: 'configuration' },
        metadata: { severity: 'ERROR' },
      }),
      auditEventFixture({
        ...common,
        actor: { id: 'refund-operator' },
        correlationId: 'other-flow',
        eventId: 'b5-refund',
        timestamp: '2026-10-04T04:00:00Z',
        eventType: 'PAYMENT_REFUNDED',
        resource: { type: 'payment', id: 'payment-1' },
        context: { service: 'payments' },
        metadata: { severity: 'INFO' },
      }),
      auditEventFixture({
        ...common,
        tenantId: 'b5-tenant-b',
        eventId: 'b5-other-tenant',
        timestamp: '2026-10-04T00:00:00Z',
      }),
    ];
    for (const event of events) {
      const result = await repository.create(event);
      if (result.status !== 'created') throw new Error('Expected seeded event');
      stored.push(result.event);
    }
  }, 30000);

  afterAll(async () => {
    if (app) await app.close();
    await auth.close();
    if (pool) await pool.end();
    if (admin) {
      try {
        await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      } finally {
        await admin.end();
      }
    }
  });

  it.each([
    '/audit/events',
    '/audit/statistics',
    '/audit/timeline/b5-flow',
    '/audit/events/00000000-0000-4000-8000-000000000001',
  ])('requires authentication and read permission on %s', async (path) => {
    await request(app.getHttpServer()).get(path).expect(401);
    await request(app.getHttpServer())
      .get(path)
      .set('Authorization', 'Bearer broken')
      .expect(401);
    await request(app.getHttpServer())
      .get(path)
      .set(
        'Authorization',
        `Bearer ${auth.token(tenantId, { permissions: [], roles: ['AUDIT_ADMIN'] })}`,
      )
      .expect(403);
  });

  it('allows a viewer and isolates simultaneous tenants across all read endpoints', async () => {
    const a = await api().get('/audit/events').expect(200);
    const b = await request(app.getHttpServer())
      .get('/audit/events')
      .set(
        'Authorization',
        `Bearer ${auth.token('b5-tenant-b', { roles: ['AUDIT_VIEWER'] })}`,
      )
      .expect(200);
    expect(a.body.total).toBe(5);
    expect(b.body.items).toEqual([
      { ...stored[5], actor: { ...stored[5]!.actor, email: '[MASKED]' } },
    ]);
    const asB = (path: string) =>
      request(app.getHttpServer())
        .get(path)
        .set('Authorization', `Bearer ${auth.token('b5-tenant-b')}`);
    await asB(`/audit/events/${stored[0]!.id}`).expect(404);
    await asB(`/audit/events/${stored[5]!.id}`).expect(200);
    const timeline = await asB(`/audit/timeline/${correlationId}`).expect(200);
    expect(timeline.body.items).toEqual([
      { ...stored[5], actor: { ...stored[5]!.actor, email: '[MASKED]' } },
    ]);
    const stats = await asB('/audit/statistics').expect(200);
    expect(stats.body.total).toBe(1);
    const [parallelA, parallelB] = await Promise.all([
      api().get('/audit/events'),
      asB('/audit/events'),
    ]);
    expect(parallelA.body.total).toBe(5);
    expect(parallelB.body.total).toBe(1);
  });

  it('keeps health public and rejects a signed token without a trusted tenant', async () => {
    await request(app.getHttpServer()).get('/health').expect(200);
    await request(app.getHttpServer())
      .get('/audit/events')
      .set('X-Tenant-Id', tenantId)
      .set(
        'Authorization',
        `Bearer ${auth.token(tenantId, { [auth.config.tenantClaim]: undefined })}`,
      )
      .expect(401);
  });

  it('returns defaults and stable non-overlapping pages with a total', async () => {
    const all = await api().get('/audit/events').expect(200);
    expect(all.body).toMatchObject({
      total: 5,
      page: 1,
      limit: 25,
      totalPages: 1,
    });
    expect(all.body.items).toHaveLength(5);
    const page1 = await api()
      .get('/audit/events')
      .query({ page: 1, limit: 2 })
      .expect(200);
    const page2 = await api()
      .get('/audit/events')
      .query({ page: 2, limit: 2 })
      .expect(200);
    expect(page1.body.items).toEqual(all.body.items.slice(0, 2));
    expect(page2.body.items).toEqual(all.body.items.slice(2, 4));
    expect(page2.body).toMatchObject({
      total: 5,
      page: 2,
      limit: 2,
      totalPages: 3,
    });
    const beyond = await api()
      .get('/audit/events')
      .query({ page: 9, limit: 2 })
      .expect(200);
    expect(beyond.body).toMatchObject({ items: [], total: 5, totalPages: 3 });
  });

  it.each([
    [{ eventType: 'USER_LOGIN' }, 1],
    [{ actor: 'user-1' }, 2],
    [{ actor: 'user@example.com' }, 2],
    [{ resourceType: 'report' }, 1],
    [{ resourceId: 'payment-1' }, 1],
    [{ service: 'identity' }, 2],
    [{ severity: 'WARN' }, 1],
    [{ correlationId }, 3],
    [{ from: '2026-10-04T02:00:00Z' }, 3],
    [{ to: '2026-10-04T01:00:00Z' }, 2],
  ])('applies filter %j', async (query, count) => {
    const response = await api().get('/audit/events').query(query).expect(200);
    expect(response.body.total).toBe(count);
    expect(response.body.items).toHaveLength(count);
  });

  it('combines all filters with inclusive timestamp boundaries', async () => {
    const query = {
      eventType: 'USER_LOGIN',
      actor: 'user-1',
      resourceType: 'session',
      resourceId: 'session-1',
      service: 'identity',
      severity: 'INFO',
      correlationId,
      from: '2026-10-04T00:00:00Z',
      to: '2026-10-04T05:30:00+05:30',
    };
    const response = await api().get('/audit/events').query(query).expect(200);
    expect(response.body.total).toBe(1);
    expect(response.body.items[0].eventId).toBe('b5-login');
  });

  it('returns event details by database UUID and hides another tenant', async () => {
    await api()
      .get(`/audit/events/${stored[0]!.id}`)
      .expect(200)
      .expect({
        ...stored[0],
        actor: { ...stored[0]!.actor, email: '[MASKED]' },
      });
    await api().get(`/audit/events/${stored[5]!.id}`).expect(404);
    await api().get(`/audit/events/${randomUUID()}`).expect(404);
    await api().get('/audit/events/not-a-uuid').expect(400);
  });

  it('orders timelines by event timestamp and paginates them', async () => {
    const response = await api()
      .get(`/audit/timeline/${correlationId}`)
      .expect(200);
    expect(
      response.body.items.map((event: PersistedAuditEvent) => event.eventId),
    ).toEqual(['b5-login', 'b5-export', 'b5-role']);
    const page = await api()
      .get(`/audit/timeline/${correlationId}`)
      .query({ limit: 2, page: 2 })
      .expect(200);
    expect(page.body).toMatchObject({ total: 3, totalPages: 2 });
    expect(page.body.items[0].eventId).toBe('b5-role');
    const filtered = await api()
      .get(`/audit/timeline/${correlationId}`)
      .query({ service: 'reporting' })
      .expect(200);
    expect(filtered.body.total).toBe(1);
    const absent = await api().get('/audit/timeline/absent').expect(200);
    expect(absent.body).toMatchObject({ items: [], total: 0, totalPages: 0 });
    await api()
      .get(`/audit/timeline/${correlationId}`)
      .query({ correlationId: 'conflict' })
      .expect(400);
  });

  it('returns tenant-scoped and filtered statistics', async () => {
    const response = await api().get('/audit/statistics').expect(200);
    expect(response.body).toEqual({
      total: 5,
      byEventType: {
        USER_LOGIN: 1,
        USER_ROLE_CHANGED: 1,
        DATA_EXPORTED: 1,
        CONFIG_CHANGED: 1,
        PAYMENT_REFUNDED: 1,
      },
    });
    const filtered = await api()
      .get('/audit/statistics')
      .query({ from: '2026-10-04T01:00:00Z', to: '2026-10-04T02:00:00Z' })
      .expect(200);
    expect(filtered.body).toEqual({
      total: 2,
      byEventType: {
        USER_LOGIN: 0,
        USER_ROLE_CHANGED: 1,
        DATA_EXPORTED: 1,
        CONFIG_CHANGED: 0,
        PAYMENT_REFUNDED: 0,
      },
    });
  });

  it.each([
    '/audit/events?page=0',
    '/audit/events?limit=101',
    '/audit/events?actor=a&actor=b',
    '/audit/events?from=2026-02-30T00:00:00Z',
    '/audit/events?eventType=UNKNOWN',
    '/audit/events?tenantId=b5-tenant-b',
    '/audit/statistics?page=1',
    '/audit/events?from=2026-10-05T00:00:00Z&to=2026-10-04T00:00:00Z',
  ])('returns 400 for invalid DTO query %s', async (path) => {
    await api().get(path).expect(400);
  });

  it('ignores a client tenant header and parameterizes hostile filters', async () => {
    const response = await api()
      .get('/audit/events')
      .set('X-Tenant-Id', 'b5-tenant-b')
      .expect(200);
    expect(response.body.total).toBe(5);
    const hostile = await api()
      .get('/audit/events')
      .query({ actor: "' OR 1=1 --" })
      .expect(200);
    expect(hostile.body).toMatchObject({ items: [], total: 0 });
  });

  it('returns a safe 503 on repository failure without exposing driver details', async () => {
    const failure = jest
      .spyOn(repository, 'findPage')
      .mockRejectedValueOnce(new Error('must-not-be-exposed'));
    const log = jest
      .spyOn(Logger.prototype, 'error')
      .mockImplementation(() => {});
    try {
      const response = await api().get('/audit/events').expect(503);
      expect(JSON.stringify(response.body)).not.toContain(
        'must-not-be-exposed',
      );
      expect(JSON.stringify(log.mock.calls)).not.toContain(
        'must-not-be-exposed',
      );
    } finally {
      failure.mockRestore();
      log.mockRestore();
    }
  });

  it('masks REST detail/list/timeline by default and reveals only actor email with permission', async () => {
    const event = auditEventFixture({
      eventId: randomUUID(),
      tenantId,
      correlationId: randomUUID(),
      actor: { id: 'b10-user', email: 'sensitive@example.com' },
      changes: {
        before: { phone: '5551234' },
        after: { cardNumber: '4111111111111111' },
      },
      metadata: { email: 'private@example.com', phoneNumber: '5559876' },
    });
    const result = await repository.create(event);
    if (result.status !== 'created') throw new Error('Expected event');
    try {
      const detail = await api()
        .get(`/audit/events/${result.event.id}`)
        .set('X-Permissions', 'audit:view-sensitive')
        .expect(200);
      const list = await api()
        .get('/audit/events')
        .query({ correlationId: event.correlationId })
        .expect(200);
      const timeline = await api()
        .get(`/audit/timeline/${event.correlationId}`)
        .expect(200);
      for (const data of [
        detail.body,
        list.body.items[0],
        timeline.body.items[0],
      ]) {
        expect(data.actor.email).toBe('[MASKED]');
        expect(data.changes).toEqual({
          before: { phone: '[MASKED]' },
          after: { cardNumber: '[MASKED]' },
        });
        expect(data.metadata).toEqual({
          email: '[MASKED]',
          phoneNumber: '[MASKED]',
        });
      }
      const privileged = await request(app.getHttpServer())
        .get(`/audit/events/${result.event.id}`)
        .set(
          'Authorization',
          `Bearer ${auth.token(tenantId, { permissions: ['audit:read', 'audit:view-sensitive'] })}`,
        )
        .expect(200);
      expect(privileged.body.actor.email).toBe(event.actor.email);
      expect(privileged.headers['cache-control']).toBe('private, no-store');
      expect(privileged.headers.vary).toContain('Authorization');
      expect(privileged.body.changes).toEqual(detail.body.changes);
      expect(privileged.body.metadata).toEqual(detail.body.metadata);
      expect(
        (await repository.findByEventId(tenantId, event.eventId))!.changes,
      ).toEqual(event.changes);
      expect(
        (await repository.findByEventId(tenantId, event.eventId))!.actor.email,
      ).toBe(event.actor.email);
    } finally {
      await pool.query('DELETE FROM audit_events WHERE id=$1', [
        result.event.id,
      ]);
    }
  });

  it('refuses to expose prohibited credentials inserted outside the repository', async () => {
    const log = jest
      .spyOn(Logger.prototype, 'error')
      .mockImplementation(() => {});
    await pool.query('UPDATE audit_events SET metadata=$1::jsonb WHERE id=$2', [
      JSON.stringify({ password: 'must-not-be-exposed' }),
      stored[0]!.id,
    ]);
    try {
      const detail = await api()
        .get(`/audit/events/${stored[0]!.id}`)
        .expect(503);
      const list = await api().get('/audit/events').expect(503);
      const privileged = await request(app.getHttpServer())
        .get(`/audit/events/${stored[0]!.id}`)
        .set(
          'Authorization',
          `Bearer ${auth.token(tenantId, { permissions: ['audit:read', 'audit:view-sensitive'] })}`,
        )
        .expect(503);
      expect(
        JSON.stringify([
          detail.body,
          list.body,
          privileged.body,
          log.mock.calls,
        ]),
      ).not.toContain('must-not-be-exposed');
    } finally {
      await pool.query(
        'UPDATE audit_events SET metadata=$1::jsonb WHERE id=$2',
        [JSON.stringify(stored[0]!.metadata), stored[0]!.id],
      );
      log.mockRestore();
    }
  });
});
