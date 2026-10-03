import {
  FeatureFlagService,
  LAUNCHDARKLY_CLIENT,
} from '../src/feature-flags/feature-flag.service';
import { INestApplication, Logger } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import { firstValueFrom, take, toArray } from 'rxjs';
import { AppModule } from '../src/app.module';
import { AuditRepository } from '../src/audit/audit.repository';
import { AUTH0_CONFIG } from '../src/auth/auth0.config';
import { AuditConsumerService } from '../src/kafka/audit-consumer.service';
import { AuditIngestionService } from '../src/kafka/audit-ingestion.service';
import { AuditEventBus } from '../src/realtime/audit-event-bus';
import { AuditStreamController } from '../src/realtime/audit-stream.controller';
import { Auth0Fixture } from './fixtures/auth0.fixture';
import { auditEventFixture } from './fixtures/audit-event.fixture';

describe('Authenticated SSE delivery', () => {
  const auth = new Auth0Fixture();
  const create = jest.fn();
  let app: INestApplication;
  let url: string;
  let ingestion: AuditIngestionService;
  beforeAll(async () => {
    await auth.start();
    const module = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(LAUNCHDARKLY_CLIENT)
      .useValue(null)
      .overrideProvider(FeatureFlagService)
      .useValue({ isEnabled: jest.fn().mockResolvedValue(true) })
      .overrideProvider(AUTH0_CONFIG)
      .useValue(auth.config)
      .overrideProvider(AuditConsumerService)
      .useValue({})
      .overrideProvider(AuditRepository)
      .useValue({ create })
      .compile();
    app = module.createNestApplication();
    await app.listen(0, '127.0.0.1');
    url = await app.getUrl();
    ingestion = app.get(AuditIngestionService);
  });
  afterAll(async () => {
    await app?.close();
    await auth.close();
  });
  beforeEach(() => {
    create.mockReset();
    jest.mocked(app.get(FeatureFlagService).isEnabled).mockResolvedValue(true);
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  it('returns 401/403 before opening a stream and rejects tenant query overrides', async () => {
    await request(app.getHttpServer()).get('/audit/stream').expect(401);
    await request(app.getHttpServer())
      .get('/audit/stream')
      .set(
        'Authorization',
        `Bearer ${auth.token('tenant-a', { permissions: [] })}`,
      )
      .expect(403);
    await request(app.getHttpServer())
      .get('/audit/stream?tenantId=tenant-b')
      .set('Authorization', `Bearer ${auth.token('tenant-a')}`)
      .expect(400);
  });

  it('blocks a disabled stream before subscribing', async () => {
    const flags = app.get(FeatureFlagService);
    jest.spyOn(flags, 'isEnabled').mockResolvedValue(false);
    await request(app.getHttpServer())
      .get('/audit/stream')
      .set('Authorization', `Bearer ${auth.token('tenant-a')}`)
      .expect(403);
  });

  it.each([false, true])(
    'streams only committed tenant events with permission-controlled masking (%s)',
    async (privileged) => {
      const abort = new AbortController();
      const response = await fetch(`${url}/audit/stream`, {
        headers: {
          Authorization: `Bearer ${auth.token('tenant-a', { permissions: privileged ? ['audit:read', 'audit:view-sensitive'] : ['audit:read'] })}`,
          'X-Tenant-Id': 'tenant-b',
        },
        signal: abort.signal,
      });
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain(
        'text/event-stream',
      );
      const reader = response.body!.getReader();
      const event = {
        ...auditEventFixture({
          tenantId: 'tenant-a',
          eventId: 'stream-event',
          actor: { id: 'user-1', email: 'stream@example.com' },
        }),
        id: 'database-id',
        createdAt: new Date().toISOString(),
      };
      const location = { topic: 'audit.events', partition: 0, offset: '0' };
      try {
        create.mockResolvedValueOnce({
          status: 'created',
          event: { ...event, tenantId: 'tenant-b', eventId: 'other-tenant' },
        });
        await ingestion.handle(
          Buffer.from(
            JSON.stringify({
              ...event,
              id: undefined,
              createdAt: undefined,
              tenantId: 'tenant-b',
            }),
          ),
          location,
        );
        create.mockResolvedValueOnce({ status: 'duplicate' });
        await ingestion.handle(
          Buffer.from(JSON.stringify(auditEventFixture())),
          location,
        );
        create.mockResolvedValueOnce({ status: 'created', event });
        await ingestion.handle(
          Buffer.from(
            JSON.stringify(auditEventFixture({ tenantId: 'tenant-a' })),
          ),
          location,
        );
        let text = '';
        while (!text.includes('event: audit-event')) {
          const chunk = await reader.read();
          if (chunk.done) throw new Error('Stream closed early');
          text += new TextDecoder().decode(chunk.value);
        }
        expect(text).not.toContain('other-tenant');
        expect(text).toContain('retry: 3000');
        expect(text).toContain('id: "stream-event"');
        const line = text
          .split('\n')
          .find((value) => value.startsWith('data: '))!;
        expect(JSON.parse(line.slice(6))).toEqual({
          eventId: event.eventId,
          eventType: event.eventType,
          timestamp: event.timestamp,
          tenantId: event.tenantId,
          correlationId: event.correlationId,
          actor: {
            ...event.actor,
            email: privileged ? event.actor.email : '[MASKED]',
          },
          resource: event.resource,
          action: event.action,
        });
        expect(event.actor.email).toBe('stream@example.com');
      } finally {
        abort.abort();
        await reader.cancel().catch(() => {});
      }
      const reconnect = new AbortController();
      try {
        const resumed = await fetch(`${url}/audit/stream`, {
          headers: {
            Authorization: `Bearer ${auth.token('tenant-a')}`,
            'Last-Event-ID': '"stream-event"',
          },
          signal: reconnect.signal,
        });
        expect(resumed.status).toBe(200);
      } finally {
        reconnect.abort();
      }
    },
  );
});

describe('Stream lifecycle', () => {
  afterEach(() => jest.useRealTimers());
  it('sends heartbeats and closes at token expiration', async () => {
    jest.useFakeTimers();
    const bus = new AuditEventBus();
    const controller = new AuditStreamController(bus);
    const result = firstValueFrom(
      controller
        .stream(
          'tenant-a',
          {
            headers: {},
            principal: {
              subject: 'viewer',
              tenantId: 'tenant-a',
              permissions: ['audit:read'],
              expiresAt: Date.now() / 1000 + 16,
            },
          },
          {},
        )
        .pipe(toArray()),
    );
    await jest.advanceTimersByTimeAsync(16000);
    expect(await result).toEqual([
      expect.objectContaining({ type: 'heartbeat', retry: 3000 }),
    ]);
  });
  it('filters tenants and completes bus subscribers on shutdown', async () => {
    const bus = new AuditEventBus();
    const values = firstValueFrom(
      bus.forTenant('tenant-a').pipe(take(1), toArray()),
    );
    const event = {
      ...auditEventFixture({ tenantId: 'tenant-a' }),
      id: 'id',
      createdAt: new Date().toISOString(),
    };
    bus.publish({ ...event, tenantId: 'tenant-b' });
    bus.publish(event);
    expect(await values).toEqual([event]);
    const completion = firstValueFrom(
      bus.forTenant('tenant-a').pipe(toArray()),
    );
    bus.onApplicationShutdown();
    expect(await completion).toEqual([]);
  });
});
