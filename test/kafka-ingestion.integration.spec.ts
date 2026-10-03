import {
  FeatureFlagService,
  LAUNCHDARKLY_CLIENT,
} from '../src/feature-flags/feature-flag.service';
import { randomUUID } from 'node:crypto';
import { INestApplication, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Test } from '@nestjs/testing';
import { Admin, Consumer, Kafka, logLevel, Producer } from 'kafkajs';
import { Pool } from 'pg';
import { AppModule } from '../src/app.module';
import { AuditRepository } from '../src/audit/audit.repository';
import { databasePoolConfig } from '../src/database/database.config';
import { DatabaseService } from '../src/database/database.service';
import { runMigrations } from '../src/database/migrate';
import {
  KAFKA_CLIENT,
  KAFKA_CONFIG,
  kafkaConfig,
  KafkaConfiguration,
} from '../src/kafka/kafka.config';
import { auditEventFixture } from './fixtures/audit-event.fixture';
import { generateEvents } from '../tools/simulator/events';
import { simulatorOptionsSchema } from '../tools/simulator/options';
import { publishEvents } from '../tools/simulator/publisher';
import { RETRY_CONFIG } from '../src/kafka/retry.config';
import { FailureEnvelope } from '../src/dlq/failure-envelope';
import request from 'supertest';
import { AUTH0_CONFIG } from '../src/auth/auth0.config';
import { Auth0Fixture } from './fixtures/auth0.fixture';
import { DlqRepository } from '../src/dlq/dlq.repository';
import { DlqReplayPublisher } from '../src/dlq/dlq-replay.publisher';

async function waitFor(predicate: () => Promise<boolean>, description: string) {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waiting for ${description}`);
}

describe('Kafka -> validation -> PostgreSQL', () => {
  const suffix = randomUUID().replace(/-/g, '');
  const schema = `b3_test_${suffix}`;
  const topic = `b3-test-${suffix}`;
  const groupId = `b3-test-${suffix}`;
  const retryTopic = `${topic}.retry`;
  const dlqTopic = `${topic}.dlq`;
  const failures: FailureEnvelope[] = [];
  const auth = new Auth0Fixture();
  let dlqConsumer: Consumer;
  let databaseAdmin: Pool;
  let pool: Pool;
  let admin: Admin;
  let producer: Producer;
  let kafka: Kafka;
  let consumer: Consumer;
  let config: KafkaConfiguration;
  let app: INestApplication;
  let repository: AuditRepository;
  let committedOffset: string;
  let crashed = false;

  async function startApp() {
    const module = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(DatabaseService)
      .useValue({ pool })
      .overrideProvider(KAFKA_CONFIG)
      .useValue(config)
      .overrideProvider(RETRY_CONFIG)
      .useValue({
        sourceTopic: topic,
        retryTopic,
        dlqTopic,
        maxRetries: 2,
        delayMs: 0,
      })
      .overrideProvider(KAFKA_CLIENT)
      .useValue(kafka)
      .overrideProvider(LAUNCHDARKLY_CLIENT)
      .useValue(null)
      .overrideProvider(FeatureFlagService)
      .useValue({ isEnabled: jest.fn().mockResolvedValue(true) })
      .overrideProvider(AUTH0_CONFIG)
      .useValue(auth.config)
      .compile();
    app = module.createNestApplication();
    repository = app.get(AuditRepository);
    await app.init();
  }

  beforeAll(async () => {
    await auth.start();
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'error').mockImplementation(() => {});
    const dbConfig = databasePoolConfig(new ConfigService(process.env));
    databaseAdmin = new Pool(dbConfig);
    await databaseAdmin.query(`CREATE SCHEMA "${schema}"`);
    pool = new Pool({ ...dbConfig, options: `-c search_path=${schema}` });
    await runMigrations(pool);
    config = {
      ...kafkaConfig(new ConfigService(process.env)),
      enabled: true,
      topic,
      groupId,
    };
    kafka = new Kafka({
      clientId: groupId,
      brokers: config.brokers,
      logLevel: logLevel.NOTHING,
      retry: { retries: 5 },
    });
    const createConsumer = kafka.consumer.bind(kafka);
    jest.spyOn(kafka, 'consumer').mockImplementation((options) => {
      consumer = createConsumer(options);
      consumer.on(consumer.events.CRASH, () => {
        crashed = true;
      });
      return consumer;
    });
    admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
      topics: [topic, retryTopic, dlqTopic].map((topic) => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1,
      })),
      waitForLeaders: true,
    });
    producer = kafka.producer({ allowAutoTopicCreation: false });
    await producer.connect();
    dlqConsumer = createConsumer({ groupId: `${groupId}-dlq` });
    await dlqConsumer.connect();
    await dlqConsumer.subscribe({ topic: dlqTopic, fromBeginning: true });
    await dlqConsumer.run({
      eachMessage: async ({ message }) => {
        failures.push(JSON.parse(message.value!.toString()) as FailureEnvelope);
      },
    });
    await startApp();
  }, 60000);

  afterAll(async () => {
    try {
      if (app) await app.close();
      await auth.close();
      if (producer) await producer.disconnect();
      if (dlqConsumer) await dlqConsumer.disconnect();
      if (admin) {
        try {
          const { groups } = await admin.listGroups();
          if (groups.some((group) => group.groupId === groupId)) {
            await admin.deleteGroups([groupId, `${groupId}-dlq`]);
          }
        } finally {
          try {
            await admin.deleteTopics({ topics: [topic, retryTopic, dlqTopic] });
          } finally {
            await admin.disconnect();
          }
        }
      }
    } finally {
      if (pool) await pool.end();
      if (databaseAdmin) {
        try {
          await databaseAdmin.query(
            `DROP SCHEMA IF EXISTS "${schema}" CASCADE`,
          );
        } finally {
          await databaseAdmin.end();
        }
      }
      jest.restoreAllMocks();
    }
  }, 30000);

  it('persists a valid event, ignores duplicates, and continues after invalid messages', async () => {
    const event = auditEventFixture({ eventId: `${suffix}-event` });
    const marker = auditEventFixture({
      eventId: `${suffix}-marker`,
      eventType: 'DATA_EXPORTED',
    });
    const messages = [
      { value: JSON.stringify(event) },
      { value: JSON.stringify({ ...event, action: 'must-not-overwrite' }) },
      { value: '{malformed' },
      { value: null },
      {
        value: JSON.stringify({
          ...event,
          eventId: 'bad-version',
          schemaVersion: '2.0',
        }),
      },
      {
        value: JSON.stringify({
          ...event,
          eventId: 'bad-credentials',
          metadata: { password: 'must-never-be-logged' },
        }),
      },
      {
        value: JSON.stringify({
          ...event,
          eventId: 'bad-type',
          eventType: 'UNKNOWN',
        }),
      },
      {
        value: JSON.stringify({
          ...event,
          eventId: 'missing-tenant',
          tenantId: undefined,
        }),
      },
      { value: JSON.stringify(marker) },
    ];
    const sent = await producer.send({ topic, messages });
    const baseOffset = sent[0]?.baseOffset;
    if (baseOffset === undefined)
      throw new Error('Producer returned no offset');
    committedOffset = (BigInt(baseOffset) + BigInt(messages.length)).toString();
    await waitFor(async () => {
      const offsets = await admin.fetchOffsets({ groupId, topics: [topic] });
      return offsets[0]?.partitions[0]?.offset === committedOffset;
    }, 'committed marker offset');
    expect((await repository.getStatistics(event.tenantId)).total).toBe(2);
    expect(
      (await repository.findByEventId(event.tenantId, event.eventId))?.action,
    ).toBe('login');
    expect(
      await repository.findByEventId('other-tenant', event.eventId),
    ).toBeNull();
    expect(
      await repository.findByEventId(event.tenantId, marker.eventId),
    ).not.toBeNull();
    expect(crashed).toBe(false);
    expect(Logger.prototype.log).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'persisted',
        eventId: event.eventId,
        tenantId: event.tenantId,
        correlationId: event.correlationId,
        eventType: event.eventType,
        service: 'identity',
      }),
    );
    expect(Logger.prototype.log).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'duplicate' }),
    );
    expect(
      JSON.stringify(jest.mocked(Logger.prototype.warn).mock.calls),
    ).not.toContain('must-never-be-logged');
    await waitFor(
      async () => failures.length === 6,
      'invalid messages routed to DLQ',
    );
    expect(JSON.stringify(failures)).not.toContain('must-never-be-logged');
  }, 60000);

  it('retries a transient write failure through Kafka and persists once', async () => {
    const failedEvent = auditEventFixture({
      eventId: `${suffix}-failed-write`,
    });
    const failure = jest.spyOn(repository, 'create').mockRejectedValueOnce(
      Object.assign(new Error('simulated DB outage'), {
        code: 'ECONNREFUSED',
      }),
    );
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(failedEvent) }],
    });
    await waitFor(
      async () =>
        (await repository.findByEventId(
          failedEvent.tenantId,
          failedEvent.eventId,
        )) !== null,
      'event persisted through retry topic',
    );
    expect((await repository.getStatistics(failedEvent.tenantId)).total).toBe(
      3,
    );
    expect(failure).toHaveBeenCalledTimes(2);
    failure.mockRestore();
    expect(crashed).toBe(false);
  }, 60000);
  it('routes exhausted transient retries to DLQ with original failure metadata', async () => {
    const event = auditEventFixture({ eventId: `${suffix}-exhausted` });
    const realCreate = repository.create.bind(repository);
    const failure = jest
      .spyOn(repository, 'create')
      .mockImplementation((payload) =>
        (payload as { eventId?: string }).eventId === event.eventId
          ? Promise.reject(
              Object.assign(new Error('private driver details'), {
                code: 'ETIMEDOUT',
              }),
            )
          : realCreate(payload),
      );
    try {
      await producer.send({
        topic,
        messages: [{ value: JSON.stringify(event) }],
      });
      await waitFor(
        async () =>
          failures.some(
            (item) =>
              (item.originalEvent as { eventId?: string })?.eventId ===
              event.eventId,
          ),
        'exhausted retries DLQ',
      );
      const envelope = failures.find(
        (item) =>
          (item.originalEvent as { eventId?: string })?.eventId ===
          event.eventId,
      )!;
      expect(envelope).toMatchObject({
        originalEvent: event,
        failureReason: 'transient_persistence',
        retryCount: 2,
        sourceTopic: topic,
        correlationId: event.correlationId,
      });
      expect(Number.isFinite(Date.parse(envelope.failedAt))).toBe(true);
      expect(failure).toHaveBeenCalledTimes(3);
      expect(
        await repository.findByEventId(event.tenantId, event.eventId),
      ).toBeNull();
    } finally {
      failure.mockRestore();
    }
  }, 60000);
  it('indexes tenant-scoped DLQ records and authorizes one durable replay through Kafka', async () => {
    const tenant = 'b9-tenant-a';
    const event = auditEventFixture({
      eventId: `${suffix}-replay`,
      tenantId: tenant,
      actor: { id: 'replay-user', email: 'replay@example.com' },
      metadata: { phone: '5551234' },
    });
    const envelope: FailureEnvelope = {
      originalEvent: event,
      failureReason: 'transient_persistence',
      retryCount: 2,
      failedAt: new Date().toISOString(),
      sourceTopic: topic,
      correlationId: event.correlationId,
    };
    await producer.send({
      topic: dlqTopic,
      messages: [
        { value: JSON.stringify(envelope) },
        {
          value: JSON.stringify({
            ...envelope,
            originalEvent: {
              ...event,
              eventId: `${suffix}-tenant-b`,
              tenantId: 'b9-tenant-b',
            },
          }),
        },
      ],
    });
    const dlq = app.get(DlqRepository);
    await waitFor(
      async () => !!(await dlq.detail(tenant, event.eventId)),
      'durable DLQ index',
    );
    const header = (permissions = ['audit:read']) =>
      `Bearer ${auth.token(tenant, { permissions })}`;
    const path = `/audit/dlq/${event.eventId}`;
    await request(app.getHttpServer()).get('/audit/dlq').expect(401);
    await request(app.getHttpServer())
      .get(path)
      .set('Authorization', header([]))
      .expect(403);
    const page = await request(app.getHttpServer())
      .get('/audit/dlq?limit=1')
      .set('Authorization', header())
      .expect(200);
    expect(page.body).toMatchObject({
      total: 1,
      page: 1,
      limit: 1,
      totalPages: 1,
    });
    expect(page.body.items[0].envelope).toEqual({
      ...envelope,
      originalEvent: {
        ...event,
        actor: { ...event.actor, email: '[MASKED]' },
        metadata: { phone: '[MASKED]' },
      },
    });
    const privilegedDetail = await request(app.getHttpServer())
      .get(path)
      .set('Authorization', header(['audit:read', 'audit:view-sensitive']))
      .expect(200);
    expect(privilegedDetail.body.envelope.originalEvent.actor.email).toBe(
      event.actor.email,
    );
    expect(privilegedDetail.body.envelope.originalEvent.metadata.phone).toBe(
      '[MASKED]',
    );
    const maskedDetail = await request(app.getHttpServer())
      .get(path)
      .set('Authorization', header())
      .expect(200);
    expect(maskedDetail.body.envelope.originalEvent.actor.email).toBe(
      '[MASKED]',
    );
    await request(app.getHttpServer())
      .get(path)
      .set('Authorization', `Bearer ${auth.token('b9-tenant-b')}`)
      .expect(404);
    await request(app.getHttpServer())
      .get('/audit/dlq?tenantId=b9-tenant-b')
      .set('Authorization', header())
      .expect(400);
    await request(app.getHttpServer())
      .post(`${path}/replay`)
      .set('Authorization', header())
      .send({})
      .expect(403);
    await request(app.getHttpServer()).post(`${path}/replay`).expect(401);
    await request(app.getHttpServer())
      .post(`${path}/replay`)
      .set(
        'Authorization',
        `Bearer ${auth.token('b9-tenant-b', { permissions: ['audit:read', 'audit:replay'] })}`,
      )
      .send({})
      .expect(404);
    await request(app.getHttpServer())
      .post(`${path}/replay`)
      .set('Authorization', header(['audit:read', 'audit:replay']))
      .send({ tenantId: 'b9-tenant-b' })
      .expect(400);
    expect(await repository.findByEventId(tenant, event.eventId)).toBeNull();
    const flags = app.get(FeatureFlagService);
    const flagSpy = jest.spyOn(flags, 'isEnabled').mockResolvedValue(false);
    await request(app.getHttpServer())
      .post(`${path}/replay`)
      .set('Authorization', header(['audit:read', 'audit:replay']))
      .send({})
      .expect(403);
    expect((await dlq.detail(tenant, event.eventId))!.replayStatus).toBe(
      'pending',
    );
    const masked = await request(app.getHttpServer())
      .get(path)
      .set('Authorization', header(['audit:read', 'audit:view-sensitive']))
      .expect(200);
    expect(masked.body.envelope.originalEvent.actor.email).toBe('[MASKED]');
    flagSpy.mockResolvedValue(true);

    const replay = () =>
      request(app.getHttpServer())
        .post(`${path}/replay`)
        .set('Authorization', header(['audit:read', 'audit:replay']))
        .send({});
    const responses = await Promise.all([replay(), replay()]);
    expect(responses.map((response) => response.status).sort()).toEqual([
      202, 409,
    ]);
    await waitFor(
      async () => !!(await repository.findByEventId(tenant, event.eventId)),
      'replay persisted through ingestion',
    );
    expect((await repository.getStatistics(tenant)).total).toBe(1);
    expect(
      (await repository.findByEventId(tenant, event.eventId))!.actor.email,
    ).toBe(event.actor.email);
    expect(
      (await repository.findByEventId(tenant, event.eventId))!.metadata.phone,
    ).toBe(event.metadata.phone);
    const audit = await pool.query(
      'SELECT * FROM audit_dlq_replays WHERE tenant_id=$1 AND event_id=$2',
      [tenant, event.eventId],
    );
    expect(audit.rows).toHaveLength(1);
    expect(audit.rows[0]).toMatchObject({
      actor_id: 'viewer',
      status: 'published',
    });
    expect(Logger.prototype.log).toHaveBeenCalledWith(
      expect.objectContaining({
        message: 'DLQ replay published',
        tenantId: tenant,
        eventId: event.eventId,
      }),
    );
    const duplicate = await producer.send({
      topic: dlqTopic,
      messages: [{ value: JSON.stringify(envelope) }],
    });
    await waitFor(async () => {
      const offsets = await admin.fetchOffsets({ groupId, topics: [dlqTopic] });
      return (
        offsets[0]?.partitions[0]?.offset ===
        (BigInt(duplicate[0]!.baseOffset!) + 1n).toString()
      );
    }, 'duplicate DLQ delivery indexed without resetting replay state');
    await replay().expect(409);
    expect((await dlq.detail(tenant, event.eventId))!.replayStatus).toBe(
      'published',
    );
  }, 60000);
  it('blocks invalid and failed replay attempts without exposing errors or creating loops', async () => {
    const tenant = 'b9-failure-tenant';
    const event = auditEventFixture({
      tenantId: tenant,
      eventId: `${suffix}-publish-failure`,
    });
    const envelope = {
      originalEvent: event,
      failureReason: 'permanent_persistence',
      retryCount: 0,
      failedAt: new Date().toISOString(),
      sourceTopic: topic,
      correlationId: event.correlationId,
    };
    await producer.send({
      topic: dlqTopic,
      messages: [
        { value: JSON.stringify(envelope) },
        {
          value: JSON.stringify({
            ...envelope,
            originalEvent: {
              ...event,
              eventId: `${suffix}-invalid-replay`,
              schemaVersion: '2.0',
            },
          }),
        },
      ],
    });
    await waitFor(
      async () =>
        !!(await app
          .get(DlqRepository)
          .detail(tenant, `${suffix}-invalid-replay`)),
      'failed replay fixtures indexed',
    );
    const post = (eventId: string) =>
      request(app.getHttpServer())
        .post(`/audit/dlq/${eventId}/replay`)
        .set(
          'Authorization',
          `Bearer ${auth.token(tenant, { permissions: ['audit:read', 'audit:replay'] })}`,
        )
        .send({});
    await post(`${suffix}-invalid-replay`).expect(409);
    const failure = jest
      .spyOn(app.get(DlqReplayPublisher), 'publish')
      .mockRejectedValueOnce(new Error('private-broker-details'));
    try {
      const response = await post(event.eventId).expect(503);
      expect(JSON.stringify(response.body)).not.toContain(
        'private-broker-details',
      );
      await post(event.eventId).expect(409);
      expect(failure).toHaveBeenCalledTimes(1);
      expect(
        (await app.get(DlqRepository).detail(tenant, event.eventId))!
          .replayStatus,
      ).toBe('failed');
    } finally {
      failure.mockRestore();
    }
  }, 60000);
  it('persists simulator batches, all event types, correlated flow, and duplicate delivery through Kafka', async () => {
    const tenant = `b4-${suffix}`;
    const singleEvents = [
      'USER_LOGIN',
      'USER_ROLE_CHANGED',
      'DATA_EXPORTED',
      'CONFIG_CHANGED',
      'PAYMENT_REFUNDED',
    ].flatMap((type) =>
      generateEvents(simulatorOptionsSchema.parse({ tenant, type })),
    );
    const hundred = generateEvents(
      simulatorOptionsSchema.parse({ tenant, count: 100 }),
    );
    const thousand = generateEvents(
      simulatorOptionsSchema.parse({ tenant, count: 1000 }),
    );
    const flow = generateEvents(
      simulatorOptionsSchema.parse({ tenant, scenario: 'correlated' }),
    );
    const duplicate = generateEvents(
      simulatorOptionsSchema.parse({ tenant, scenario: 'duplicate' }),
    );
    const simulatorProducer = kafka.producer({ allowAutoTopicCreation: false });
    const all = [
      ...singleEvents,
      ...hundred,
      ...thousand,
      ...flow,
      ...duplicate,
    ];
    expect(await publishEvents(simulatorProducer, topic, all)).toBe(1112);
    await waitFor(
      async () => (await repository.getStatistics(tenant)).total === 1111,
      'all simulator messages persisted with duplicate ignored',
    );
    const timeline = await repository.findByCorrelationId(
      tenant,
      flow[0]!.correlationId,
    );
    expect(timeline.map((event) => event.eventId)).toEqual(
      flow.map((event) => event.eventId),
    );
    expect(
      await repository.findByEventId(tenant, duplicate[0]!.eventId),
    ).not.toBeNull();
    expect((await repository.getStatistics(tenant)).byEventType).toEqual({
      USER_LOGIN: 223,
      USER_ROLE_CHANGED: 222,
      DATA_EXPORTED: 222,
      CONFIG_CHANGED: 222,
      PAYMENT_REFUNDED: 222,
    });
  }, 60000);
  it('routes the simulator DLQ scenario through Kafka without persisting it', async () => {
    const events = generateEvents(
      simulatorOptionsSchema.parse({ scenario: 'dlq', tenant: `b8-${suffix}` }),
    );
    await publishEvents(
      kafka.producer({ allowAutoTopicCreation: false }),
      topic,
      events,
      undefined,
      'dlq',
    );
    await waitFor(
      async () =>
        failures.some(
          (item) =>
            (item.originalEvent as { eventId?: string })?.eventId ===
            events[0]!.eventId,
        ),
      'simulator DLQ message',
    );
    expect(
      failures.find(
        (item) =>
          (item.originalEvent as { eventId?: string })?.eventId ===
          events[0]!.eventId,
      ),
    ).toMatchObject({
      failureReason: 'unsupported_schema_version',
      retryCount: 0,
    });
    expect(
      await repository.findByEventId(events[0]!.tenantId, events[0]!.eventId),
    ).toBeNull();
  }, 60000);
});
