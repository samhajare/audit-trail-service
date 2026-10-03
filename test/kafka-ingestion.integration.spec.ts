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
      .overrideProvider(KAFKA_CLIENT)
      .useValue(kafka)
      .compile();
    app = module.createNestApplication();
    repository = app.get(AuditRepository);
    await app.init();
  }

  beforeAll(async () => {
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
      topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      waitForLeaders: true,
    });
    producer = kafka.producer({ allowAutoTopicCreation: false });
    await producer.connect();
    await startApp();
  }, 60000);

  afterAll(async () => {
    try {
      if (app) await app.close();
      if (producer) await producer.disconnect();
      if (admin) {
        try {
          const { groups } = await admin.listGroups();
          if (groups.some((group) => group.groupId === groupId)) {
            await admin.deleteGroups([groupId]);
          }
        } finally {
          try {
            await admin.deleteTopics({ topics: [topic] });
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
  }, 60000);

  it('leaves a failed write uncommitted and resumes on operator restart', async () => {
    const failedEvent = auditEventFixture({
      eventId: `${suffix}-failed-write`,
    });
    const failure = jest
      .spyOn(repository, 'create')
      .mockRejectedValueOnce(new Error('simulated DB outage'));
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(failedEvent) }],
    });
    await waitFor(
      async () => crashed,
      'consumer stopping after persistence failure',
    );
    const offsets = await admin.fetchOffsets({ groupId, topics: [topic] });
    expect(offsets[0]?.partitions[0]?.offset).toBe(committedOffset);
    expect(
      await repository.findByEventId(failedEvent.tenantId, failedEvent.eventId),
    ).toBeNull();
    failure.mockRestore();
    await app.close();
    await startApp();
    await waitFor(
      async () =>
        (await repository.findByEventId(
          failedEvent.tenantId,
          failedEvent.eventId,
        )) !== null,
      'uncommitted event persisted after restart',
    );
    expect((await repository.getStatistics(failedEvent.tenantId)).total).toBe(
      3,
    );
  }, 60000);
});
