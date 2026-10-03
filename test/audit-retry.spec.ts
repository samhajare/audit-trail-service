import { DlqRepository } from '../src/dlq/dlq.repository';
import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka } from 'kafkajs';
import { AuditIngestionService } from '../src/kafka/audit-ingestion.service';
import {
  AuditPersistenceError,
  isTransientPersistenceError,
} from '../src/kafka/audit-persistence-error';
import { AuditRetryService } from '../src/kafka/audit-retry.service';
import { retryConfig } from '../src/kafka/retry.config';
import { auditEventFixture } from './fixtures/audit-event.fixture';

describe('Bounded Kafka retry and DLQ routing', () => {
  const config = {
    sourceTopic: 'audit.events',
    retryTopic: 'audit.events.retry',
    dlqTopic: 'audit.events.dlq',
    maxRetries: 2,
    delayMs: 0,
  };
  const location = { topic: config.sourceTopic, partition: 0, offset: '4' };
  const event = auditEventFixture();
  const producer = {
    connect: jest.fn(),
    disconnect: jest.fn(),
    send: jest.fn(),
  };
  const ingestion = { handle: jest.fn() };
  let service: AuditRetryService;
  beforeEach(() => {
    Object.values(producer).forEach((mock) =>
      mock.mockReset().mockResolvedValue(undefined),
    );
    ingestion.handle.mockReset().mockResolvedValue('persisted');
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    service = new AuditRetryService(
      { producer: () => producer } as unknown as Kafka,
      config,
      ingestion as unknown as AuditIngestionService,
      { store: jest.fn() } as unknown as DlqRepository,
    );
  });
  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
  });
  const payload = (value: unknown) => Buffer.from(JSON.stringify(value));
  const lastEnvelope = () =>
    JSON.parse(producer.send.mock.calls.at(-1)![0].messages[0].value);
  it.each(['persisted', 'duplicate'])(
    'does not route %s events',
    async (status) => {
      ingestion.handle.mockResolvedValue(status);
      await service.handle(payload(event), location);
      expect(producer.send).not.toHaveBeenCalled();
    },
  );
  it('retries exactly twice before DLQ, retaining metadata and event', async () => {
    ingestion.handle.mockRejectedValue(new AuditPersistenceError(true));
    await service.handle(payload(event), location);
    expect(producer.send).toHaveBeenLastCalledWith(
      expect.objectContaining({ topic: config.retryTopic, acks: -1 }),
    );
    expect(lastEnvelope()).toMatchObject({
      originalEvent: event,
      retryCount: 1,
      failureReason: 'transient_persistence',
      sourceTopic: config.sourceTopic,
      correlationId: event.correlationId,
    });
    await service.handle(payload(lastEnvelope()), {
      ...location,
      topic: config.retryTopic,
    });
    expect(lastEnvelope().retryCount).toBe(2);
    await service.handle(payload(lastEnvelope()), {
      ...location,
      topic: config.retryTopic,
    });
    expect(producer.send).toHaveBeenLastCalledWith(
      expect.objectContaining({ topic: config.dlqTopic }),
    );
    expect(lastEnvelope()).toMatchObject({
      originalEvent: event,
      retryCount: 2,
    });
    expect(ingestion.handle).toHaveBeenCalledTimes(3);
  });
  it('routes permanent persistence failures without retry', async () => {
    ingestion.handle.mockRejectedValue(new AuditPersistenceError(false));
    await service.handle(payload(event), location);
    expect(lastEnvelope()).toMatchObject({
      failureReason: 'permanent_persistence',
      retryCount: 0,
    });
    expect(producer.send.mock.calls[0]![0].topic).toBe(config.dlqTopic);
  });
  it.each([
    [{ ...event, schemaVersion: '2.0' }, 'unsupported_schema_version'],
    [{ ...event, tenantId: undefined }, 'invalid_schema'],
    [{ ...event, eventType: 'UNKNOWN' }, 'invalid_schema'],
  ])(
    'routes invalid payload directly to DLQ with %s',
    async (value, reason) => {
      await service.handle(payload(value), location);
      expect(ingestion.handle).not.toHaveBeenCalled();
      expect(lastEnvelope()).toMatchObject({
        originalEvent: JSON.parse(JSON.stringify(value)),
        failureReason: reason,
        retryCount: 0,
      });
    },
  );
  it.each([null, Buffer.from('{invalid'), Buffer.from([255])])(
    'routes malformed data without retaining raw bytes',
    async (value) => {
      await service.handle(value, location);
      expect(lastEnvelope()).toMatchObject({
        originalEvent: null,
        originalPayloadOmitted: true,
        failureReason: 'malformed_json',
      });
      expect(lastEnvelope().originalPayloadSha256).toMatch(/^[a-f0-9]{64}$/);
      expect(ingestion.handle).not.toHaveBeenCalled();
    },
  );
  it('never stores credential-bearing original events or logs their values', async () => {
    await service.handle(
      payload({ ...event, metadata: { 'Access-Token': 'never-retain-this' } }),
      location,
    );
    expect(lastEnvelope()).toMatchObject({
      originalEvent: null,
      failureReason: 'prohibited_credentials',
      originalPayloadOmitted: true,
    });
    expect(
      JSON.stringify([
        producer.send.mock.calls,
        jest.mocked(Logger.prototype.warn).mock.calls,
      ]),
    ).not.toContain('never-retain-this');
  });
  it.each([{}, { retryCount: -1 }, { retryCount: 99 }])(
    'rejects poisoned retry envelopes %j without a loop',
    async (value) => {
      await service.handle(payload(value), {
        ...location,
        topic: config.retryTopic,
      });
      expect(lastEnvelope().failureReason).toBe('invalid_retry_envelope');
      expect(producer.send.mock.calls[0]![0].topic).toBe(config.dlqTopic);
      expect(ingestion.handle).not.toHaveBeenCalled();
    },
  );
  it('propagates failed routing safely so offsets remain uncommitted', async () => {
    producer.send.mockRejectedValue(new Error('private-broker-details'));
    await expect(
      service.handle(Buffer.from('{invalid'), location),
    ).rejects.toThrow('Audit failure routing failed');
  });
  it('waits for retry due time while heartbeating', async () => {
    jest.useFakeTimers();
    const heartbeat = jest.fn().mockResolvedValue(undefined);
    const delayed = new AuditRetryService(
      { producer: () => producer } as unknown as Kafka,
      { ...config, delayMs: 2000 },
      ingestion as unknown as AuditIngestionService,
      { store: jest.fn() } as unknown as DlqRepository,
    );
    const promise = delayed.handle(
      payload({
        originalEvent: event,
        failureReason: 'transient_persistence',
        retryCount: 1,
        failedAt: new Date().toISOString(),
        retryAt: new Date(Date.now() + 2000).toISOString(),
        sourceTopic: config.sourceTopic,
        correlationId: event.correlationId,
      }),
      { ...location, topic: config.retryTopic },
      heartbeat,
    );
    expect(ingestion.handle).not.toHaveBeenCalled();
    await jest.advanceTimersByTimeAsync(2000);
    await promise;
    expect(heartbeat).toHaveBeenCalledTimes(2);
    expect(ingestion.handle).toHaveBeenCalledTimes(1);
  });
});

describe('Transient error classification', () => {
  it.each([
    '08006',
    '40001',
    '40P01',
    '57P01',
    '57014',
    'ECONNREFUSED',
    'ETIMEDOUT',
    'ECONNRESET',
  ])('retries known transient code %s', (code) =>
    expect(isTransientPersistenceError({ code })).toBe(true),
  );
  it.each([
    new Error('unexpected failure'),
    { code: '23514' },
    { code: '42501' },
    { code: '42P01' },
    null,
  ])('does not retry unknown or permanent errors', (error) =>
    expect(isTransientPersistenceError(error)).toBe(false),
  );
});

describe('Retry configuration', () => {
  function config(values: Record<string, unknown>) {
    const service = new ConfigService();
    jest.spyOn(service, 'get').mockImplementation((key) => values[String(key)]);
    return service;
  }
  it('uses three retries and separate topics by default', () =>
    expect(retryConfig(config({}))).toEqual({
      sourceTopic: 'audit.events',
      retryTopic: 'audit.events.retry',
      dlqTopic: 'audit.events.dlq',
      maxRetries: 3,
      delayMs: 1000,
    }));
  it.each([
    { KAFKA_MAX_RETRIES: -1 },
    { KAFKA_MAX_RETRIES: 11 },
    { KAFKA_RETRY_DELAY_MS: 10001 },
    { KAFKA_RETRY_TOPIC: 'audit.events' },
    { KAFKA_DLQ_TOPIC: 'bad topic' },
  ])('rejects invalid settings %j', (values) =>
    expect(() => retryConfig(config(values))).toThrow(
      'Invalid Kafka retry configuration',
    ),
  );
});
