import { Logger } from '@nestjs/common';
import { Consumer, EachMessagePayload, Kafka } from 'kafkajs';
import { AuditConsumerService } from '../src/kafka/audit-consumer.service';
import { AuditRetryService } from '../src/kafka/audit-retry.service';
import { KafkaConfiguration } from '../src/kafka/kafka.config';

describe('Kafka consumer offsets and lifecycle', () => {
  const config: KafkaConfiguration = {
    enabled: true,
    brokers: ['localhost:9092'],
    clientId: 'test',
    groupId: 'test',
    topic: 'audit.events',
  };
  const retry = {
    sourceTopic: config.topic,
    retryTopic: config.topic + '.retry',
    dlqTopic: config.topic + '.dlq',
    maxRetries: 3,
    delayMs: 0,
  };
  const payload: EachMessagePayload = {
    topic: 'audit.events',
    partition: 0,
    message: {
      key: null,
      value: Buffer.from('{}'),
      timestamp: '0',
      attributes: 0,
      offset: '9007199254740993',
      headers: {},
    },
    heartbeat: jest.fn(),
    pause: jest.fn(),
  };
  let consumer: {
    connect: jest.Mock;
    subscribe: jest.Mock;
    run: jest.Mock;
    commitOffsets: jest.Mock;
    stop: jest.Mock;
    disconnect: jest.Mock;
    on: jest.Mock;
    events: { CRASH: string };
  };
  let ingestion: AuditRetryService;
  let kafka: Kafka;

  beforeEach(() => {
    consumer = {
      connect: jest.fn().mockResolvedValue(undefined),
      subscribe: jest.fn().mockResolvedValue(undefined),
      run: jest.fn().mockResolvedValue(undefined),
      commitOffsets: jest.fn().mockResolvedValue(undefined),
      stop: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      on: jest.fn(),
      events: { CRASH: 'crash' },
    };
    kafka = {
      consumer: jest.fn().mockReturnValue(consumer as unknown as Consumer),
    } as unknown as Kafka;
    ingestion = {
      start: jest.fn().mockResolvedValue(undefined),
      close: jest.fn().mockResolvedValue(undefined),
      handle: jest.fn().mockResolvedValue(undefined),
    } as unknown as AuditRetryService;
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => {});
  });
  afterEach(() => jest.restoreAllMocks());

  it('subscribes from the earliest uncommitted offset with auto commit disabled', async () => {
    const service = new AuditConsumerService(kafka, config, ingestion, retry);
    await service.onApplicationBootstrap();
    expect(consumer.subscribe).toHaveBeenCalledWith({
      topics: ['audit.events', 'audit.events.retry', 'audit.events.dlq'],
      fromBeginning: true,
    });
    expect(consumer.run).toHaveBeenCalledWith(
      expect.objectContaining({ autoCommit: false }),
    );
    expect(kafka.consumer).toHaveBeenCalledWith(
      expect.objectContaining({
        retry: { retries: 5, restartOnFailure: expect.any(Function) },
      }),
    );
    await service.beforeApplicationShutdown();
    expect(consumer.stop.mock.invocationCallOrder[0]).toBeLessThan(
      consumer.disconnect.mock.invocationCallOrder[0]!,
    );
  });
  it.each(['persisted', 'duplicate', 'rejected'])(
    'commits the next offset only after %s processing',
    async (outcome) => {
      jest.spyOn(ingestion, 'handle').mockImplementation(async () => {
        expect(consumer.commitOffsets).not.toHaveBeenCalled();
        void outcome;
      });
      const service = new AuditConsumerService(kafka, config, ingestion, retry);
      await service.processMessage(payload);
      expect(consumer.commitOffsets).toHaveBeenCalledWith([
        { topic: payload.topic, partition: 0, offset: '9007199254740994' },
      ]);
    },
  );
  it('never commits a failed persistence operation', async () => {
    jest
      .spyOn(ingestion, 'handle')
      .mockRejectedValue(new Error('DB unavailable'));
    const service = new AuditConsumerService(kafka, config, ingestion, retry);
    await expect(service.processMessage(payload)).rejects.toThrow(
      'DB unavailable',
    );
    expect(consumer.commitOffsets).not.toHaveBeenCalled();
  });
  it('supports disabled ingestion without contacting Kafka', async () => {
    const service = new AuditConsumerService(
      kafka,
      { ...config, enabled: false },
      ingestion,
      retry,
    );
    await service.onApplicationBootstrap();
    await service.beforeApplicationShutdown();
    expect(consumer.connect).not.toHaveBeenCalled();
    expect(consumer.disconnect).not.toHaveBeenCalled();
  });
  it('disconnects after a subscription failure and fails startup safely', async () => {
    consumer.subscribe.mockRejectedValue(new Error('broker details'));
    const service = new AuditConsumerService(kafka, config, ingestion, retry);
    await expect(service.onApplicationBootstrap()).rejects.toThrow(
      'Kafka audit consumer startup failed',
    );
    expect(consumer.disconnect).toHaveBeenCalled();
  });
});
