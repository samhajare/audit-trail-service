import { ConfigService } from '@nestjs/config';
import { kafkaConfig } from '../src/kafka/kafka.config';

function config(values: Record<string, unknown> = {}) {
  const service = new ConfigService();
  jest.spyOn(service, 'get').mockImplementation((key) => values[String(key)]);
  return service;
}

describe('Kafka configuration', () => {
  it('defaults to the audit.events topic and enabled consumption', () => {
    expect(kafkaConfig(config())).toEqual({
      enabled: true,
      brokers: ['localhost:9092'],
      clientId: 'audit-trail-service',
      groupId: 'audit-trail-service-v1',
      topic: 'audit.events',
    });
  });
  it('supports explicit brokers and disabling consumption', () => {
    expect(
      kafkaConfig(
        config({
          KAFKA_ENABLED: 'false',
          KAFKA_BROKERS: 'kafka:29092, [::1]:9092',
          KAFKA_CLIENT_ID: 'client',
          KAFKA_GROUP_ID: 'group',
          KAFKA_TOPIC: 'test-topic',
        }),
      ),
    ).toMatchObject({
      enabled: false,
      brokers: ['kafka:29092', '[::1]:9092'],
      topic: 'test-topic',
    });
  });
  it.each([
    { KAFKA_BROKERS: '' },
    { KAFKA_BROKERS: 'host' },
    { KAFKA_BROKERS: 'host:0' },
    { KAFKA_BROKERS: 'host:65536' },
    { KAFKA_ENABLED: 'yes' },
    { KAFKA_TOPIC: 'bad topic' },
    { KAFKA_TOPIC: '..' },
    { KAFKA_TOPIC: 'a'.repeat(250) },
    { KAFKA_GROUP_ID: ' ' },
  ])('rejects invalid configuration %j', (values) => {
    expect(() => kafkaConfig(config(values))).toThrow(
      'Invalid KAFKA_* configuration',
    );
  });
});
