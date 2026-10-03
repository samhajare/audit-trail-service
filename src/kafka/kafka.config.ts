import { ConfigService } from '@nestjs/config';
import { z } from 'zod';

export const KAFKA_CONFIG = Symbol('KAFKA_CONFIG');
export const KAFKA_CLIENT = Symbol('KAFKA_CLIENT');

const nonBlank = z.string().trim().min(1);
const schema = z.object({
  enabled: z.enum(['true', 'false']),
  brokers: z.array(nonBlank.regex(/^[^\s:/]+:\d+$/)).min(1),
  clientId: nonBlank,
  groupId: nonBlank,
  topic: nonBlank.regex(/^[a-zA-Z0-9._-]+$/),
});

export function kafkaConfig(config: ConfigService) {
  const brokers = config.get<unknown>('KAFKA_BROKERS') ?? 'localhost:9092';
  const result = schema.safeParse({
    enabled: config.get<unknown>('KAFKA_ENABLED') ?? 'true',
    brokers: typeof brokers === 'string' ? brokers.split(',').map((value) => value.trim()) : brokers,
    clientId: config.get<unknown>('KAFKA_CLIENT_ID') ?? 'audit-trail-service',
    groupId: config.get<unknown>('KAFKA_GROUP_ID') ?? 'audit-trail-service-v1',
    topic: config.get<unknown>('KAFKA_TOPIC') ?? 'audit.events',
  });
  if (!result.success) throw new Error('Invalid KAFKA_* configuration');
  return { ...result.data, enabled: result.data.enabled === 'true' };
}

export type KafkaConfiguration = ReturnType<typeof kafkaConfig>;
