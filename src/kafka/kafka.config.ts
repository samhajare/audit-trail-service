import { ConfigService } from '@nestjs/config';
import { z } from 'zod';

export const KAFKA_CONFIG = Symbol('KAFKA_CONFIG');
export const KAFKA_CLIENT = Symbol('KAFKA_CLIENT');

const nonBlank = z.string().trim().min(1);
const schema = z.object({
  enabled: z.enum(['true', 'false']),
  brokers: z
    .array(
      nonBlank.regex(/^(?:\[[a-fA-F0-9:]+\]|[^\s:/]+):\d+$/).refine((value) => {
        const port = Number(value.slice(value.lastIndexOf(':') + 1));
        return port >= 1 && port <= 65535;
      }),
    )
    .min(1),
  clientId: nonBlank,
  groupId: nonBlank,
  topic: nonBlank
    .max(249)
    .regex(/^[a-zA-Z0-9._-]+$/)
    .refine((value) => value !== '.' && value !== '..'),
});

export function kafkaConfig(config: ConfigService) {
  const brokers = config.get<unknown>('KAFKA_BROKERS') ?? 'localhost:9092';
  const result = schema.safeParse({
    enabled: config.get<unknown>('KAFKA_ENABLED') ?? 'true',
    brokers:
      typeof brokers === 'string'
        ? brokers.split(',').map((value) => value.trim())
        : brokers,
    clientId: config.get<unknown>('KAFKA_CLIENT_ID') ?? 'audit-trail-service',
    groupId: config.get<unknown>('KAFKA_GROUP_ID') ?? 'audit-trail-service-v1',
    topic: config.get<unknown>('KAFKA_TOPIC') ?? 'audit.events',
  });
  if (!result.success) throw new Error('Invalid KAFKA_* configuration');
  return { ...result.data, enabled: result.data.enabled === 'true' };
}

export type KafkaConfiguration = ReturnType<typeof kafkaConfig>;
