import { ConfigService } from '@nestjs/config';
import { z } from 'zod';
export const RETRY_CONFIG = Symbol('RETRY_CONFIG');
const topic = z
  .string()
  .min(1)
  .max(249)
  .regex(/^[a-zA-Z0-9._-]+$/)
  .refine((value) => value !== '.' && value !== '..');
export function retryConfig(config: ConfigService) {
  const sourceTopic = config.get<string>('KAFKA_TOPIC') ?? 'audit.events';
  const parsed = z
    .object({
      sourceTopic: topic,
      retryTopic: topic,
      dlqTopic: topic,
      maxRetries: z.coerce.number().int().min(0).max(10),
      delayMs: z.coerce.number().int().min(0).max(10000),
    })
    .refine(
      (value) =>
        new Set([value.sourceTopic, value.retryTopic, value.dlqTopic]).size ===
        3,
    )
    .safeParse({
      sourceTopic,
      retryTopic:
        config.get<string>('KAFKA_RETRY_TOPIC') ?? `${sourceTopic}.retry`,
      dlqTopic: config.get<string>('KAFKA_DLQ_TOPIC') ?? `${sourceTopic}.dlq`,
      maxRetries: config.get<unknown>('KAFKA_MAX_RETRIES') ?? 3,
      delayMs: config.get<unknown>('KAFKA_RETRY_DELAY_MS') ?? 1000,
    });
  if (!parsed.success) throw new Error('Invalid Kafka retry configuration');
  return parsed.data;
}
export type RetryConfiguration = ReturnType<typeof retryConfig>;
