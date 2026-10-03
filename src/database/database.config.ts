import { ConfigService } from '@nestjs/config';
import type { PoolConfig } from 'pg';
import { z } from 'zod';

export function databasePoolConfig(config: ConfigService): PoolConfig {
  const text = z.string().refine((value) => value.trim().length > 0);
  const production = config.get<string>('APP_ENV') === 'production';
  const values = z
    .object({
      host: text,
      port: z.coerce.number().int().min(1).max(65535),
      database: text,
      user: text,
      password: text,
      ssl: z.enum(['true', 'false']),
    })
    .safeParse({
      host: config.get<unknown>('DATABASE_HOST') ?? 'localhost',
      port: config.get<unknown>('DATABASE_PORT') ?? '5432',
      database: config.get<unknown>('DATABASE_NAME') ?? 'audit_trail',
      user: config.get<unknown>('DATABASE_USER') ?? 'audit',
      password:
        config.get<unknown>('DATABASE_PASSWORD') ??
        (production ? undefined : 'local_audit_password'),
      ssl:
        config.get<unknown>('DATABASE_SSL') ?? (production ? 'true' : 'false'),
    });
  if (!values.success) {
    // Never include credentials or raw configuration in an error.
    throw new Error('Invalid DATABASE_* configuration');
  }
  return {
    ...values.data,
    ssl: values.data.ssl === 'true' ? { rejectUnauthorized: true } : false,
    max: 10,
    connectionTimeoutMillis: 5000,
    idleTimeoutMillis: 30000,
    statement_timeout: 10000,
  };
}
