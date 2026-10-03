import { Injectable, Logger } from '@nestjs/common';
import { TextDecoder } from 'node:util';
import {
  AuditPersistenceError,
  isTransientPersistenceError,
} from './audit-persistence-error';
import { AuditRepository } from '../audit/audit.repository';
import { auditEventSchema } from '../contracts/audit-event.schema';
import { ProhibitedCredentialFieldsError } from '../database/assert-no-credentials';
import { AuditEventBus } from '../realtime/audit-event-bus';

export interface KafkaMessageLocation {
  topic: string;
  partition: number;
  offset: string;
}

export function logIdentifiers(payload: unknown): Record<string, string> {
  const fields: Record<string, string> = {};
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload))
    return fields;
  const record = payload as Record<string, unknown>;
  for (const key of ['eventId', 'tenantId', 'correlationId', 'eventType']) {
    const value = record[key];
    if (typeof value === 'string' && value.trim()) {
      fields[key] = value.replace(/[\r\n\t]/g, ' ').slice(0, 256);
    }
  }
  const context = record.context;
  if (
    context !== null &&
    typeof context === 'object' &&
    !Array.isArray(context)
  ) {
    const service = (context as Record<string, unknown>).service;
    if (typeof service === 'string' && service.trim()) {
      fields.service = service.replace(/[\r\n\t]/g, ' ').slice(0, 256);
    }
  }
  return fields;
}

@Injectable()
export class AuditIngestionService {
  private readonly logger = new Logger(AuditIngestionService.name);

  constructor(
    private readonly repository: AuditRepository,
    private readonly events: AuditEventBus,
  ) {}

  async handle(
    value: Buffer | null,
    location: KafkaMessageLocation,
  ): Promise<'persisted' | 'duplicate' | 'rejected'> {
    let payload: unknown;
    try {
      if (value === null) throw new Error('Empty Kafka value');
      payload = JSON.parse(
        new TextDecoder('utf-8', { fatal: true }).decode(value),
      );
    } catch {
      this.logger.warn({
        message: 'Audit message rejected',
        reason: 'malformed_json',
        ...location,
      });
      return 'rejected';
    }
    const identifiers = logIdentifiers(payload);
    const parsed = auditEventSchema.safeParse(payload);
    if (!parsed.success) {
      this.logger.warn({
        message: 'Audit message rejected',
        reason: 'invalid_schema',
        ...location,
        ...identifiers,
      });
      return 'rejected';
    }
    try {
      const result = await this.repository.create(parsed.data);
      if (result.status === 'created') this.events.publish(result.event);
      const status = result.status === 'created' ? 'persisted' : 'duplicate';
      this.logger.log({
        message: 'Audit message processed',
        status,
        ...location,
        ...identifiers,
      });
      return status;
    } catch (error) {
      if (error instanceof ProhibitedCredentialFieldsError) {
        this.logger.warn({
          message: 'Audit message rejected',
          reason: 'prohibited_credentials',
          ...location,
          ...identifiers,
        });
        return 'rejected';
      }
      this.logger.error({
        message: 'Audit persistence failed; offset remains uncommitted',
        ...location,
        ...identifiers,
      });
      // Never pass driver errors (which may contain data) into KafkaJS logging.
      throw new AuditPersistenceError(isTransientPersistenceError(error));
    }
  }
}
