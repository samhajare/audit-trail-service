import { Inject, Injectable, Logger } from '@nestjs/common';
import { createHash } from 'node:crypto';
import { TextDecoder } from 'node:util';
import { Kafka, KafkaJSNonRetriableError, Producer } from 'kafkajs';
import { auditEventSchema } from '../contracts/audit-event.schema';
import { assertNoCredentials } from '../database/assert-no-credentials';
import {
  FailureEnvelope,
  failureEnvelopeSchema,
} from '../dlq/failure-envelope';
import {
  AuditIngestionService,
  KafkaMessageLocation,
  logIdentifiers,
} from './audit-ingestion.service';
import { AuditPersistenceError } from './audit-persistence-error';
import { KAFKA_CLIENT } from './kafka.config';
import { RETRY_CONFIG, RetryConfiguration } from './retry.config';
import { DlqRepository } from '../dlq/dlq.repository';

@Injectable()
export class AuditRetryService {
  private readonly producer: Producer;
  private readonly logger = new Logger(AuditRetryService.name);
  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(RETRY_CONFIG) private readonly config: RetryConfiguration,
    private readonly ingestion: AuditIngestionService,
    private readonly dlq: DlqRepository,
  ) {
    this.producer = kafka.producer({ allowAutoTopicCreation: false });
  }
  async start() {
    await this.producer.connect();
  }
  async close() {
    await this.producer.disconnect();
  }

  async handle(
    value: Buffer | null,
    location: KafkaMessageLocation,
    heartbeat: () => Promise<void> = async () => {},
  ) {
    let payload: unknown = null;
    let retryCount = 0;
    let reason: FailureEnvelope['failureReason'] | undefined;
    try {
      if (!value) throw new Error();
      payload = JSON.parse(
        new TextDecoder('utf-8', { fatal: true }).decode(value),
      );
    } catch {
      reason = 'malformed_json';
    }
    if (location.topic === this.config.dlqTopic) {
      const envelope = failureEnvelopeSchema.safeParse(payload);
      if (!envelope.success) {
        this.logger.warn({
          message: 'Invalid DLQ envelope discarded',
          ...location,
        });
        return;
      }
      try {
        assertNoCredentials(envelope.data);
      } catch {
        this.logger.warn({
          message: 'Credential-bearing DLQ envelope discarded',
          ...location,
        });
        return;
      }
      try {
        await this.dlq.store(envelope.data, location);
      } catch {
        throw new KafkaJSNonRetriableError('DLQ indexing failed');
      }
      return;
    }
    if (location.topic === this.config.retryTopic && !reason) {
      const envelope = failureEnvelopeSchema.safeParse(payload);
      if (
        !envelope.success ||
        envelope.data.sourceTopic !== this.config.sourceTopic ||
        envelope.data.retryCount < 1 ||
        envelope.data.retryCount > this.config.maxRetries ||
        envelope.data.failureReason !== 'transient_persistence'
      ) {
        reason = 'invalid_retry_envelope';
      } else {
        payload = envelope.data.originalEvent;
        retryCount = envelope.data.retryCount;
        // Persisted due time survives restart. Cap waiting even on hostile envelopes.
        const remaining = Math.min(
          this.config.delayMs,
          Math.max(
            0,
            Date.parse(envelope.data.retryAt ?? envelope.data.failedAt) -
              Date.now(),
          ),
        );
        let wait = remaining;
        while (wait > 0) {
          const step = Math.min(1000, wait);
          await new Promise((resolve) => setTimeout(resolve, step));
          await heartbeat();
          wait -= step;
        }
      }
    }
    let omitted = false;
    try {
      assertNoCredentials(payload);
    } catch {
      payload = null;
      omitted = true;
      reason = 'prohibited_credentials';
    }
    const parsed = auditEventSchema.safeParse(payload);
    if (!reason && !parsed.success) {
      reason =
        payload &&
        typeof payload === 'object' &&
        'schemaVersion' in payload &&
        payload.schemaVersion !== '1.0'
          ? 'unsupported_schema_version'
          : 'invalid_schema';
    }
    if (!reason && parsed.success) {
      try {
        const outcome = await this.ingestion.handle(
          Buffer.from(JSON.stringify(parsed.data)),
          location,
        );
        if (outcome !== 'rejected') return;
        reason = 'prohibited_credentials';
        payload = null;
        omitted = true;
      } catch (error) {
        if (!(error instanceof AuditPersistenceError)) throw error;
        reason = error.retryable
          ? 'transient_persistence'
          : 'permanent_persistence';
      }
    }
    const retry =
      reason === 'transient_persistence' && retryCount < this.config.maxRetries;
    const correlationId =
      payload &&
      typeof payload === 'object' &&
      'correlationId' in payload &&
      typeof payload.correlationId === 'string'
        ? payload.correlationId
        : null;
    const envelope: FailureEnvelope = failureEnvelopeSchema.parse({
      originalEvent: payload,
      failureReason: reason,
      retryCount: retry ? retryCount + 1 : retryCount,
      failedAt: new Date().toISOString(),
      sourceTopic: this.config.sourceTopic,
      correlationId,
      ...(retry
        ? { retryAt: new Date(Date.now() + this.config.delayMs).toISOString() }
        : {}),
      ...(omitted || reason === 'malformed_json'
        ? {
            originalPayloadOmitted: true,
            originalPayloadSha256: createHash('sha256')
              .update(value ?? Buffer.alloc(0))
              .digest('hex'),
          }
        : {}),
    });
    try {
      await this.producer.send({
        topic: retry ? this.config.retryTopic : this.config.dlqTopic,
        acks: -1,
        messages: [{ key: correlationId, value: JSON.stringify(envelope) }],
      });
    } catch {
      throw new KafkaJSNonRetriableError('Audit failure routing failed');
    }
    this.logger.warn({
      message: retry ? 'Audit retry scheduled' : 'Audit event sent to DLQ',
      reason,
      retryCount: envelope.retryCount,
      ...location,
      ...logIdentifiers(payload),
    });
  }
}
