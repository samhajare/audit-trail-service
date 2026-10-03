import { Inject, Injectable, OnApplicationShutdown } from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { AuditEvent } from '../contracts/audit-event';
import { KAFKA_CLIENT } from '../kafka/kafka.config';
import { RETRY_CONFIG, RetryConfiguration } from '../kafka/retry.config';

@Injectable()
export class DlqReplayPublisher implements OnApplicationShutdown {
  private readonly producer: Producer;
  private connection?: Promise<void>;
  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(RETRY_CONFIG) private readonly config: RetryConfiguration,
  ) {
    this.producer = kafka.producer({ allowAutoTopicCreation: false });
  }
  get sourceTopic() {
    return this.config.sourceTopic;
  }
  async publish(event: AuditEvent, replayId: string) {
    this.connection ??= this.producer.connect();
    await this.connection;
    await this.producer.send({
      topic: this.config.sourceTopic,
      acks: -1,
      messages: [
        {
          key: event.correlationId,
          value: JSON.stringify(event),
          headers: { 'audit-replay-id': replayId, 'audit-replay-count': '1' },
        },
      ],
    });
  }
  async onApplicationShutdown() {
    if (this.connection) {
      await this.connection.catch(() => {});
      await this.producer.disconnect();
    }
  }
}
