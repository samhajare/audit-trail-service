import {
  BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
} from '@nestjs/common';
import { Consumer, EachMessagePayload, Kafka } from 'kafkajs';
import { AuditRetryService } from './audit-retry.service';
import { RETRY_CONFIG, RetryConfiguration } from './retry.config';
import { KAFKA_CLIENT, KAFKA_CONFIG, KafkaConfiguration } from './kafka.config';

@Injectable()
export class AuditConsumerService
  implements OnApplicationBootstrap, BeforeApplicationShutdown
{
  private readonly logger = new Logger(AuditConsumerService.name);
  private readonly consumer: Consumer;
  private connected = false;

  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(KAFKA_CONFIG) private readonly config: KafkaConfiguration,
    private readonly ingestion: AuditRetryService,
    @Inject(RETRY_CONFIG) private readonly retry: RetryConfiguration,
  ) {
    this.consumer = kafka.consumer({
      groupId: config.groupId,
      allowAutoTopicCreation: false,
      retry: { retries: 5, restartOnFailure: async () => false },
    });
    this.consumer.on(this.consumer.events.CRASH, () => {
      this.logger.error({
        message: 'Kafka audit consumer stopped; operator restart required',
        topic: this.config.topic,
      });
    });
  }

  async onApplicationBootstrap() {
    if (!this.config.enabled) return;
    try {
      await this.consumer.connect();
      this.connected = true;
      await this.ingestion.start();
      await this.consumer.subscribe({
        topics: [this.config.topic, this.retry.retryTopic, this.retry.dlqTopic],
        fromBeginning: true,
      });
      await this.consumer.run({
        autoCommit: false,
        partitionsConsumedConcurrently: 1,
        eachMessage: (payload) => this.processMessage(payload),
      });
      this.logger.log({
        message: 'Kafka audit consumer started',
        topic: this.config.topic,
        groupId: this.config.groupId,
      });
    } catch {
      if (this.connected) {
        await Promise.allSettled([
          this.consumer.disconnect(),
          this.ingestion.close(),
        ]);
      }
      this.connected = false;
      throw new Error('Kafka audit consumer startup failed');
    }
  }

  async processMessage({
    topic,
    partition,
    message,
    heartbeat,
  }: EachMessagePayload): Promise<void> {
    await this.ingestion.handle(
      message.value,
      {
        topic,
        partition,
        offset: message.offset,
      },
      heartbeat,
    );
    await this.consumer.commitOffsets([
      { topic, partition, offset: (BigInt(message.offset) + 1n).toString() },
    ]);
  }

  async beforeApplicationShutdown() {
    if (!this.connected) return;
    // Drain ingestion before the database pool's onApplicationShutdown closes it.
    try {
      await this.consumer.stop();
    } finally {
      await Promise.allSettled([
        this.consumer.disconnect(),
        this.ingestion.close(),
      ]);
      this.connected = false;
    }
  }
}
