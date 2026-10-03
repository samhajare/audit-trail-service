import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Kafka, logLevel } from 'kafkajs';
import { DatabaseModule } from '../database/database.module';
import { RealtimeModule } from '../realtime/realtime.module';
import { AuditConsumerService } from './audit-consumer.service';
import { AuditIngestionService } from './audit-ingestion.service';
import { AuditRetryService } from './audit-retry.service';
import { RETRY_CONFIG, retryConfig } from './retry.config';
import {
  KAFKA_CLIENT,
  KAFKA_CONFIG,
  KafkaConfiguration,
  kafkaConfig,
} from './kafka.config';

@Module({
  imports: [ConfigModule, DatabaseModule, RealtimeModule],
  providers: [
    { provide: RETRY_CONFIG, inject: [ConfigService], useFactory: retryConfig },
    AuditRetryService,
    { provide: KAFKA_CONFIG, inject: [ConfigService], useFactory: kafkaConfig },
    {
      provide: KAFKA_CLIENT,
      inject: [KAFKA_CONFIG],
      useFactory: (config: KafkaConfiguration) =>
        new Kafka({
          clientId: config.clientId,
          brokers: config.brokers,
          logLevel: logLevel.NOTHING,
          connectionTimeout: 5000,
          requestTimeout: 10000,
          retry: { retries: 5 },
        }),
    },
    AuditIngestionService,
    AuditConsumerService,
  ],
  exports: [AuditConsumerService, KAFKA_CLIENT, RETRY_CONFIG],
})
export class KafkaModule {}
