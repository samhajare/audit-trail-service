import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { Kafka, logLevel } from 'kafkajs';
import { DatabaseModule } from '../database/database.module';
import { AuditConsumerService } from './audit-consumer.service';
import { AuditIngestionService } from './audit-ingestion.service';
import { KAFKA_CLIENT, KAFKA_CONFIG, KafkaConfiguration, kafkaConfig } from './kafka.config';

@Module({
  imports: [ConfigModule, DatabaseModule],
  providers: [
    { provide: KAFKA_CONFIG, inject: [ConfigService], useFactory: kafkaConfig },
    {
      provide: KAFKA_CLIENT,
      inject: [KAFKA_CONFIG],
      useFactory: (config: KafkaConfiguration) => new Kafka({
        clientId: config.clientId, brokers: config.brokers,
        logLevel: logLevel.NOTHING,
        connectionTimeout: 5000, requestTimeout: 10000,
        retry: { retries: 0 },
      }),
    },
    AuditIngestionService,
    AuditConsumerService,
  ],
  exports: [AuditConsumerService],
})
export class KafkaModule {}
