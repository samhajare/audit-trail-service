import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { validateEnvironment } from './common/config/environment';
import { HealthController } from './common/health/health.controller';
import { DatabaseModule } from './database/database.module';
import { KafkaModule } from './kafka/kafka.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true, validate: validateEnvironment }),
    DatabaseModule,
    KafkaModule,
  ],
  controllers: [HealthController],
})
export class AppModule {}
