import { FeatureFlagsModule } from '../feature-flags/feature-flags.module';
import { Module } from '@nestjs/common';
import { AuthModule } from '../auth/auth.module';
import { DatabaseModule } from '../database/database.module';
import { KafkaModule } from '../kafka/kafka.module';
import { DlqController } from './dlq.controller';
import { DlqService } from './dlq.service';
import { DlqReplayPublisher } from './dlq-replay.publisher';
@Module({
  imports: [FeatureFlagsModule, AuthModule, DatabaseModule, KafkaModule],
  controllers: [DlqController],
  providers: [DlqService, DlqReplayPublisher],
})
export class DlqModule {}
