import { FeatureFlagsModule } from '../feature-flags/feature-flags.module';
import { Module } from '@nestjs/common';
import { AuthModule } from '../auth/auth.module';
import { AuditEventBus } from './audit-event-bus';
import { AuditStreamController } from './audit-stream.controller';

@Module({
  imports: [FeatureFlagsModule, AuthModule],
  controllers: [AuditStreamController],
  providers: [AuditEventBus],
  exports: [AuditEventBus],
})
export class RealtimeModule {}
