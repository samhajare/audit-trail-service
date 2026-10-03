import { FeatureFlagsModule } from '../../feature-flags/feature-flags.module';
import { Module } from '@nestjs/common';
import { APP_INTERCEPTOR } from '@nestjs/core';
import { SensitiveDataService } from './sensitive-data.service';
import { SensitiveDataInterceptor } from './sensitive-data.interceptor';
@Module({
  imports: [FeatureFlagsModule],
  providers: [
    SensitiveDataService,
    { provide: APP_INTERCEPTOR, useClass: SensitiveDataInterceptor },
  ],
  exports: [SensitiveDataService],
})
export class MaskingModule {}
