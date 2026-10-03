import { Module, Logger } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { init } from '@launchdarkly/node-server-sdk';
import {
  FeatureFlagService,
  LAUNCHDARKLY_CLIENT,
} from './feature-flag.service';
import { FeatureFlagGuard } from './feature-flag.guard';
@Module({
  imports: [ConfigModule],
  providers: [
    {
      provide: LAUNCHDARKLY_CLIENT,
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const key = config.get<string>('LAUNCHDARKLY_SDK_KEY')?.trim();
        if (!key) return null;
        const logger = new Logger('LaunchDarkly');
        return init(key, {
          logger: {
            debug: () => {},
            info: () => {},
            warn: () => {
              logger.warn({ message: 'LaunchDarkly SDK warning' });
            },
            error: () => {
              logger.error({ message: 'LaunchDarkly SDK error' });
            },
          },
        });
      },
    },
    FeatureFlagService,
    FeatureFlagGuard,
  ],
  exports: [FeatureFlagService, FeatureFlagGuard],
})
export class FeatureFlagsModule {}
