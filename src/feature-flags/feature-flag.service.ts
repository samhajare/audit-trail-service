import {
  Inject,
  Injectable,
  Logger,
  OnApplicationShutdown,
} from '@nestjs/common';
import { LDClient } from '@launchdarkly/node-server-sdk';
import { AuditPermission, AuthPrincipal } from '../auth/auth-principal';

export const LAUNCHDARKLY_CLIENT = Symbol('LAUNCHDARKLY_CLIENT');
export const FEATURE_PERMISSIONS = {
  'audit-live-stream': 'audit:read',
  'audit-data-export': 'audit:export',
  'audit-sensitive-data-view': 'audit:view-sensitive',
  'audit-dlq-replay': 'audit:replay',
} as const satisfies Record<string, AuditPermission>;
export type AuditFeatureFlag = keyof typeof FEATURE_PERMISSIONS;
export type FeatureFlagClient = Pick<
  LDClient,
  'initialized' | 'variation' | 'close'
>;
@Injectable()
export class FeatureFlagService implements OnApplicationShutdown {
  private readonly logger = new Logger(FeatureFlagService.name);
  constructor(
    @Inject(LAUNCHDARKLY_CLIENT)
    private readonly client: FeatureFlagClient | null,
  ) {}
  async isEnabled(
    flag: AuditFeatureFlag,
    principal: AuthPrincipal,
  ): Promise<boolean> {
    if (!principal.permissions.includes(FEATURE_PERMISSIONS[flag]))
      return false;
    if (!this.client?.initialized()) return false;
    try {
      return (
        (await this.client.variation(
          flag,
          {
            kind: 'multi',
            user: { key: principal.subject },
            tenant: { key: principal.tenantId },
          },
          false,
        )) === true
      );
    } catch {
      this.logger.warn({ message: 'Feature flag evaluation failed', flag });
      return false;
    }
  }
  onApplicationShutdown() {
    this.client?.close();
  }
}
