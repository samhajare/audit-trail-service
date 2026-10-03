import {
  FeatureFlagService,
  FEATURE_PERMISSIONS,
  FeatureFlagClient,
} from '../src/feature-flags/feature-flag.service';
import { AuthPrincipal } from '../src/auth/auth-principal';

describe('LaunchDarkly feature flag abstraction', () => {
  const principal: AuthPrincipal = {
    subject: 'auth0|user-a',
    tenantId: 'tenant-a',
    permissions: Object.values(FEATURE_PERMISSIONS),
    expiresAt: 2000000000,
  };
  const client = () => ({
    initialized: jest.fn().mockReturnValue(true),
    variation: jest.fn().mockResolvedValue(true),
    close: jest.fn(),
  });
  it.each(
    Object.keys(FEATURE_PERMISSIONS) as (keyof typeof FEATURE_PERMISSIONS)[],
  )(
    'targets verified tenant/user and requires permission for %s',
    async (flag) => {
      const sdk = client();
      const service = new FeatureFlagService(sdk as FeatureFlagClient);
      expect(
        await service.isEnabled(flag, { ...principal, permissions: [] }),
      ).toBe(false);
      expect(sdk.variation).not.toHaveBeenCalled();
      expect(await service.isEnabled(flag, principal)).toBe(true);
      expect(sdk.variation).toHaveBeenCalledWith(
        flag,
        {
          kind: 'multi',
          user: { key: principal.subject },
          tenant: { key: principal.tenantId },
        },
        false,
      );
      await service.isEnabled(flag, {
        ...principal,
        tenantId: 'tenant-b',
        subject: 'auth0|user-b',
      });
      expect(sdk.variation).toHaveBeenLastCalledWith(
        flag,
        {
          kind: 'multi',
          user: { key: 'auth0|user-b' },
          tenant: { key: 'tenant-b' },
        },
        false,
      );
    },
  );
  it('disables capabilities without a client or before initialization', async () => {
    expect(
      await new FeatureFlagService(null).isEnabled(
        'audit-live-stream',
        principal,
      ),
    ).toBe(false);
    const sdk = client();
    sdk.initialized.mockReturnValue(false);
    expect(
      await new FeatureFlagService(sdk).isEnabled(
        'audit-live-stream',
        principal,
      ),
    ).toBe(false);
    expect(sdk.variation).not.toHaveBeenCalled();
  });
  it.each([false, undefined, null, 'true', 1])(
    'fails closed for result %s',
    async (value) => {
      const sdk = client();
      sdk.variation.mockResolvedValue(value);
      expect(
        await new FeatureFlagService(sdk).isEnabled(
          'audit-live-stream',
          principal,
        ),
      ).toBe(false);
    },
  );
  it('handles evaluation errors and closes the client', async () => {
    const sdk = client();
    sdk.variation.mockRejectedValue(new Error('unavailable'));
    const service = new FeatureFlagService(sdk);
    expect(await service.isEnabled('audit-live-stream', principal)).toBe(false);
    service.onApplicationShutdown();
    expect(sdk.close).toHaveBeenCalledTimes(1);
  });
});
