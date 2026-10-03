import { validateEnvironment } from '../src/common/config/environment';

describe('Environment configuration', () => {
  it('provides local defaults', () => {
    expect(validateEnvironment({})).toEqual({
      APP_PORT: 3000,
      APP_ENV: 'development',
    });
  });

  it('accepts an explicit port and environment', () => {
    expect(
      validateEnvironment({ APP_PORT: '4000', APP_ENV: 'production' }),
    ).toEqual({ APP_PORT: 4000, APP_ENV: 'production' });
  });

  it.each(['abc', '', '0', '65536', '1.5'])('rejects port %s', (port) => {
    expect(() => validateEnvironment({ APP_PORT: port })).toThrow('APP_PORT');
  });

  it('rejects an unknown environment', () => {
    expect(() => validateEnvironment({ APP_ENV: 'unknown' })).toThrow(
      'APP_ENV',
    );
  });
});
