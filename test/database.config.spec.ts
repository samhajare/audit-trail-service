import { ConfigService } from '@nestjs/config';
import { databasePoolConfig } from '../src/database/database.config';

function isolatedConfig(values: Record<string, unknown>) {
  const config = new ConfigService();
  jest.spyOn(config, 'get').mockImplementation((key) => values[String(key)]);
  return config;
}

describe('Database configuration', () => {
  it('uses explicit connection settings and verified TLS', () => {
    const options = databasePoolConfig(
      isolatedConfig({
        DATABASE_HOST: 'db.example.com',
        DATABASE_PORT: '5433',
        DATABASE_NAME: 'audit',
        DATABASE_USER: 'service',
        DATABASE_PASSWORD: 'test-only-value',
        DATABASE_SSL: 'true',
      }),
    );
    expect(options).toMatchObject({
      host: 'db.example.com',
      port: 5433,
      database: 'audit',
      user: 'service',
      ssl: { rejectUnauthorized: true },
    });
  });

  it.each(['0', '65536', '1.5', 'invalid', ''])(
    'rejects invalid database port %s',
    (port) => {
      expect(() =>
        databasePoolConfig(isolatedConfig({ DATABASE_PORT: port })),
      ).toThrow('Invalid DATABASE_* configuration');
    },
  );

  it('requires a production password without exposing values', () => {
    expect(() =>
      databasePoolConfig(isolatedConfig({ APP_ENV: 'production' })),
    ).toThrow('Invalid DATABASE_* configuration');
  });

  it('rejects unknown TLS values', () => {
    expect(() =>
      databasePoolConfig(isolatedConfig({ DATABASE_SSL: 'maybe' })),
    ).toThrow('Invalid DATABASE_* configuration');
  });
});
