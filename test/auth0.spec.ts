import { ConfigService } from '@nestjs/config';
import jwt from 'jsonwebtoken';
import { auth0Config } from '../src/auth/auth0.config';
import { Auth0TokenService } from '../src/auth/auth0-token.service';
import { Auth0Fixture } from './fixtures/auth0.fixture';

describe('Auth0 JWT verification', () => {
  const fixture = new Auth0Fixture();
  let tokens: Auth0TokenService;
  beforeAll(async () => {
    await fixture.start();
    tokens = new Auth0TokenService(fixture.config);
  });
  afterAll(() => fixture.close());
  it('verifies an RSA signature through JWKS and extracts trusted claims', async () => {
    await expect(
      tokens.authenticate(`Bearer ${fixture.token('tenant-a')}`),
    ).resolves.toMatchObject({
      subject: 'viewer',
      tenantId: 'tenant-a',
      permissions: ['audit:read'],
    });
  });
  it.each([
    { exp: 1 },
    { exp: undefined },
    { nbf: Math.floor(Date.now() / 1000) + 3600 },
    { iss: 'https://attacker.example/' },
    { aud: 'wrong-api' },
    { sub: '' },
    { permissions: 'audit:read' },
    { 'https://audit-trail.example.com/tenantId': undefined },
    { 'https://audit-trail.example.com/tenantId': ' ' },
    { 'https://audit-trail.example.com/tenantId': ['tenant-a'] },
  ])('rejects invalid signed claims %j', async (claims) => {
    await expect(
      tokens.authenticate(`Bearer ${fixture.token('tenant-a', claims)}`),
    ).rejects.toThrow('Invalid or missing bearer token');
  });
  it.each([undefined, '', 'Basic value', 'Bearer broken', 'Bearer a b'])(
    'rejects malformed authorization %s',
    async (header) => {
      await expect(tokens.authenticate(header)).rejects.toThrow(
        'Invalid or missing bearer token',
      );
    },
  );
  it('rejects unknown keys, tampered signatures, and symmetric algorithms', async () => {
    const token = fixture.token();
    const parts = token.split('.');
    parts[2] = 'invalid-signature';
    for (const invalid of [
      fixture.token('tenant-a', {}, 'unknown'),
      parts.join('.'),
      jwt.sign({ sub: 'viewer' }, 'test-only-secret', { algorithm: 'HS256' }),
    ]) {
      await expect(tokens.authenticate(`Bearer ${invalid}`)).rejects.toThrow(
        'Invalid or missing bearer token',
      );
    }
  });
  it('fails closed without Auth0 configuration', async () => {
    await expect(
      new Auth0TokenService(null).authenticate(`Bearer ${fixture.token()}`),
    ).rejects.toThrow('Invalid or missing bearer token');
  });
});

describe('Auth0 configuration', () => {
  function config(values: Record<string, unknown>) {
    const service = new ConfigService();
    jest.spyOn(service, 'get').mockImplementation((key) => values[String(key)]);
    return service;
  }
  it('derives a trusted HTTPS issuer and JWKS URL', () => {
    expect(
      auth0Config(
        config({
          AUTH0_DOMAIN: 'demo.us.auth0.com',
          AUTH0_AUDIENCE: 'audit-api',
        }),
      ),
    ).toMatchObject({
      issuer: 'https://demo.us.auth0.com/',
      jwksUri: 'https://demo.us.auth0.com/.well-known/jwks.json',
    });
  });
  it('allows unconfigured health/ingestion while audit authentication fails closed', () =>
    expect(auth0Config(config({}))).toBeNull());
  it.each([
    { AUTH0_DOMAIN: 'https://attacker.example' },
    { AUTH0_DOMAIN: 'demo.auth0.com' },
    { AUTH0_AUDIENCE: 'audit-api' },
    {
      AUTH0_DOMAIN: 'demo.auth0.com',
      AUTH0_AUDIENCE: 'audit-api',
      AUTH0_TENANT_CLAIM: 'tenantId',
    },
  ])('rejects invalid or partial configuration %j', (values) =>
    expect(() => auth0Config(config(values))).toThrow(
      'Invalid Auth0 configuration',
    ),
  );
});
