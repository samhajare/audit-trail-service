import { generateKeyPairSync } from 'node:crypto';
import { createServer, Server } from 'node:http';
import jwt from 'jsonwebtoken';
import { Auth0Config } from '../../src/auth/auth0.config';

/** Local signing keys/JWKS only; no Auth0 account or credentials required. */
export class Auth0Fixture {
  private readonly keys = generateKeyPairSync('rsa', { modulusLength: 2048 });
  private server?: Server;
  readonly config: Auth0Config = {
    issuer: 'https://test.auth0.example/',
    audience: 'audit-api',
    jwksUri: '',
    tenantClaim: 'https://audit-trail.example.com/tenantId',
  };
  async start() {
    const key = {
      ...this.keys.publicKey.export({ format: 'jwk' }),
      kid: 'test-key',
      alg: 'RS256',
      use: 'sig',
    };
    this.server = createServer((_req, res) => {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ keys: [key] }));
    });
    await new Promise<void>((resolve) =>
      this.server!.listen(0, '127.0.0.1', resolve),
    );
    const address = this.server.address();
    if (!address || typeof address === 'string')
      throw new Error('Expected JWKS port');
    this.config.jwksUri = `http://127.0.0.1:${address.port}/.well-known/jwks.json`;
  }
  token(
    tenantId = 'b5-tenant-a',
    overrides: Record<string, unknown> = {},
    kid = 'test-key',
  ) {
    const payload: Record<string, unknown> = {
      sub: 'viewer',
      iss: this.config.issuer,
      aud: this.config.audience,
      exp: Math.floor(Date.now() / 1000) + 300,
      permissions: ['audit:read'],
      [this.config.tenantClaim]: tenantId,
      ...overrides,
    };
    for (const [key, value] of Object.entries(payload)) {
      if (value === undefined) delete payload[key];
    }
    return jwt.sign(payload, this.keys.privateKey, {
      algorithm: 'RS256',
      keyid: kid,
    });
  }
  async close() {
    if (this.server)
      await new Promise<void>((resolve, reject) =>
        this.server!.close((error) => (error ? reject(error) : resolve())),
      );
  }
}
