import { ConfigService } from '@nestjs/config';
import { z } from 'zod';

export const AUTH0_CONFIG = Symbol('AUTH0_CONFIG');
export interface Auth0Config {
  issuer: string;
  audience: string;
  jwksUri: string;
  tenantClaim: string;
}
export function auth0Config(config: ConfigService): Auth0Config | null {
  const domain = config.get<string>('AUTH0_DOMAIN');
  const audience = config.get<string>('AUTH0_AUDIENCE');
  if (!domain && !audience) return null;
  const parsed = z
    .object({
      domain: z
        .string()
        .regex(/^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$/),
      audience: z.string().trim().min(1),
      tenantClaim: z.url().startsWith('https://'),
    })
    .safeParse({
      domain,
      audience,
      tenantClaim:
        config.get<string>('AUTH0_TENANT_CLAIM') ??
        'https://audit-trail.example.com/tenantId',
    });
  if (!parsed.success) throw new Error('Invalid Auth0 configuration');
  return {
    issuer: `https://${parsed.data.domain}/`,
    audience: parsed.data.audience,
    jwksUri: `https://${parsed.data.domain}/.well-known/jwks.json`,
    tenantClaim: parsed.data.tenantClaim,
  };
}
