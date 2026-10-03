import { Inject, Injectable, UnauthorizedException } from '@nestjs/common';
import jwt from 'jsonwebtoken';
import jwks from 'jwks-rsa';
import { z } from 'zod';
import { AUTH0_CONFIG, Auth0Config } from './auth0.config';
import { AuthPrincipal } from './auth-principal';

@Injectable()
export class Auth0TokenService {
  private readonly client: jwks.JwksClient | null;
  constructor(
    @Inject(AUTH0_CONFIG) private readonly config: Auth0Config | null,
  ) {
    this.client = config
      ? jwks({
          jwksUri: config.jwksUri,
          cache: true,
          cacheMaxEntries: 5,
          cacheMaxAge: 600000,
          rateLimit: true,
          jwksRequestsPerMinute: 10,
          timeout: 5000,
        })
      : null;
  }
  async authenticate(authorization: unknown): Promise<AuthPrincipal> {
    try {
      if (
        !this.config ||
        !this.client ||
        typeof authorization !== 'string' ||
        authorization.length > 16384
      )
        throw new Error();
      const match = /^Bearer ([^\s]+)$/i.exec(authorization);
      if (!match?.[1]) throw new Error();
      const token = match[1];
      const decoded = jwt.decode(token, { complete: true });
      if (
        !decoded ||
        decoded.header.alg !== 'RS256' ||
        typeof decoded.header.kid !== 'string' ||
        !decoded.header.kid ||
        decoded.header.kid.length > 256
      )
        throw new Error();
      const key = await this.client.getSigningKey(decoded.header.kid);
      const payload = jwt.verify(token, key.getPublicKey(), {
        algorithms: ['RS256'],
        issuer: this.config.issuer,
        audience: this.config.audience,
      });
      const claims = z
        .object({
          sub: z.string().trim().min(1),
          exp: z.number().finite(),
          permissions: z.array(z.string()).default([]),
        })
        .parse(payload);
      const tenantId = z
        .string()
        .min(1)
        .max(256)
        .refine((value) => value.trim().length > 0 && !/[\r\n]/.test(value))
        .parse((payload as jwt.JwtPayload)[this.config.tenantClaim]);
      return {
        subject: claims.sub,
        tenantId,
        permissions: claims.permissions,
        expiresAt: claims.exp,
      };
    } catch {
      // Never include token values, claims, or verification error details.
      throw new UnauthorizedException('Invalid or missing bearer token');
    }
  }
}
