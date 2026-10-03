import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  SetMetadata,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { AuditPermission, AuthenticatedRequest } from './auth-principal';
import { Auth0TokenService } from './auth0-token.service';
const REQUIRED_PERMISSIONS = 'audit.requiredPermissions';
export const RequirePermissions = (...permissions: AuditPermission[]) =>
  SetMetadata(REQUIRED_PERMISSIONS, permissions);
@Injectable()
export class JwtAuthGuard implements CanActivate {
  constructor(private readonly tokens: Auth0TokenService) {}
  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    request.principal = await this.tokens.authenticate(
      request.headers.authorization,
    );
    return true;
  }
}
@Injectable()
export class PermissionsGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}
  canActivate(context: ExecutionContext): boolean {
    const required =
      this.reflector.getAllAndOverride<AuditPermission[]>(
        REQUIRED_PERMISSIONS,
        [context.getHandler(), context.getClass()],
      ) ?? [];
    const principal = context
      .switchToHttp()
      .getRequest<AuthenticatedRequest>().principal;
    if (
      !principal ||
      !required.every((permission) =>
        principal.permissions.includes(permission),
      )
    )
      throw new ForbiddenException('Insufficient audit permissions');
    return true;
  }
}
