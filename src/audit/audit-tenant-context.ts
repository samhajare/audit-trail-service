import {
  createParamDecorator,
  ExecutionContext,
  UnauthorizedException,
} from '@nestjs/common';
import { AuthenticatedRequest } from '../auth/auth-principal';
export const AuditTenant = createParamDecorator(
  (_data: unknown, context: ExecutionContext): string => {
    const principal = context
      .switchToHttp()
      .getRequest<AuthenticatedRequest>().principal;
    if (!principal) throw new UnauthorizedException();
    return principal.tenantId;
  },
);
