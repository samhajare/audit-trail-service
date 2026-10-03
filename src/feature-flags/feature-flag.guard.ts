import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  SetMetadata,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { AuthenticatedRequest } from '../auth/auth-principal';
import { AuditFeatureFlag, FeatureFlagService } from './feature-flag.service';
const REQUIRED_FEATURE = 'audit.requiredFeature';
export const RequireFeature = (flag: AuditFeatureFlag) =>
  SetMetadata(REQUIRED_FEATURE, flag);
@Injectable()
export class FeatureFlagGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly flags: FeatureFlagService,
  ) {}
  async canActivate(context: ExecutionContext): Promise<boolean> {
    const flag = this.reflector.getAllAndOverride<AuditFeatureFlag>(
      REQUIRED_FEATURE,
      [context.getHandler(), context.getClass()],
    );
    if (!flag) return true;
    const principal = context
      .switchToHttp()
      .getRequest<AuthenticatedRequest>().principal;
    if (!principal || !(await this.flags.isEnabled(flag, principal)))
      throw new ForbiddenException('Audit capability is disabled');
    return true;
  }
}
