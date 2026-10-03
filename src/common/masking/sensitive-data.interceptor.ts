import { FeatureFlagService } from '../../feature-flags/feature-flag.service';
import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { map } from 'rxjs';
import { AuthenticatedRequest } from '../../auth/auth-principal';
import { SensitiveDataService } from './sensitive-data.service';

@Injectable()
export class SensitiveDataInterceptor implements NestInterceptor {
  constructor(
    private readonly masking: SensitiveDataService,
    private readonly flags: FeatureFlagService,
  ) {}
  async intercept(context: ExecutionContext, next: CallHandler) {
    const principal = context
      .switchToHttp()
      .getRequest<AuthenticatedRequest>().principal;
    const allowed =
      principal?.permissions.includes('audit:view-sensitive') === true &&
      (await this.flags.isEnabled('audit-sensitive-data-view', principal));
    if (principal) {
      const response = context.switchToHttp().getResponse<{
        setHeader(name: string, value: string): unknown;
        vary(field: string): unknown;
      }>();
      response.setHeader('Cache-Control', 'private, no-store');
      response.vary('Authorization');
    }
    return next
      .handle()
      .pipe(map((value) => this.masking.mask(value, allowed)));
  }
}
