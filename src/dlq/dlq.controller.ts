import {
  FeatureFlagGuard,
  RequireFeature,
} from '../feature-flags/feature-flag.guard';
import {
  Body,
  Controller,
  Get,
  HttpCode,
  Param,
  Post,
  Query,
  Req,
  UseGuards,
} from '@nestjs/common';
import { z } from 'zod';
import { AuditQueryValidationPipe } from '../audit/audit-query.dto';
import { AuditTenant } from '../audit/audit-tenant-context';
import { AuthenticatedRequest } from '../auth/auth-principal';
import {
  JwtAuthGuard,
  PermissionsGuard,
  RequirePermissions,
} from '../auth/auth.guards';
import { DlqService } from './dlq.service';

const integer = z
  .string()
  .regex(/^[1-9]\d*$/)
  .transform(Number)
  .pipe(z.number().int().positive().max(Number.MAX_SAFE_INTEGER));
const querySchema = z
  .strictObject({
    page: integer.default(1),
    limit: integer.pipe(z.number().max(100)).default(25),
  })
  .refine((value) => Number.isSafeInteger((value.page - 1) * value.limit));
const idSchema = z
  .string()
  .min(1)
  .refine((value) => !!value.trim());
const empty = new AuditQueryValidationPipe(z.strictObject({}).default({}));
@Controller('audit/dlq')
@UseGuards(JwtAuthGuard, PermissionsGuard)
@RequirePermissions('audit:read')
export class DlqController {
  constructor(private readonly service: DlqService) {}
  @Get()
  list(
    @AuditTenant() tenantId: string,
    @Query(new AuditQueryValidationPipe(querySchema))
    query: { page: number; limit: number },
  ) {
    return this.service.list(tenantId, query.page, query.limit);
  }
  @Get(':eventId')
  detail(
    @AuditTenant() tenantId: string,
    @Param('eventId', new AuditQueryValidationPipe(idSchema)) eventId: string,
    @Query(empty) query: Record<string, never>,
  ) {
    void query;
    return this.service.detail(tenantId, eventId);
  }
  @Post(':eventId/replay')
  @UseGuards(FeatureFlagGuard)
  @RequireFeature('audit-dlq-replay')
  @HttpCode(202)
  @RequirePermissions('audit:read', 'audit:replay')
  replay(
    @AuditTenant() tenantId: string,
    @Param('eventId', new AuditQueryValidationPipe(idSchema)) eventId: string,
    @Req() request: AuthenticatedRequest,
    @Body(empty) body: Record<string, never>,
    @Query(empty) query: Record<string, never>,
  ) {
    void body;
    void query;
    return this.service.replay(tenantId, eventId, request.principal!.subject);
  }
}
