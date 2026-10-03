import {
  JwtAuthGuard,
  PermissionsGuard,
  RequirePermissions,
} from '../auth/auth.guards';
import { AuditTenant } from './audit-tenant-context';
import {
  BadRequestException,
  Controller,
  Get,
  Param,
  ParseUUIDPipe,
  Query,
  UseGuards,
} from '@nestjs/common';
import { AuditService } from './audit.service';
import { z } from 'zod';
import { auditFiltersSchema } from './audit-filters';
import {
  AuditEventsQueryDto,
  AuditQueryValidationPipe,
  AuditStatisticsQueryDto,
  auditEventsQuerySchema,
} from './audit-query.dto';

@Controller('audit')
@UseGuards(JwtAuthGuard, PermissionsGuard)
@RequirePermissions('audit:read')
export class AuditController {
  constructor(private readonly service: AuditService) {}

  @Get('events')
  list(
    @AuditTenant() tenantId: string,
    @Query(new AuditQueryValidationPipe(auditEventsQuerySchema))
    query: AuditEventsQueryDto,
  ) {
    return this.service.list(tenantId, query);
  }

  @Get('events/:id')
  detail(
    @AuditTenant() tenantId: string,
    @Param('id', new ParseUUIDPipe()) id: string,
    @Query(new AuditQueryValidationPipe(z.strictObject({})))
    query: Record<string, never>,
  ) {
    void query;
    return this.service.detail(tenantId, id);
  }

  @Get('timeline/:correlationId')
  timeline(
    @AuditTenant() tenantId: string,
    @Param('correlationId') correlationId: string,
    @Query(new AuditQueryValidationPipe(auditEventsQuerySchema))
    query: AuditEventsQueryDto,
  ) {
    if (!correlationId.trim() || correlationId.length > 256)
      throw new BadRequestException('Invalid correlationId');
    return this.service.list(tenantId, query, correlationId);
  }

  @Get('statistics')
  statistics(
    @AuditTenant() tenantId: string,
    @Query(new AuditQueryValidationPipe(auditFiltersSchema))
    query: AuditStatisticsQueryDto,
  ) {
    return this.service.statistics(tenantId, query);
  }
}
