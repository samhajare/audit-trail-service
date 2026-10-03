import {
  FeatureFlagGuard,
  RequireFeature,
} from '../feature-flags/feature-flag.guard';
import {
  Controller,
  MessageEvent,
  Query,
  Req,
  Sse,
  UseGuards,
} from '@nestjs/common';
import { interval, map, merge, takeUntil, timer } from 'rxjs';
import { z } from 'zod';
import { AuditQueryValidationPipe } from '../audit/audit-query.dto';
import { AuditTenant } from '../audit/audit-tenant-context';
import { AuthenticatedRequest } from '../auth/auth-principal';
import {
  JwtAuthGuard,
  PermissionsGuard,
  RequirePermissions,
} from '../auth/auth.guards';
import { AuditEventBus } from './audit-event-bus';

@Controller('audit')
@UseGuards(JwtAuthGuard, PermissionsGuard, FeatureFlagGuard)
@RequirePermissions('audit:read')
export class AuditStreamController {
  constructor(private readonly bus: AuditEventBus) {}
  @Sse('stream')
  @RequireFeature('audit-live-stream')
  stream(
    @AuditTenant() tenantId: string,
    @Req() request: AuthenticatedRequest,
    @Query(new AuditQueryValidationPipe(z.strictObject({})))
    query: Record<string, never>,
  ) {
    void query;
    const events = this.bus.forTenant(tenantId).pipe(
      map(
        (event) =>
          ({
            type: 'audit-event',
            // JSON encoding prevents producer-controlled IDs injecting SSE lines.
            id: JSON.stringify(event.eventId),
            retry: 3000,
            data: {
              eventId: event.eventId,
              eventType: event.eventType,
              timestamp: event.timestamp,
              tenantId: event.tenantId,
              correlationId: event.correlationId,
              actor: event.actor,
              resource: event.resource,
              action: event.action,
            },
          }) satisfies MessageEvent,
      ),
    );
    const heartbeat = interval(15000).pipe(
      map(
        () =>
          ({
            type: 'heartbeat',
            data: { timestamp: new Date().toISOString() },
            retry: 3000,
          }) satisfies MessageEvent,
      ),
    );
    // Require a fresh token on reconnect instead of retaining an expired session.
    return merge(events, heartbeat).pipe(
      takeUntil(
        timer(Math.max(0, request.principal!.expiresAt * 1000 - Date.now())),
      ),
    );
  }
}
