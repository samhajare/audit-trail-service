import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { Subject, filter } from 'rxjs';
import { PersistedAuditEvent } from '../audit/audit.repository';
import { assertNoCredentials } from '../database/assert-no-credentials';

/** Process-local live delivery; publication follows a committed repository write. */
@Injectable()
export class AuditEventBus implements OnApplicationShutdown {
  private readonly events = new Subject<PersistedAuditEvent>();
  publish(event: PersistedAuditEvent): void {
    assertNoCredentials(event);
    this.events.next(event);
  }
  forTenant(tenantId: string) {
    return this.events.pipe(filter((event) => event.tenantId === tenantId));
  }
  onApplicationShutdown() {
    this.events.complete();
  }
}
