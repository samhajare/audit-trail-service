import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AuditRepository } from '../audit/audit.repository';
import { DatabaseService } from './database.service';
import { PostgresAuditRepository } from './postgres-audit.repository';
import { DlqRepository } from '../dlq/dlq.repository';
import { PostgresDlqRepository } from './postgres-dlq.repository';

@Module({
  imports: [ConfigModule],
  providers: [
    DatabaseService,
    { provide: DlqRepository, useClass: PostgresDlqRepository },
    { provide: AuditRepository, useClass: PostgresAuditRepository },
  ],
  exports: [DatabaseService, AuditRepository, DlqRepository],
})
export class DatabaseModule {}
