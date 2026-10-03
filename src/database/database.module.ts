import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AuditRepository } from '../audit/audit.repository';
import { DatabaseService } from './database.service';
import { PostgresAuditRepository } from './postgres-audit.repository';

@Module({
  imports: [ConfigModule],
  providers: [
    DatabaseService,
    { provide: AuditRepository, useClass: PostgresAuditRepository },
  ],
  exports: [DatabaseService, AuditRepository],
})
export class DatabaseModule {}
