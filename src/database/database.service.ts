import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';
import { databasePoolConfig } from './database.config';

@Injectable()
export class DatabaseService implements OnModuleDestroy {
  readonly pool: Pool;
  private readonly logger = new Logger(DatabaseService.name);

  constructor(config: ConfigService) {
    this.pool = new Pool(databasePoolConfig(config));
    this.pool.on('error', () => {
      this.logger.error({ message: 'Idle PostgreSQL connection failed' });
    });
  }

  async onModuleDestroy() {
    await this.pool.end();
  }
}
