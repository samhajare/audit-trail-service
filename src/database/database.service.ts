import { Injectable, Logger, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Pool } from 'pg';
import { databasePoolConfig } from './database.config';

@Injectable()
export class DatabaseService implements OnApplicationShutdown {
  readonly pool: Pool;
  private readonly logger = new Logger(DatabaseService.name);

  constructor(config: ConfigService) {
    this.pool = new Pool(databasePoolConfig(config));
    this.pool.on('error', () => {
      this.logger.error({ message: 'Idle PostgreSQL connection failed' });
    });
  }

  async onApplicationShutdown() {
    await this.pool.end();
  }
}
