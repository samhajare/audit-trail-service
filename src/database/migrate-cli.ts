import 'reflect-metadata';
import { Logger, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import { validateEnvironment } from '../common/config/environment';
import { DatabaseModule } from './database.module';
import { DatabaseService } from './database.service';
import { runMigrations } from './migrate';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true, validate: validateEnvironment }),
    DatabaseModule,
  ],
})
class MigrationModule {}

async function main() {
  const app = await NestFactory.createApplicationContext(MigrationModule);
  try {
    await runMigrations(app.get(DatabaseService).pool);
    new Logger('Migrations').log({ message: 'Database migrations applied' });
  } finally {
    await app.close();
  }
}

void main().catch(() => {
  new Logger('Migrations').error({ message: 'Database migration failed' });
  process.exitCode = 1;
});
