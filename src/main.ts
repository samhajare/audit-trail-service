import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { ConfigService } from '@nestjs/config';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();
  const config = app.get(ConfigService);
  await app.listen(config.getOrThrow<number>('APP_PORT'));
}

void bootstrap();
