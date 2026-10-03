import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { AUTH0_CONFIG, auth0Config } from './auth0.config';
import { Auth0TokenService } from './auth0-token.service';
import { JwtAuthGuard, PermissionsGuard } from './auth.guards';
@Module({
  imports: [ConfigModule],
  providers: [
    { provide: AUTH0_CONFIG, useFactory: auth0Config, inject: [ConfigService] },
    Auth0TokenService,
    JwtAuthGuard,
    PermissionsGuard,
  ],
  exports: [Auth0TokenService, JwtAuthGuard, PermissionsGuard],
})
export class AuthModule {}
