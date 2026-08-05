import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { AppController } from './app.controller';
import { SupabaseModule } from './supabase/supabase.module';
import { StellarModule } from './stellar/stellar.module';
import { KeepAliveModule } from './keep-alive/keep-alive.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    ScheduleModule.forRoot(),
    SupabaseModule,
    StellarModule,
    KeepAliveModule,
  ],
  controllers: [AppController],
})
export class AppModule {}
