import { Module } from '@nestjs/common';
import { StellarStreamService } from './stellar-stream.service';

@Module({
  providers: [StellarStreamService],
})
export class StellarModule {}
