import { Controller, Get } from '@nestjs/common';

interface HealthResponse {
  status: string;
  service: string;
  timestamp: string;
}

@Controller()
export class AppController {
  @Get('health')
  health(): HealthResponse {
    return {
      status: 'ok',
      service: 'resolva-backend',
      timestamp: new Date().toISOString(),
    };
  }
}
