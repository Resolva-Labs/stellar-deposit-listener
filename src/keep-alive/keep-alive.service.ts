import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';

@Injectable()
export class KeepAliveService {
  private readonly logger = new Logger(KeepAliveService.name);

  // Runs every 14 minutes to prevent free-tier hosting from spinning down
  @Cron('0 */14 * * * *')
  async pingHealthEndpoint() {
    const url = process.env.PUBLIC_URL || `http://localhost:${process.env.PORT || 3001}/health`;
    
    this.logger.log(`Pinging keep-alive endpoint at ${url}`);
    
    try {
      const response = await fetch(url);
      if (response.ok) {
        this.logger.log(`Keep-alive ping successful: ${response.status} ${response.statusText}`);
      } else {
        this.logger.warn(`Keep-alive ping returned non-ok status: ${response.status}`);
      }
    } catch (error: any) {
      this.logger.error(`Keep-alive ping failed: ${error.message}`);
    }
  }
}
