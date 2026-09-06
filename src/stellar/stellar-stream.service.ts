import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Horizon, Keypair } from '@stellar/stellar-sdk';
import { AquariusClient, Asset as AquaAsset, XLM as AquaXLM } from '@aquariusdefi/sdk';
import { SupabaseService } from '../supabase/supabase.service';

type PaymentRecord = Horizon.ServerApi.PaymentOperationRecord;
type TransactionRecord = Horizon.ServerApi.TransactionRecord;

@Injectable()
export class StellarStreamService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(StellarStreamService.name);
  private readonly server: Horizon.Server;
  private readonly wallets: string[];
  private readonly closers = new Map<string, () => void>();
  private readonly reconnectDelayMs = 5_000;
  private destroyed = false;

  constructor(private readonly supabase: SupabaseService) {
    const horizonUrl = process.env.HORIZON_URL ?? 'https://horizon.stellar.org';
    this.server = new Horizon.Server(horizonUrl);
    this.wallets = StellarStreamService.resolveWallets();
  }

  private static resolveWallets(): string[] {
    const raw = process.env.PLATFORM_WALLETS?.trim();
    if (raw) {
      return raw
        .split(',')
        .map((wallet) => wallet.trim())
        .filter((wallet) => wallet.length > 0);
    }
    throw new Error('PLATFORM_WALLETS environment variable is required but not set.');
  }

  async onModuleInit(): Promise<void> {
    this.logger.log(`Booting deposit streams for ${this.wallets.length} platform wallet(s)`);
    await Promise.all(
      this.wallets.map((wallet) =>
        this.startStream(wallet).catch((error: unknown) => {
          this.logger.error(
            `Could not start stream for ${wallet}: ${this.describeError(error)}. Retrying.`,
          );
          this.scheduleReconnect(wallet);
        }),
      ),
    );
  }

  onModuleDestroy(): void {
    this.destroyed = true;
    for (const [wallet, close] of this.closers.entries()) {
      this.logger.log(`Closing stream for ${wallet}`);
      close();
    }
    this.closers.clear();
  }

  /**
   * Opens (or re-opens) the payment stream for a single wallet, resuming from the checkpoint stored in cursor_store.
   */
  private async startStream(wallet: string): Promise<void> {
    this.closers.get(wallet)?.();
    this.closers.delete(wallet);

    const cursor = await this.loadCursor(wallet);
    this.logger.log(`Streaming ${wallet} from cursor "${cursor}"`);

    const close = this.server
      .payments()
      .forAccount(wallet)
      .cursor(cursor)
      .stream({
        onmessage: (record: any) => {
          void this.handlePayment(wallet, record as PaymentRecord);
        },
        onerror: (error: unknown) => {
          this.logger.error(
            `Stream error on ${wallet}: ${this.describeError(error)}`,
          );
          this.scheduleReconnect(wallet);
        },
      });

    this.closers.set(wallet, close);
  }

  private scheduleReconnect(wallet: string): void {
    if (this.destroyed) {
      return;
    }
    setTimeout(() => {
      if (this.destroyed) {
        return;
      }
      void this.startStream(wallet).catch((error: unknown) =>
        this.logger.error(
          `Failed to reconnect ${wallet}: ${this.describeError(error)}`,
        ),
      );
    }, this.reconnectDelayMs);
  }

  /**
   * Processes a single streamed payment: resolves the memo, matches the user,
   * records the deposit, then advances the checkpoint.
   */
  private async handlePayment(
    wallet: string,
    record: PaymentRecord,
  ): Promise<void> {
    try {
      if (record.type !== 'payment' || record.to !== wallet) {
        await this.saveCursor(wallet, record.paging_token);
        return;
      }

      const tx: TransactionRecord = await record.transaction();
      const memoId =
        (tx.memo_type === 'id' || tx.memo_type === 'text') && tx.memo
          ? tx.memo
          : null;

      const assetCode =
        record.asset_type === 'native' ? 'XLM' : (record.asset_code ?? 'UNKNOWN');

      // Attempt attribution, but do not gate the write on it.
      let userId: string | null = null;

      if (memoId) {
        const { data: profile, error: profileError } = await this.supabase.client
          .from('profiles')
          .select('id')
          .eq('assigned_wallet', wallet)
          .eq('memo_id', memoId)
          .maybeSingle<{ id: string }>();

        if (profileError) {
          // Transient failure. Do not advance the cursor — the event will be
          // redelivered on reconnect rather than lost.
          this.logger.error(
            `Profile lookup failed for ${wallet}/${memoId}: ${profileError.message}`,
          );
          return;
        }

        userId = profile?.id ?? null;
      }

      const status = userId ? 'confirmed' : 'unattributed';

      // WRITE FIRST. Every inbound payment is recorded whether or not the
      // memo resolves, so a deposit is never lost to attribution failure.
      const { error: rpcError } = await this.supabase.client.rpc('record_deposit', {
        p_user_id: userId,
        p_operation_id: record.id,
        p_payment_hash: record.transaction_hash,
        p_amount: record.amount,
        p_asset_code: assetCode,
        p_wallet_address: wallet,
        p_memo_id: memoId,
        p_status: status,
        p_paging_token: record.paging_token,
      });

      if (rpcError) {
        // Write failed. Cursor stays put so the event is redelivered.
        this.logger.error(
          `Deposit record failed for op ${record.id}: ${rpcError.message}`,
        );
        return;
      }

      // Only now is it safe to advance.
      await this.saveCursor(wallet, record.paging_token);

      if (userId) {
        this.logger.log(
          `Recorded deposit ${record.amount} ${assetCode} -> user ${userId} (op ${record.id})`,
        );
      } else {
        this.logger.warn(
          `Recorded UNATTRIBUTED deposit ${record.amount} ${assetCode} on ${wallet} ` +
            `(op ${record.id}, memo ${memoId ?? 'none'}). Flagged for manual review.`,
        );
      }

      // Treasury conversion runs regardless of attribution — the position
      // needs cleaning either way.
      if (assetCode !== 'USDC') {
        void this.performSilentConversion(wallet, assetCode, record.amount).catch(
          (err) =>
            this.logger.error(
              `Silent conversion failed for ${wallet}: ${this.describeError(err)}`,
            ),
        );
      }
    } catch (error: unknown) {
      this.logger.error(
        `Unhandled error processing payment on ${wallet}: ${this.describeError(error)}`,
      );
    }
  }

  private async performSilentConversion(wallet: string, assetCode: string, amountStr: string): Promise<void> {
    const wallets = process.env.PLATFORM_WALLETS?.split(',').map(s => s.trim()) || [];
    const secrets = process.env.PLATFORM_SECRETS?.split(',').map(s => s.trim()) || [];
    const index = wallets.indexOf(wallet);
    const secret = secrets[index];
    
    if (!secret || secret.length === 0) {
      throw new Error(`No secret key found in .env for platform wallet ${wallet} (index ${index})`);
    }

    const signer = Keypair.fromSecret(secret);
    const aqua = new AquariusClient({ network: 'testnet', signer });

    let fromAsset;
    if (assetCode === 'XLM') {
      fromAsset = AquaXLM;
    } else {
      throw new Error(`Auto-conversion for ${assetCode} is not currently supported`);
    }

    const USDC_AQUA = AquaAsset.classic('USDC', 'GAHPYWLK6YRN7CVYZOO4H3VDRZ7PVF5UJGLZCSPAEIKJE2XSWF5LAGER');
    const amountStroops = BigInt(Math.floor(parseFloat(amountStr) * 10000000));

    const quote = await aqua.quote({
      from: fromAsset,
      to: USDC_AQUA,
      amountIn: amountStroops,
    });

    this.logger.log(`Executing Aquarius swap: ${amountStr} ${assetCode} -> ~${Number(quote.amountOut) / 10000000} USDC...`);
    const receipt = await quote.execute();
    
    this.logger.log(`Silent conversion successful! Received ${Number(receipt.amountOut) / 10000000} USDC for ${amountStr} ${assetCode}. TxHash: ${receipt.txHash}`);
  }

  private async loadCursor(wallet: string): Promise<string> {
    const { data, error } = await this.supabase.client
      .from('cursor_store')
      .select('cursor')
      .eq('wallet_address', wallet)
      .maybeSingle<{ cursor: string }>();

    if (error) {
      // Do NOT fall back to "now" — that silently discards every deposit
      // that arrived while the datastore was unreachable. Fail loudly and
      // let the reconnect logic retry the read.
      this.logger.error(
        `Could not load cursor for ${wallet}: ${error.message}. Refusing to start stream.`,
      );
      throw new Error(`Cursor read failed for ${wallet}: ${error.message}`);
    }

    // A missing row is the legitimate first-run case, not an error.
    return data?.cursor ?? 'now';
  }

  private async saveCursor(wallet: string, pagingToken: string): Promise<void> {
    const { error } = await this.supabase.client.from('cursor_store').upsert(
      {
        wallet_address: wallet,
        cursor: pagingToken,
        updated_at: new Date().toISOString(),
      },
      { onConflict: 'wallet_address' },
    );

    if (error) {
      this.logger.error(
        `Could not persist cursor for ${wallet}: ${error.message}`,
      );
    }
  }

  private describeError(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }
    try {
      return JSON.stringify(error);
    } catch {
      return String(error);
    }
  }
}
