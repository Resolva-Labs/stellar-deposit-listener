# Resolva Stellar Deposit Listener (NestJS)

## Project Description

This is the backend deposit-listening service for **Resolva**, a real-time Stellar deposit-tracking platform. Built with NestJS, it connects to the Stellar Horizon API and streams incoming payments across a pool of platform-controlled wallets, attributes each deposit to a user via the transaction memo, and durably records it in Supabase. 

### Non-USDC deposits are then silently swapped to USDC on the Aquarius AMM so user balances settle in a single asset.

The service is designed to run continuously (e.g. on a free-tier host) as a long-lived stream consumer rather than a request/response API — its only HTTP surface is a health-check endpoint used both for uptime monitoring and its own keep-alive cron job.

## How It Works

When the service boots up, it performs the following for **each** wallet in `PLATFORM_WALLETS`:

1. **Load Checkpoint** — Connects to Supabase and fetches the last known `cursor` (paging token) for the wallet from the `cursor_store` table.
2. **Stream Payments** — Opens a Stellar Horizon payment stream for the wallet, resuming exactly from its saved cursor so no events are missed or double-processed. If a stream errors out, it automatically reconnects after a 5s delay.
3. **Process Events** — Listens for incoming payment operations, ignoring anything that isn't an inbound payment or is missing a valid `id`/`text` memo.
4. **Attribute Deposits** — Matches the transaction memo against a user's `profiles` row (`assigned_wallet` + `memo_id`) in Supabase.
5. **Record** — On a match, calls the `record_deposit` Postgres stored procedure to atomically record the deposit.
6. **Auto-Convert (Aquarius)** — If the deposited asset isn't USDC (currently only XLM is supported), signs and submits a swap to USDC via the Aquarius DeFi SDK, using the platform wallet's own secret key.
7. **Save Checkpoint** — Updates the `cursor` in `cursor_store` to mark the event as processed, so a restart resumes exactly where it left off.

## Project Directory Structure

```
stellar-debposit-listener-backend/
├── src/
│   ├── app.controller.ts          # GET /health endpoint
│   ├── app.module.ts              # Root module — wires up all feature modules
│   ├── main.ts                    # Nest app bootstrap / entrypoint
│   │
│   ├── keep-alive/
│   │   ├── keep-alive.module.ts
│   │   └── keep-alive.service.ts  # Cron job pinging /health every 14 min
│   │
│   ├── stellar/
│   │   ├── stellar.module.ts
│   │   └── stellar-stream.service.ts  # Core: Horizon payment streams,
│   │                                  # deposit attribution, cursor
│   │                                  # checkpointing, Aquarius auto-swap
│   │
│   └── supabase/
│       ├── supabase.module.ts     # Global module exporting SupabaseService
│       └── supabase.service.ts    # Supabase client (service-role key)
│
├── .env                           # Local environment values (gitignored)
├── .env.example                   # Documented template of required env vars
├── .gitignore
├── nest-cli.json                  # Nest CLI config (source root, build output)
├── package.json
├── package-lock.json
├── tsconfig.json
└── README.md
```

## Setup & Execution

1. **Configure Environment Variables**:
   Copy the example environment file and fill in your specific values.
   ```bash
   cp .env.example .env
   ```
   Required variables:
   - `HORIZON_URL`: The Stellar Horizon server URL (e.g., `https://horizon-testnet.stellar.org`).
   - `SUPABASE_URL`: Your Supabase project URL.
   - `SUPABASE_SERVICE_ROLE_KEY`: Your Supabase service role key (required to bypass Row Level Security when recording deposits).
   - `PLATFORM_WALLETS`: A comma-separated list of Stellar public keys (wallets) that the platform is monitoring.
   - `PLATFORM_SECRETS`: A comma-separated list of Stellar secret keys, **positionally matched** to `PLATFORM_WALLETS` (same order/index). Used to sign the automatic Aquarius swap when a deposit isn't already USDC. Treat this like any hot-wallet private key — never commit it, never log it, restrict who has access to the deploy environment.

   Optional:
   - `PORT`: HTTP port for the Nest application (defaults to `3001`).
   - `PUBLIC_URL`: Full public URL used by the keep-alive cron to ping `/health` (defaults to `http://localhost:$PORT/health`, which only works for the keep-alive job when running locally).

2. **Install Dependencies**:
   ```bash
   npm install
   ```

3. **Run the Application**:
   ```bash
   # Development (with hot-reload)
   npm run start:dev

   # Production build
   npm run build
   npm run start:prod
   ```

## Health Check

- `GET /health`: Returns `{ status: 'ok', service: 'resolva-backend', timestamp: <ISO date> }` when the service is up and running. Also used internally by the keep-alive cron job to prevent free-tier hosting from spinning down due to inactivity.

## Security Notes

- `SUPABASE_SERVICE_ROLE_KEY` and `PLATFORM_SECRETS` are highly sensitive — both allow bypassing normal safeguards (RLS, and on-chain wallet control respectively). Keep them out of version control (`.env` is gitignored) and out of logs.
- The Aquarius auto-conversion path executes a swap with no explicit slippage or amount-out validation beyond whatever the SDK's `quote()`/`execute()` calls enforce by default — review this before enabling on mainnet with real funds.
