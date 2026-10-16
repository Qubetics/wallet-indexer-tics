# Qubetics TICS Processes

Background service for the Qubetics EVM chain (TICS). It reads new, missed and specific blocks, finds deposits and withdrawals (TICS and tokens) for wallets managed by Qubetics Wallet, tracks transaction status, updates balances and sends push notifications. It has no HTTP API: it works through scheduled jobs and Kafka consumers.

## Prerequisites

| Requirement | Version / notes |
|---|---|
| Node.js | 18.18.2 (same as the Docker image) |
| npm | 9 or newer |
| ZooKeeper | Holds the service configuration (see below) |
| MySQL | Shared wallet database (read and write hosts) |
| Redis | Block positions and cached token and wallet lookups |
| Kafka | Message broker shared with the backend and the other chain services |
| Qubetics EVM node | Access to a Qubetics EVM RPC endpoint |

## Configuration

The service reads two environment variables from a `.env` file in the project root. Copy the example and fill it in:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `ZOOKEEPER_HOST` | ZooKeeper address, for example `localhost:2181` |
| `ZOOKEEPER_PATH` | ZooKeeper node that holds the configuration, for example `/qubetics_wallet` |

Every other setting is read from that ZooKeeper node, which holds a single JSON document. The main keys are:

| Area | Keys |
|---|---|
| Blockchain | `TICS_RPC_URL`, `TICS_WALLET_ADDRESS`, `MIN_BLOCK_CONFORMATIONS`, `STATIC_COIN_FAMILY` |
| Database | `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_HOST_WRITE`, `DB_HOST_READ`, `DB_PORT` |
| Redis / Kafka | `REDIS_CONN`, `KAFKA_BROKER`, `KAFKA_CLIENT_ID`, and the `TICS_*_TOPIC` / `TICS_*_GROUP_ID` topic and consumer group names |
| Push notifications | `FIREBASE_SERVICE_ACCOUNT` |
| Node alert emails | `MAILGUN_API_KEY`, `MAILGUN_BASE_URL`, `MAILGUN_DOMAIN`, `FROM_GMAIL`, `NODE_ISSUE_ALERT_EMAIL` |

The service refuses to start if `FIREBASE_SERVICE_ACCOUNT` is missing (see `src/config/required-secrets.ts`).

Never commit `.env` or any configuration values to the repository.

## Run locally

```bash
# 1. Install dependencies
npm install

# 2. Build and start (compiles TypeScript into build/ and starts the service)
npm run start
```

On start-up the service:

1. connects to ZooKeeper and loads the configuration,
2. checks the required keys,
3. connects to MySQL, Redis and Kafka,
4. starts the scheduled block readers, pending withdrawal check and wallet balance update, and the Kafka consumers (block transactions, deposits and withdrawals, transaction status, balance updates).

A successful start logs `Zookeeper connected...`, `producer connetced`, `Connected to redis client successfully` and `Database connected successfully.`, followed by the block reader output.

For development, `npm run dev` compiles and runs the service with `nodemon`.

## Other commands

| Command | Description |
|---|---|
| `npm run compile` | Compile TypeScript into `build/` without starting |
| `npm test` | Compile and run the tests in `test/` |

## Run with Docker

```bash
docker build -t wallet-indexer-tics .
docker run -d --name wallet-indexer-tics \
  -e ZOOKEEPER_HOST=<zookeeper-host:port> \
  -e ZOOKEEPER_PATH=<config-node> \
  wallet-indexer-tics
```

The service does not listen on a port, so no port mapping is needed. ZooKeeper, MySQL, Redis, Kafka and the Qubetics EVM node must be reachable from inside the container.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Missing ZooKeeper config keys: ...` | A required key is missing from the ZooKeeper node |
| Hangs at start-up with no `Zookeeper connected...` line | ZooKeeper unreachable, or wrong `ZOOKEEPER_HOST` / `ZOOKEEPER_PATH` |
| `ECONNREFUSED` for Redis, MySQL or Kafka | The service is not running, or the address in the configuration is wrong |
| Blocks not advancing | Check `TICS_RPC_URL` and that the node is synced |
