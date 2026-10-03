# Audit Trail Service

Phases B0–B3: NestJS foundation, audit event contract, PostgreSQL persistence, and Kafka ingestion
for the Real-Time Audit Trail Explorer.

## Requirements

Node.js 22 or newer, npm, and Docker with Docker Compose v2.

## Local development

```powershell
npm ci
Copy-Item .env.example .env
docker compose up -d --wait
npm run db:migrate
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic audit.events --partitions 1 --replication-factor 1
npm run start:dev
```

`GET http://localhost:3000/health` returns `{"status":"ok"}`. This is process
liveness only; database connections are opened on demand by the repository.
`APP_PORT` defaults to 3000 and is validated at startup. `APP_ENV` accepts
development, test, or production. Shell environment variables override `.env`.

PostgreSQL is available on localhost:5432 and Kafka on localhost:9092. Both use
persistent named volumes. Compose credentials and plaintext Kafka are intended
for local development. Host ports bind to loopback. No Kafka UI is included.
`DATABASE_*` settings configure the application connection pool and migration CLI.
Compose uses the database name, user, password, and host port settings.
`KAFKA_*` settings configure the audit consumer.

If port 5432 is already occupied or reserved, set `DATABASE_PORT=5433` in `.env`
before starting Compose (or use `$env:DATABASE_PORT='5433'` in PowerShell).

```powershell
docker compose ps
npm run format
npm run lint
npm run typecheck
npm test
npm run build
npm start
docker compose down
```

`docker compose down` retains data volumes. Do not remove volumes unless you
intend to delete local data.

## Structure and scope

The root application module wires configuration and the health controller under
`src/common/`. The event contract and runtime schema live under `src/contracts/`.
The repository interface lives under `src/audit/`; its PostgreSQL implementation,
connection pool, and explicit migrations live under `src/database/`.
Kafka ingestion lives under `src/kafka/`.
The auth, dlq, feature-flags, realtime, simulator, and docs directories
are placeholders for later phases.
The supplied rules and plan are in `BACKEND_AGENTS.md` and
`BACKEND_IMPLEMENTATION_PLAN.md`.

No business APIs, Auth0, LaunchDarkly, SSE, retry, DLQ, or simulator logic is
implemented. Next is B4, the simulator.

## Audit event contract (B1)

`auditEventSchema` in `src/contracts/audit-event.schema.ts` validates unknown
payloads with Zod. `AuditEvent` in `audit-event.ts` is inferred from that schema.
Use `auditEventSchema.safeParse(payload)` to receive a success/error result, or
`auditEventSchema.parse(payload)` to throw on invalid input. Both return a typed
event on success. B3 decodes Kafka JSON before applying this schema.

All twelve envelope fields are required: `eventId`, `schemaVersion`, `eventType`,
`timestamp`, `tenantId`, `correlationId`, `actor`, `resource`, `action`, `changes`,
`context`, and `metadata`. Only schema version `1.0` and the five types in
`event-types.ts` are supported. IDs and action must be nonblank strings; IDs
need not be UUIDs. Timestamps must be valid ISO calendar date-times with `Z` or
an explicit timezone offset. No values are coerced or normalized.

Schema 1.0 defines these nested shapes:

- `actor`: required `id`; optional validated `email` and nonblank `role`.
- `resource`: required nonblank `type` and `id`.
- `changes`: required `before` and `after`, each a JSON object or `null`.
- `context` and `metadata`: required JSON objects, which may be empty. Nested
  JSON arrays, objects, strings, finite numbers, booleans, and null are allowed.

Unknown fields in the envelope, actor, resource, and changes are rejected.
Context, metadata, and state snapshots intentionally allow arbitrary JSON keys.
Producers must omit credentials and secrets as required by the repository rules;
schema validation is not a sensitive-data sanitizer. B2 rejects prohibited
credential field names recursively at the storage boundary. It does not log
payloads or expose HTTP audit endpoints.

## PostgreSQL persistence (B2)

Run `npm run db:migrate` before using the repository. This builds the application
and applies the versioned migration explicitly. Startup does not create or alter
tables. Each migration runs in a transaction, records its ID in `schema_migrations`,
and takes an advisory lock to serialize concurrent migration runners. Repeated
runs are safe. B2 supplies a forward migration only; it has no destructive rollback
command. Do not edit an already applied migration.

`DatabaseModule` provides the `AuditRepository` abstraction through Nest dependency
injection. Its methods are:

- `create(payload)`: validates with the B1 schema, rejects credential fields,
  and returns `{ status: 'created', event }` or `{ status: 'duplicate' }`.
- `findByEventId(tenantId, eventId)`: returns an event or `null`.
- `findMany(tenantId, options)`: newest persisted events first.
- `findByCorrelationId(tenantId, correlationId, options)`: event-time order.
- `getStatistics(tenantId)`: tenant total and counts for all five event types.

Every read requires a nonblank tenant ID from trusted server context. Authenticated
tenant extraction remains B6. Read options accept `limit` (default 100, maximum
1000), `offset` (default 0), and optional `eventType`. Ordering uses UUID as a stable
tie-breaker. These are internal repository options, not REST API DTOs.

PostgreSQL enforces global `UNIQUE(event_id)`. Atomic `ON CONFLICT DO NOTHING`
prevents duplicate rows under concurrent deliveries, does not overwrite the first
event, and does not return another tenant's existing event. Records add `id` and
`createdAt` to the contract. Timestamps use `TIMESTAMPTZ` and are returned in UTC
with millisecond precision. JSONB preserves the snapshots, context, and metadata;
`metadata.severity`, when a string, also populates the severity column.

`DATABASE_SSL=true` enables TLS with certificate verification. Production requires
an explicit password and defaults TLS to true; local development defaults match
Compose. Connections have bounded pool size and connection/query timeouts, and
the pool closes on application shutdown. `/health` remains a liveness check.

The storage boundary rejects credential keys such as passwords, access/refresh
tokens, secrets, API keys, authorization headers, and private keys, including
case and separator variations inside nested arrays/objects. Producers must also
avoid credentials concealed in arbitrary text or unrecognized field names.
Privileged disclosure and response masking remain B10.

### Persistence tests

`npm test` runs all unit and real PostgreSQL/Kafka integration tests. Start both services
first and set `DATABASE_*` to a disposable local/test database whose user can
create schemas. Each integration run creates unique temporary schemas and
removes only those schemas afterward; it does not truncate the application table.
Connection or migration failures fail the tests rather than silently skipping.

```powershell
# Override the port when using the 5433 Compose mapping.
$env:DATABASE_PORT='5433'
npm test
npm run test:unit
npm run test:integration
```

## Kafka ingestion (B3)

On application startup, the consumer connects to `KAFKA_BROKERS` (comma-separated
host:port entries), subscribes to `KAFKA_TOPIC` (default `audit.events`), and consumes
JSON payloads using `KAFKA_GROUP_ID` (default `audit-trail-service-v1`).
`KAFKA_CLIENT_ID` identifies the client. `KAFKA_ENABLED=false` disables consumption
for maintenance. The topic must exist before startup; the development command
above provisions one partition with replication factor one. Provision appropriate
partition/replication counts for other environments.

Flow: Kafka -> UTF-8/JSON decode -> B1 schema 1.0 validation -> B2 PostgreSQL
repository. No producer/simulator or extra topics are added in B3. With a new
group, existing messages are read from the beginning; an existing group resumes
at its committed offsets. Each message's next offset is committed explicitly
after successful persistence or duplicate detection. Invalid JSON/schema,
unsupported versions/types, tombstones, and prohibited credential fields are
rejected, logged without payloads, and committed so ingestion can continue.
Rejections are discarded in B3; there is no DLQ yet.

A persistence failure or exhausted Kafka protocol failure stops the consumer
without acknowledging the failed message. KafkaJS allows five protocol-level
retries for broker/coordinator communication; persistence errors are explicitly
non-retryable in B3. Automatic consumer restart is disabled; fix the failure and
restart the application to resume from the last committed offset. If persistence
succeeded before an offset commit failed, B2 idempotency makes redelivery safe.
Bounded retry and DLQ are reserved for B8. `/health` remains process liveness and
does not report whether the consumer is running.

Structured ingestion logs include topic, partition, offset, and available
eventId, tenantId, correlationId, eventType, and context.service. Raw payloads,
headers, validation values, and driver error details are not logged. The consumer
drains and disconnects before the database pool closes. The migration CLI starts
only database modules and never starts Kafka consumption.

Kafka configuration currently targets plaintext development brokers; TLS/SASL
configuration is not provided. Integration tests use a unique topic, consumer
group, and PostgreSQL schema, deleting only their own resources. The test Kafka
principal must be able to create/delete topics and consumer groups. Tests cover
valid/duplicate/invalid delivery, committed offsets, logs, and resuming an
uncommitted write after an operator restart.
