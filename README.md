# Audit Trail Service

Phases B0–B10: NestJS foundation, audit event contract, PostgreSQL persistence, Kafka ingestion, simulator, read APIs, Auth0 authorization, SSE, retry/DLQ, DLQ inspection/replay, and sensitive-data masking
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
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic audit.events.retry --partitions 1 --replication-factor 1
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --if-not-exists --topic audit.events.dlq --partitions 1 --replication-factor 1
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
The standalone simulator lives under `tools/simulator/`.
The feature-flags and docs directories
are placeholders for later phases.
The supplied rules and plan are in `BACKEND_AGENTS.md` and
`BACKEND_IMPLEMENTATION_PLAN.md`.

Read APIs are implemented under `src/audit/`, Auth0 guards under `src/auth/`, and SSE under `src/realtime/`.
DLQ inspection and replay live under `src/dlq/`, with PostgreSQL storage under
`src/database/`. Response masking lives under `src/common/masking/`.
LaunchDarkly capability flags are implemented. Next is B12, client-ready polish.

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

Every read requires a nonblank tenant ID from trusted server context. HTTP reads
use the verified access token tenant. Read options accept `limit` (default 100, maximum
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
Response masking and allowlisted privileged disclosure are described under B10 below.

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
rejected and logged without payloads. B8 routes rejected messages to DLQ before
committing so ingestion can continue.

KafkaJS allows five protocol-level retries for broker/coordinator communication.
B8 adds application retries for classified transient persistence failures, then
DLQ routing. A routing/commit failure or exhausted Kafka protocol failure stops
the consumer without acknowledging the failed message. Automatic consumer restart
is disabled; fix the infrastructure and restart to resume. If persistence succeeded
before an offset commit failed, B2 idempotency makes redelivery safe. `/health`
remains process liveness and does not report whether the consumer is running.

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
transient write recovery through Kafka retry and exhausted failures sent to DLQ.

## Simulator (B4)

Build once with `npm run build`, then run the simulator in a second terminal
while the application consumer is running. It loads `.env`, with shell variables
taking precedence, and uses the existing `KAFKA_BROKERS`, `KAFKA_CLIENT_ID`, and
`KAFKA_TOPIC` settings. It never connects to PostgreSQL or starts an application
consumer. `KAFKA_ENABLED` controls the application consumer, not this producer.
The target topic must already exist.

In PowerShell, use `npm.cmd` so simulator flags are forwarded unchanged; in
other shells, use `npm`.

```powershell
npm.cmd run simulator -- --help
npm.cmd run simulator -- --type USER_LOGIN
npm.cmd run simulator -- --type USER_ROLE_CHANGED
npm.cmd run simulator -- --type DATA_EXPORTED
npm.cmd run simulator -- --type CONFIG_CHANGED
npm.cmd run simulator -- --type PAYMENT_REFUNDED
npm.cmd run simulator -- --count 100
npm.cmd run simulator -- --count 1000
npm.cmd run simulator -- --type CONFIG_CHANGED --count 100 --tenant demo-tenant
npm.cmd run simulator -- --scenario correlated --tenant demo-tenant --correlation demo-flow
npm.cmd run simulator -- --scenario duplicate --type PAYMENT_REFUNDED
```

The default `events` scenario publishes one event. `--type all` cycles through
the five supported event types for bulk runs; `--count` accepts only 1, 100, or 1000. Each event receives a fresh UUID and schema version `1.0` and is validated
with the backend schema before publishing. Example state changes and actors are
synthetic and contain no credentials. The default tenant is `demo-tenant`.

`correlated` publishes five ordered events, one of each type, with a shared
correlation ID and tenant. `duplicate` sends an identical event twice, including
its eventId, so the B2/B3 pipeline should create one row. For these scenarios,
`--count` must remain 1; a correlated flow always includes all five types.
`--correlation` may also group ordinary events. Correlation IDs are Kafka keys
to preserve ordering within a partition.

Publishing uses sequential batches of up to 100 with acknowledgements from all
in-sync replicas. JSON logs contain eventId, tenantId, correlationId, eventType,
service, and topic after acknowledgement, followed by a completion count. Kafka
acknowledgement does not guarantee that the consumer has already persisted the
event. Failure exits nonzero and may leave earlier batches published; rerunning
generates fresh IDs. B8 adds the retry and DLQ demos described below.

`npm run build` builds the service in `dist/` and simulator in `dist-simulator/`.
`npm run typecheck` checks both. Unit tests cover generation, CLI arguments, and
producer failures. The real integration test publishes all five types, 100/1000
events, correlated flow, and duplicates through Kafka, then checks persistence
only via the backend repository in its isolated test schema.

## Read REST API (B5)

All four read endpoints require a verified Auth0 bearer access token with `audit:read`.
The tenant is extracted from its configured namespaced claim. Query parameters and headers cannot select a tenant.

| Endpoint                             | Response                                             |
| ------------------------------------ | ---------------------------------------------------- |
| `GET /audit/events`                  | Paginated events, newest persistence time first      |
| `GET /audit/events/:id`              | One event by its database UUID (`id`, not `eventId`) |
| `GET /audit/timeline/:correlationId` | Paginated events, ascending event timestamp          |
| `GET /audit/statistics`              | Total and counts for all five event types            |

List and timeline responses have `{ items, total, page, limit, totalPages }`.
Pages start at 1; the default limit is 25 and maximum is 100. UUID breaks ordering
ties. Empty/out-of-range pages return empty items while preserving the matching
total. Page rows and total use the same PostgreSQL statement snapshot. Offset
pagination may shift between separate requests while new events are ingested.

List, timeline, and statistics accept these optional filters, combined with AND:

- `eventType`: one of the five supported types.
- `actor`: exact actor ID or email.
- `resourceType`, `resourceId`, `correlationId`: exact matches.
- `service`: exact string in `context.service`.
- `severity`: exact value from the severity column (`metadata.severity` at insert).
- `from`, `to`: inclusive event timestamp bounds, ISO date-times with timezone.

Text filters preserve the supplied value and allow up to 256 characters.
Statistics accepts filters but not page/limit. A timeline's path supplies its
correlationId; a conflicting query correlationId is rejected. Unknown query keys,
repeated/array values, unsupported event types, invalid pagination/date ranges,
and malformed detail UUIDs return 400. A missing or other-tenant detail returns
404; an empty timeline returns 200 with an empty page. Database failures return a
generic 503 without driver details. Detail requests
accept no query keys.

```powershell
Invoke-RestMethod -Headers @{ Authorization = 'Bearer ' + $env:AUDIT_ACCESS_TOKEN } 'http://localhost:3000/audit/events?page=1&limit=25'
Invoke-RestMethod -Headers @{ Authorization = 'Bearer ' + $env:AUDIT_ACCESS_TOKEN } 'http://localhost:3000/audit/events?eventType=USER_LOGIN&service=demo-identity&severity=INFO'
Invoke-RestMethod -Headers @{ Authorization = 'Bearer ' + $env:AUDIT_ACCESS_TOKEN } 'http://localhost:3000/audit/events?from=2026-10-04T00:00:00Z&to=2026-10-04T23:59:59Z'
Invoke-RestMethod -Headers @{ Authorization = 'Bearer ' + $env:AUDIT_ACCESS_TOKEN } 'http://localhost:3000/audit/timeline/demo-flow?page=1&limit=25'
Invoke-RestMethod -Headers @{ Authorization = 'Bearer ' + $env:AUDIT_ACCESS_TOKEN } 'http://localhost:3000/audit/statistics?resourceType=payment'
# Replace the UUID with an id from a list response.
Invoke-RestMethod -Headers @{ Authorization = 'Bearer ' + $env:AUDIT_ACCESS_TOKEN } 'http://localhost:3000/audit/events/00000000-0000-4000-8000-000000000001'
```

Responses include actor/state/context/metadata fields subject to B10 masking.
Existing prohibited-field
protection is also checked before returning records, so credentials inserted
outside the repository fail closed rather than being exposed. No write, export,
SSE, DLQ, or replay endpoints are added in B5.

API integration tests use Supertest with a real PostgreSQL schema and the actual
controller/service/repository wiring. Kafka consumption is disabled in those HTTP
tests; ingestion and simulator integration tests still run separately in the full
suite. Every temporary test schema is removed afterward.

## Auth0 and tenant isolation (B6)

Set `AUTH0_DOMAIN` to the Auth0 tenant/custom hostname without a scheme or path,
`AUTH0_AUDIENCE` to the registered API identifier, and `AUTH0_TENANT_CLAIM` to an
HTTPS namespaced claim (default `https://audit-trail.example.com/tenantId`).
Configure the Auth0 API for RS256 and enable RBAC and **Add Permissions in the
Access Token**. Define roles `AUDIT_VIEWER`, `AUDIT_ANALYST`, and `AUDIT_ADMIN`
and permissions `audit:read`, `audit:export`, `audit:view-sensitive`, `audit:replay`,
and `audit:manage`. Assign `audit:read` to readers. Roles alone never grant access.
Only read authorization is used by the current endpoints; other capabilities
remain in their planned phases.

Use an Auth0 post-login Action to add the configured tenant claim to the access
token from an administrator-controlled membership source (for example protected
app_metadata). Never derive it from client input or user-editable metadata.
A user with multiple tenant memberships must receive a token for one authorized
tenant. Tenant IDs must match event tenant IDs exactly. API tokens must contain a
subject, expiration, and nonblank tenant claim. Do not use an ID token.

The backend verifies RS256 signatures using the configured issuer's HTTPS JWKS,
issuer, audience, expiration, and not-before time. JWKS keys are cached for ten
minutes with a bounded cache, five-second fetch timeout, and ten fetches/minute.
No token-supplied URL is fetched. Missing/invalid tokens or tenant claims return
401; valid tokens without `audit:read` return 403. Raw tokens and verification
errors are never logged or reflected. Unconfigured Auth0 fails closed for audit
reads while health/ingestion can run; partial/invalid configuration fails startup.
The old `APP_AUDIT_TENANT_ID` fallback is removed. All HTTP read queries pass the
verified tenant explicitly to the repository. Other-tenant details return 404.

Tests verify locally signed tokens against a real local JWKS HTTP endpoint and
exercise the actual guards and tenant isolation with PostgreSQL. No live Auth0
account is needed for tests; deployers must configure their Auth0 API, roles, and
trusted tenant claim. Capability flags are documented under B11 below.

## Live stream (B7)

`GET /audit/stream` requires a bearer access token with `audit:read` and a verified
tenant claim, just like REST reads. Query parameters are rejected, and client
tenant headers cannot change the tenant. Use a fetch-based SSE client that can
send the Authorization header; native browser EventSource cannot set this header.
Do not put access tokens in the URL.

```powershell
curl.exe -N -H "Authorization: Bearer $env:AUDIT_ACCESS_TOKEN" http://localhost:3000/audit/stream
```

Kafka ingestion publishes internally only after PostgreSQL returns a newly
created record. Duplicates, rejected messages, and failed writes produce no
event. The process-local bus filters by the authenticated tenant before delivery.
Each `audit-event` SSE message contains `eventId`, `eventType`, `timestamp`,
`tenantId`, `correlationId`, `actor`, `resource`, and `action`. It omits snapshots,
context, and metadata. Credential-field protection still applies.

The SSE `id` is the JSON-encoded eventId string to prevent control characters in
producer IDs from injecting protocol lines. Prefer the data's `eventId` for
deduplication; JSON.parse the SSE id if using it. Messages advertise a 3000ms
reconnect delay. Heartbeat messages arrive every 15 seconds, without an event ID.
Disconnects release subscriptions and timers. Streams close at token expiry;
refresh the token before reconnecting.

This is live delivery without a durable replay buffer. `Last-Event-ID` is accepted
but does not replay missed events. On reconnect, refresh tenant-scoped REST data
and deduplicate live events by eventId. A crash between commit and publication can
lose a live notification while the row remains available through REST. With
multiple service replicas in the same Kafka group, a connection receives events
consumed by its own process only; this implementation targets a single replica.
Slow-client queues are managed by Nest's SSE transport and have no explicit
application limit in B7. SSE tests cover real HTTP framing, guards, tenant
isolation, reconnection, heartbeat/expiry, and persistence-before-publication.

## Bounded retry and DLQ (B8)

Provision all three topics before startup; automatic topic creation is disabled.
The consumer subscribes to `KAFKA_TOPIC`, `KAFKA_RETRY_TOPIC`, and, since B9,
`KAFKA_DLQ_TOPIC` for durable indexing. DLQ messages never enter ingestion or retry.
Defaults are `audit.events`, `audit.events.retry`, and `audit.events.dlq`.
For a custom source topic, omitted retry/DLQ settings derive `.retry`/`.dlq` from
it. All three topics must be distinct. Configure retention and broker ACLs for
these audit payloads; producers should publish only to the source topic.

`KAFKA_MAX_RETRIES` defaults to 3 (allowed 0–10), meaning one initial attempt plus
at most three retries. `KAFKA_RETRY_DELAY_MS` defaults to 1000 (allowed 0–10000).
Retry envelopes retain a due timestamp; the retry handler waits until due,
heartbeating during the bounded delay. Waiting blocks that consumer's processing,
and retry routing can reorder events relative to later source-topic messages.

Known database connection failures, timeouts, serialization failures, deadlocks,
and temporary resource/server failures are retryable. Constraint, permission,
schema/configuration failures and unclassified errors go directly to DLQ.
Malformed JSON/UTF-8, invalid schema, unsupported versions, and credential fields
never enter the retry loop. Retry envelope validation rejects poisoned counters,
wrong source topics, and unsupported reasons. Do not lower the retry cap with
in-flight retry records unless routing those records to DLQ is acceptable.

Retry and DLQ records contain `originalEvent`, `failureReason`, `retryCount`,
`failedAt`, `sourceTopic`, and `correlationId`; retry records also contain
`retryAt`. Reasons are safe classifications, never driver messages. Valid original
events are preserved. Credential-bearing payloads and malformed raw bytes are
omitted (`originalEvent: null`), with `originalPayloadOmitted` and a SHA-256 digest
for identification. Their correlationId is null if unavailable or omitted.
This exception prevents DLQ storage from retaining prohibited credentials; such
records cannot be reconstructed from the DLQ. Producers must also avoid credentials
concealed in arbitrary text or unrecognized field names.

The source offset commits only after persistence/duplicate detection or a retry/DLQ
send acknowledged by all in-sync replicas. Failed routing leaves it uncommitted.
Routing is at least once, without an atomic send-and-offset transaction, so a crash
between send and commit can duplicate failure envelopes. Each retry chain remains
bounded; PostgreSQL eventId uniqueness prevents duplicate audit rows. SSE emits
only after a new persistence succeeds, including successful retries.

```powershell
npm.cmd run simulator -- --scenario dlq --tenant demo-tenant
npm.cmd run simulator -- --scenario retry --tenant demo-tenant
```

The DLQ demo publishes a synthetic event with unsupported schemaVersion `2.0`
through the source Kafka topic. The retry demo publishes a valid event; run it
during a controlled transient database outage to observe retries, then restore
the database before the cap to observe recovery. The simulator does not disable
the database or add production fault-injection switches. Both demos use one event.
The integration suite injects temporary database errors in its isolated repository
and verifies retry recovery, bounded exhaustion, metadata, credential omission,
and the DLQ simulator using real Kafka. B9 adds the APIs described below.

## DLQ inspection and replay (B9)

Run `npm run db:migrate` before starting the upgraded application. Migration 002
adds `audit_dlq` and `audit_dlq_replays`; migration 001 is unchanged. Startup never
creates tables. The consumer indexes validated, credential-free DLQ envelopes
from Kafka and commits their offsets only after storage succeeds. Indexing failure
stops the consumer without committing; restart after repairing the database.
Invalid or credential-bearing envelopes published directly to DLQ are discarded
with safe logs instead of being stored or recursively routed.

| Endpoint                          | Permission                      | Behavior                                     |
| --------------------------------- | ------------------------------- | -------------------------------------------- |
| `GET /audit/dlq`                  | `audit:read`                    | Tenant-scoped page of failure records        |
| `GET /audit/dlq/:eventId`         | `audit:read`                    | One failure by original eventId              |
| `POST /audit/dlq/:eventId/replay` | `audit:read` and `audit:replay` | Reserve and publish one replay through Kafka |

List pagination uses page 1 and limit 25 by default, maximum limit 100, and returns
`{ items, total, page, limit, totalPages }`, newest first with UUID tie-breaking.
Rows and count use the same repeatable-read snapshot. Unknown query keys and
invalid pagination return 400. Detail and replay accept no query keys. Replay
accepts an empty body only; clients cannot replace payloads or select a tenant or
topic. Missing or other-tenant events return 404. All endpoints use the verified
Auth0 tenant, never client filtering. Unauthorized requests return 401/403.

Records include `envelope`, `replayStatus`, eventId/tenantId, database id, and
createdAt. A unique tenant/eventId pair keeps the first failure and preserves its
replay state across duplicate deliveries and subsequent failures. Kafka location
uniqueness also prevents duplicate storage of unassigned envelopes. Records without
a trustworthy original tenantId and eventId are stored for operator investigation
but excluded from tenant APIs; omitted original payloads cannot be replayed.

Replay revalidates schema 1.0 and credential protection, verifies event/tenant IDs
and the configured source topic, and returns 409 for unsupported or unreplayable
events. It never repairs invalid payloads or writes directly to `audit_events`.
A transaction reserves one attempt and records the authenticated subject and
request time in `audit_dlq_replays` before publishing. Concurrent requests and all
later requests for that tenant/event return 409. Replay publishes the original
event, preserving eventId and correlationId, with replay headers to the configured
source topic and waits for Kafka acknowledgements. The ordinary validation,
idempotent persistence, bounded retry/DLQ, and SSE pipeline handles it afterward.
DLQ ingestion never automatically replays events.

Successful publication returns 202 `{ eventId, replayId, status: "published" }`;
this acknowledges Kafka publication, not eventual persistence. Audit records store
reserved/published/failed state and timestamps. Structured replay logs include
available eventId, tenantId, correlationId, eventType, service, subject, and replayId.
Errors return generic 503 responses without driver/broker details.

```powershell
$headers = @{ Authorization = 'Bearer ' + $env:AUDIT_ACCESS_TOKEN }
Invoke-RestMethod -Headers $headers 'http://localhost:3000/audit/dlq?page=1&limit=25'
# Replace demo-event with an eventId from the list; URL-encode it when necessary.
Invoke-RestMethod -Headers $headers 'http://localhost:3000/audit/dlq/demo-event'
Invoke-RestMethod -Method Post -Headers $headers -ContentType 'application/json' -Body '{}' 'http://localhost:3000/audit/dlq/demo-event/replay'
```

The database reservation and Kafka publication are not an atomic transaction.
A crash or ambiguous send can leave a reserved/failed attempt blocked, even if
Kafka accepted it. Publication followed by a failed status update returns 503 and
retains the reservation. There is no automatic reset or resend; operators must
reconcile the replay audit and broker state to avoid loops. This conservative limit
allows one manual replay per tenant/event. Kafka retention bounds historical DLQ
backfill; indexing is eventually consistent and reads use the database index.
Run the consumer to index DLQ messages; disabling Kafka consumption pauses indexing
but authorized replay can still publish when the broker is available. Sensitive-data
masking and existing credential protection apply to DLQ reads.

## Sensitive-data masking (B10)

`SensitiveDataService` applies a server-owned policy recursively to JSON objects
and arrays. A global response interceptor applies it to audit REST responses,
DLQ list/detail responses, and each SSE message. It creates new objects and never
mutates stored events, failure envelopes, internal bus events, or Kafka replay
payloads. No database migration or new environment variable is required.

By default, keys `email`, `emailAddress`, `phone`, `phoneNumber`, `mobilePhone`,
`cardNumber`, and `pan` have their entire value replaced with `[MASKED]`, including
values nested in changes, context, metadata, and arrays. Matching ignores case
and separators, so `card_number` and `Card-Number` are also masked. Masked values
are placeholders rather than partial email/phone/card disclosure. IDs, timestamps,
pagination, statistics, and SSE event IDs are preserved.

Only a verified access-token permission `audit:view-sensitive` enables privileged
disclosure. The explicit allowlist reveals **only string actor.email** in canonical
event responses: detail, list/timeline items, DLQ original events, and SSE data.
Other emails, phone numbers, and card numbers remain masked even for privileged
users. Roles, client headers, and query parameters never grant disclosure. Existing
read/replay permissions and tenant isolation remain required. Add fields or allowed
paths only through a reviewed change to `SENSITIVE_DATA_POLICY`; clients cannot
configure the policy. Authenticated REST responses use private/no-store cache
headers and vary by Authorization to prevent reuse across permission levels.

Password, access/refresh token, secret, API key, authorization header, and private
key fields are always `[REDACTED]` by the reusable service, regardless of permission.
The same credential-key classification protects storage and responses. Existing
storage/read validation still rejects credential-bearing records with a safe error
before response masking; privileged access cannot bypass that protection.

This policy matches configured field names, not arbitrary text contents or unknown
field aliases. Producers must not hide credentials in free text or unrecognized
keys. Masking controls returned values, not filter semantics: actor email filters
still query stored values, and counts may reveal whether a filter matched. Tests
cover nested masking, field-name variations, credential redaction, the disclosure
allowlist, mutation safety, actual REST/DLQ/SSE permissions, and raw replay behavior.
Sensitive disclosure also requires the audit-sensitive-data-view flag described below.

## LaunchDarkly capability flags (B11)

Set `LAUNCHDARKLY_SDK_KEY` to a server-side SDK key using your secret environment configuration. Never commit it or send it to a browser. An absent key starts no SDK connection and disables flagged capabilities. Initialization is asynchronous: until initialized, evaluations return false without delaying startup. SDK evaluation errors, missing flags, and non-boolean results also fail closed. SDK shutdown closes its connection; SDK logs omit raw messages and credentials.

Create these boolean flags, with false defaults:

| Flag                      | Required permission                         | Capability                                       |
| ------------------------- | ------------------------------------------- | ------------------------------------------------ |
| audit-live-stream         | audit:read                                  | Open an SSE connection                           |
| audit-data-export         | audit:export                                | Registered for export; no export endpoint exists |
| audit-sensitive-data-view | audit:view-sensitive                        | Reveal the B10 allowlisted actor email fields    |
| audit-dlq-replay          | audit:replay (plus audit:read on the route) | Submit a DLQ replay                              |

The server evaluates a multi-context with `user.key` equal to the verified Auth0 subject and `tenant.key` equal to the verified tenant claim. Configure LaunchDarkly targeting against the user or tenant context kind. No token, email, event payload, or permission list is sent. Client query parameters and headers cannot override these keys. Enabling a flag never grants permissions or access to another tenant.

Disabled SSE/replay flags return 403 before opening a stream or reserving/publishing a replay. A disabled sensitive-data flag keeps fields masked, even for privileged users; ordinary read APIs remain available. Checks occur per request, including SSE connection establishment. Existing SSE sessions retain their flag decisions until reconnect or token expiry. After initial SDK initialization, the SDK may use its last known flag values during an upstream outage.

Tests use a mock SDK client and existing real Auth0/PostgreSQL/Kafka fixtures; they do not require a LaunchDarkly account. B12 remains unimplemented.
