# IMPLEMENTATION_PLAN.md

# Audit Trail Service — Backend Phased Plan

Build one phase per Codex session.

## B0 — Foundation

Create NestJS project and structure:

```text
src/
  auth/
  audit/
  common/
  contracts/
  database/
  dlq/
  feature-flags/
  kafka/
  realtime/
tools/simulator/
docs/
test/
```

Configure:
- TypeScript strict mode
- ESLint
- Prettier
- Jest
- environment config
- Docker Compose
- PostgreSQL
- Kafka
- optional Kafka UI

Add `GET /health`.

Acceptance:
- app starts
- health works
- Kafka/Postgres start
- lint/typecheck/test/build pass

Codex prompt:

```text
Read AGENTS.md and IMPLEMENTATION_PLAN.md.
Inspect the repository.
Implement Phase B0 only.

Set up:
- Node.js
- TypeScript strict mode
- NestJS
- PostgreSQL
- Kafka
- Docker Compose
- ESLint
- Prettier
- Jest
- environment configuration
- GET /health

Create the folder structure defined in AGENTS.md.

Do not implement:
- business logic
- Auth0
- LaunchDarkly
- database tables
- Kafka consumers

Run format, lint, typecheck, tests, and build.
Verify Docker Compose.

At the end report:
1. files changed
2. architecture decisions
3. commands run
4. known limitations
5. acceptance criteria status
6. confirm only B0 was implemented
```

## B1 — Audit Event Contract

Create under `src/contracts/`:
- audit-event.ts
- audit-event.schema.ts
- event-types.ts
- schema-version.ts

Supported events:
- USER_LOGIN
- USER_ROLE_CHANGED
- DATA_EXPORTED
- CONFIG_CHANGED
- PAYMENT_REFUNDED

Runtime validation required.
schemaVersion = `1.0`.

Acceptance:
- valid event passes
- missing eventId fails
- invalid timestamp fails
- missing tenantId fails
- unsupported type/version fails
- tests pass

Codex prompt:

```text
Read AGENTS.md and IMPLEMENTATION_PLAN.md.
Implement Phase B1 only.

Create the AuditEvent TypeScript contract and runtime validation.
Add the five supported event types.
Use schemaVersion 1.0.
Add unit tests for valid and invalid payloads.

Do not implement Kafka yet.

Run lint, typecheck, tests, and build.
```

## B2 — PostgreSQL Persistence

Create `audit_events` migration.

Requirements:
- UNIQUE(event_id)
- JSONB before_data
- JSONB after_data
- JSONB context
- JSONB metadata
- required indexes

Repository/service:
- create
- findByEventId
- findMany
- findByCorrelationId
- getStatistics

Acceptance:
- migration runs
- record persists
- duplicates prevented
- tenant-aware queries exist
- tests pass

Codex prompt:

```text
Implement Phase B2 only.

Create PostgreSQL persistence for audit events:
- migration
- audit_events table
- UNIQUE(event_id)
- JSONB fields
- indexes
- repository/service abstraction
- persistence tests

Do not implement Kafka yet.

Run migrations, lint, typecheck, tests, and build.
```

## B3 — Kafka Ingestion

Flow:

```text
audit.events -> consumer -> validation -> idempotency -> PostgreSQL
```

Requirements:
- Kafka config
- consumer
- JSON parsing
- runtime validation
- schema 1.0 support
- structured logs
- duplicate-safe persistence

Acceptance:
- valid event stored
- duplicate ignored safely
- invalid event does not crash process
- correlationId logged
- integration test passes

Codex prompt:

```text
Implement Phase B3 only.

Create Kafka ingestion for topic audit.events.

Requirements:
- consume messages
- parse JSON
- validate using B1 contract
- support schemaVersion 1.0
- persist valid events
- handle duplicates idempotently
- structured logging with eventId, tenantId, correlationId
- integration tests

Do not implement retry/DLQ yet.
```

## B4 — Simulator

Create `tools/simulator/`.

Support:
- USER_LOGIN
- USER_ROLE_CHANGED
- DATA_EXPORTED
- CONFIG_CHANGED
- PAYMENT_REFUNDED
- 1 event
- 100 events
- 1000 events
- correlated flow
- duplicate event

Must publish only through Kafka.

Codex prompt:

```text
Implement Phase B4 only.

Create tools/simulator.

The simulator must publish through Kafka and reuse the backend event contract.

Support:
- each audit event type
- one event
- 100 events
- 1000 events
- correlated flow
- duplicate event

Do not write directly to PostgreSQL.

Add README usage examples.
```

## B5 — REST API

Endpoints:
- GET /audit/events
- GET /audit/events/:id
- GET /audit/timeline/:correlationId
- GET /audit/statistics

Filters:
- eventType
- actor
- resourceType
- resourceId
- service
- severity
- correlationId
- from
- to
- page
- limit

Acceptance:
- pagination works
- filters combine
- event details work
- timeline ordered
- statistics work
- DTO validation exists
- API tests pass

Codex prompt:

```text
Implement Phase B5 only.

Create:
- GET /audit/events
- GET /audit/events/:id
- GET /audit/timeline/:correlationId
- GET /audit/statistics

Add pagination, filtering, and DTO validation.
Add integration tests.

Do not implement Auth0 yet.
```

## B6 — Auth0 + Tenant Isolation

Add JWT validation and backend permission guards.

Roles:
- AUDIT_VIEWER
- AUDIT_ANALYST
- AUDIT_ADMIN

Permissions:
- audit:read
- audit:export
- audit:view-sensitive
- audit:replay
- audit:manage

Acceptance:
- unauthenticated => 401
- unauthorized => 403
- viewer can read
- tenant A cannot read tenant B
- tests pass

Codex prompt:

```text
Implement Phase B6 only.

Integrate Auth0:
- validate JWT
- add permission guards
- extract tenant context
- tenant-scope all audit queries
- add authorization tests
- add tenant isolation tests

Do not implement LaunchDarkly yet.
```

## B7 — SSE

Endpoint:
- GET /audit/stream

Flow:
```text
Kafka -> persist -> publish internally -> SSE
```

Acceptance:
- only persisted events stream
- auth/tenant isolation applies
- reconnection-friendly
- eventId can be used for client deduplication

Codex prompt:

```text
Implement Phase B7 only.

Add Server-Sent Events at GET /audit/stream.

Only emit after successful persistence.
Respect Auth0 and tenant isolation.

Include:
- eventId
- eventType
- timestamp
- tenantId
- correlationId
- actor
- resource
- action

Add tests where practical.
```

## B8 — Retry + DLQ

Topics:
- audit.events
- audit.events.retry
- audit.events.dlq

Retry transient failures only.

Acceptance:
- transient failure retries
- retry bounded
- invalid messages do not retry forever
- max retry -> DLQ
- DLQ keeps failure metadata

Codex prompt:

```text
Implement Phase B8 only.

Add Kafka retry and DLQ.

Classify:
Retryable:
- DB connectivity failure
- transient timeout

Non-retryable:
- malformed payload
- invalid event
- unsupported schema version

Preserve:
- original event
- failure reason
- retry count
- failedAt
- sourceTopic
- correlationId

Add tests.
```

## B9 — DLQ Inspection + Replay

Endpoints:
- GET /audit/dlq
- GET /audit/dlq/:eventId
- POST /audit/dlq/:eventId/replay

Replay requires `audit:replay`.
Replay goes through Kafka.

Acceptance:
- authorization works
- replay goes back through Kafka
- loops prevented
- replay logged/audited

Codex prompt:

```text
Implement Phase B9 only.

Add DLQ list, detail, and replay APIs.

Replay requires audit:replay.
Replay must publish back through Kafka.
Prevent replay loops.
Add authorization and replay tests.
```

## B10 — Sensitive Data Masking

Mask examples:
- email
- phone
- cardNumber
- authorization
- token
- password
- secret

Acceptance:
- default response masked
- allowed privileged fields can be revealed
- passwords/tokens/secrets never returned

Codex prompt:

```text
Implement Phase B10 only.

Add a reusable sensitive-data masking service.
Default API responses should mask configured values.
audit:view-sensitive may reveal only explicitly allowed fields.
Never expose passwords, tokens, secrets, or credentials.

Add unit and integration tests.
```

## B11 — LaunchDarkly

Flags:
- audit-live-stream
- audit-data-export
- audit-sensitive-data-view
- audit-dlq-replay

Wrap LaunchDarkly in a feature flag service.

Acceptance:
- tenant/user targeting
- authorization still required
- disabled flag blocks capability

Codex prompt:

```text
Implement Phase B11 only.

Integrate LaunchDarkly server-side SDK behind a feature flag abstraction.

Flags:
- audit-live-stream
- audit-data-export
- audit-sensitive-data-view
- audit-dlq-replay

Feature flags must not replace authorization.
Support tenant/user targeting.
Document environment variables.
```

## B12 — Client-Ready Polish

Add/review:
- structured logging
- error handling
- README
- architecture docs
- event contract docs
- API docs
- simulator docs
- Docker setup
- .env.example
- GitHub Actions CI

CI:
- install
- lint
- typecheck
- test
- build

Acceptance:
- fresh clone runs locally
- simulator demo works
- tests/build/CI pass
- no secrets committed

Codex prompt:

```text
Implement Phase B12 only.

Prepare audit-trail-service for client sharing.

Add:
- complete README
- architecture docs
- event contract docs
- API docs
- simulator docs
- .env.example
- GitHub Actions CI

CI must run:
- install
- lint
- typecheck
- test
- build

Verify no secrets are committed.
Run the full validation suite before finishing.
```

# Build Order

```text
B0 Foundation
B1 Contract
B2 PostgreSQL
B3 Kafka
B4 Simulator
B5 REST API
B6 Auth0
B7 SSE
B8 Retry/DLQ
B9 Replay
B10 Masking
B11 LaunchDarkly
B12 Polish
```

Start the separate React repo after B5, because by then the backend exposes a usable REST API.

# Reusable Codex Session Prompt

```text
Read AGENTS.md and IMPLEMENTATION_PLAN.md.
Inspect the current repository.

Implement Phase BX only.
Do not implement future phases.
Preserve existing behavior.
Follow AGENTS.md.

Run:
- format
- lint
- typecheck
- tests
- build

Fix failures before finishing.

At the end report:
1. files changed
2. architecture decisions
3. tests/commands executed
4. known limitations
5. acceptance criteria status
6. next phase, without implementing it
```
