# AGENTS.md

# Project: Audit Trail Service

This repository is the backend service for the Real-Time Audit Trail Explorer.

## Stack
- Node.js
- TypeScript
- NestJS
- Apache Kafka
- PostgreSQL
- Auth0
- LaunchDarkly
- Server-Sent Events
- Docker Compose
- Jest

## Core Responsibilities
- consume audit events from Kafka
- validate and version event payloads
- persist audit events in PostgreSQL
- guarantee idempotent processing by eventId
- expose searchable REST APIs
- enforce Auth0 authentication and permission-based authorization
- enforce tenant isolation
- stream newly persisted events over SSE
- support bounded retry and DLQ flows
- allow authorized DLQ replay
- support LaunchDarkly feature flags
- provide a simulator under tools/simulator for client demos

The frontend lives in a separate repository and must consume this service only through REST and SSE.

## Repository Structure

```text
audit-trail-service/
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
tools/
  simulator/
docs/
test/
AGENTS.md
IMPLEMENTATION_PLAN.md
docker-compose.yml
README.md
.env.example
```

## Audit Event Contract

The backend owns the Kafka contract under `src/contracts/`.

Required fields:
- eventId
- schemaVersion
- eventType
- timestamp
- tenantId
- correlationId
- actor
- resource
- action
- changes
- context
- metadata

Initial schemaVersion: `1.0`

Supported event types:
- USER_LOGIN
- USER_ROLE_CHANGED
- DATA_EXPORTED
- CONFIG_CHANGED
- PAYMENT_REFUNDED

Use runtime validation in addition to TypeScript types.

## Kafka Topics
- audit.events
- audit.events.retry
- audit.events.dlq

## Idempotency
- `eventId` is unique.
- PostgreSQL must enforce `UNIQUE(event_id)`.
- Duplicate delivery must never create duplicate audit rows.

## Retry and DLQ
Retry only transient failures.

Retryable:
- DB connectivity failure
- temporary timeout
- transient infrastructure failure

Non-retryable:
- malformed payload
- invalid schema
- unsupported schema version

DLQ metadata must preserve:
- original event
- failure reason
- retry count
- failedAt
- sourceTopic
- correlationId

## PostgreSQL

Primary table: `audit_events`

Suggested fields:
- id UUID
- event_id VARCHAR UNIQUE
- schema_version VARCHAR
- event_type VARCHAR
- event_timestamp TIMESTAMP
- tenant_id VARCHAR
- correlation_id VARCHAR
- actor_id VARCHAR
- actor_email VARCHAR
- actor_role VARCHAR
- resource_type VARCHAR
- resource_id VARCHAR
- action VARCHAR
- severity VARCHAR
- before_data JSONB
- after_data JSONB
- context JSONB
- metadata JSONB
- created_at TIMESTAMP

Recommended indexes:
- tenant_id
- event_type
- created_at
- correlation_id
- actor_id
- resource_id

## REST API

```text
GET /health
GET /audit/events
GET /audit/events/:id
GET /audit/timeline/:correlationId
GET /audit/statistics
GET /audit/dlq
GET /audit/dlq/:eventId
POST /audit/dlq/:eventId/replay
GET /audit/stream
```

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

Use validated DTOs.

## Auth0

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

Backend authorization is authoritative.

## Tenant Isolation
Every audit event contains `tenantId`.
Every read query is scoped by authenticated tenant.
Do not trust frontend filtering.

## Real-Time Streaming
Use SSE.

Flow:
```text
Kafka -> validate -> persist -> internal publish -> SSE -> frontend
```

Only stream events after successful persistence.

## LaunchDarkly

Suggested flags:
- audit-live-stream
- audit-data-export
- audit-sensitive-data-view
- audit-dlq-replay

Feature flags never replace authorization.

## Simulator

Keep under `tools/simulator/`.

It must publish through Kafka and support:
- individual event types
- 100 events
- 1000 events
- correlated flow
- duplicate event
- retry scenario
- DLQ scenario

Never write directly to PostgreSQL.

## Sensitive Data
Never store or expose:
- passwords
- access tokens
- refresh tokens
- secrets
- API keys
- authorization headers
- private keys

## Logging
Use structured logs and include when available:
- eventId
- tenantId
- correlationId
- eventType
- service

## Testing
Use Jest.

Cover:
- event validation
- idempotency
- persistence
- REST filters/pagination
- Auth0 guards
- tenant isolation
- Kafka ingestion
- retry/DLQ
- replay
- masking

## Environment Variables
Use `.env.example`.

Categories:
- APP_*
- DATABASE_*
- KAFKA_*
- AUTH0_*
- LAUNCHDARKLY_*

Never commit real credentials.

## Scope Control
Do not add without explicit need:
- Redis
- Elasticsearch
- GraphQL
- Kubernetes
- service mesh
- CQRS framework
- event sourcing framework
- extra microservices

## Codex Rules
For every phase:
1. Read AGENTS.md.
2. Read IMPLEMENTATION_PLAN.md.
3. Inspect current code.
4. Implement only the requested phase.
5. Preserve working behavior.
6. Add/update tests.
7. Run format, lint, typecheck, tests, and build.
8. Fix failures.
9. Report files changed, architecture decisions, tests run, limitations, and acceptance criteria.
10. Do not implement future phases early.
