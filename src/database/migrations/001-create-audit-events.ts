export const createAuditEventsMigration = {
  id: '001-create-audit-events',
  sql: `
    CREATE TABLE audit_events (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      event_id VARCHAR NOT NULL UNIQUE,
      schema_version VARCHAR NOT NULL CHECK (schema_version = '1.0'),
      event_type VARCHAR NOT NULL CHECK (event_type IN (
        'USER_LOGIN', 'USER_ROLE_CHANGED', 'DATA_EXPORTED',
        'CONFIG_CHANGED', 'PAYMENT_REFUNDED'
      )),
      event_timestamp TIMESTAMPTZ NOT NULL,
      tenant_id VARCHAR NOT NULL,
      correlation_id VARCHAR NOT NULL,
      actor_id VARCHAR NOT NULL,
      actor_email VARCHAR,
      actor_role VARCHAR,
      resource_type VARCHAR NOT NULL,
      resource_id VARCHAR NOT NULL,
      action VARCHAR NOT NULL,
      severity VARCHAR,
      before_data JSONB CHECK (jsonb_typeof(before_data) = 'object'),
      after_data JSONB CHECK (jsonb_typeof(after_data) = 'object'),
      context JSONB NOT NULL CHECK (jsonb_typeof(context) = 'object'),
      metadata JSONB NOT NULL CHECK (jsonb_typeof(metadata) = 'object'),
      created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX audit_events_tenant_id_idx ON audit_events (tenant_id);
    CREATE INDEX audit_events_event_type_idx ON audit_events (event_type);
    CREATE INDEX audit_events_created_at_idx ON audit_events (created_at);
    CREATE INDEX audit_events_correlation_id_idx ON audit_events (correlation_id);
    CREATE INDEX audit_events_actor_id_idx ON audit_events (actor_id);
    CREATE INDEX audit_events_resource_id_idx ON audit_events (resource_id);
    CREATE INDEX audit_events_tenant_timeline_idx
      ON audit_events (tenant_id, correlation_id, event_timestamp, id);
    CREATE INDEX audit_events_tenant_created_idx
      ON audit_events (tenant_id, created_at DESC, id DESC);
  `,
} as const;
