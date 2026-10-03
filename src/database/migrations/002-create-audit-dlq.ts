export const createAuditDlqMigration = {
  id: '002-create-audit-dlq',
  sql: `
    CREATE TABLE audit_dlq (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id VARCHAR, event_id VARCHAR,
      kafka_topic VARCHAR NOT NULL, kafka_partition INTEGER NOT NULL, kafka_offset VARCHAR NOT NULL,
      envelope JSONB NOT NULL,
      replay_status VARCHAR NOT NULL DEFAULT 'pending' CHECK (replay_status IN ('pending','reserved','published','failed')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(kafka_topic,kafka_partition,kafka_offset), UNIQUE(tenant_id,event_id)
    );
    CREATE INDEX audit_dlq_tenant_created_idx ON audit_dlq(tenant_id,created_at DESC,id DESC);
    CREATE TABLE audit_dlq_replays (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      dlq_id UUID NOT NULL UNIQUE REFERENCES audit_dlq(id),
      tenant_id VARCHAR NOT NULL, event_id VARCHAR NOT NULL, actor_id VARCHAR NOT NULL,
      status VARCHAR NOT NULL CHECK(status IN ('reserved','published','failed')),
      requested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
      completed_at TIMESTAMPTZ
    );
  `,
};
