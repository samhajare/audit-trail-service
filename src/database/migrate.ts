import type { Pool } from 'pg';
import { createAuditEventsMigration } from './migrations/001-create-audit-events';

/** Explicit, transactional migrations; application startup never changes tables. */
export async function runMigrations(pool: Pool): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(746219830)');
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id VARCHAR PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
      )
    `);
    const applied = await client.query(
      'SELECT id FROM schema_migrations WHERE id = $1',
      [createAuditEventsMigration.id],
    );
    if (applied.rowCount === 0) {
      await client.query(createAuditEventsMigration.sql);
      await client.query('INSERT INTO schema_migrations (id) VALUES ($1)', [
        createAuditEventsMigration.id,
      ]);
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
