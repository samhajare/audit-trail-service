import type { Pool } from 'pg';
import { createAuditEventsMigration } from './migrations/001-create-audit-events';
import { createAuditDlqMigration } from './migrations/002-create-audit-dlq';

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
    for (const migration of [
      createAuditEventsMigration,
      createAuditDlqMigration,
    ]) {
      const applied = await client.query(
        'SELECT id FROM schema_migrations WHERE id = $1',
        [migration.id],
      );
      if (applied.rowCount === 0) {
        await client.query(migration.sql);
        await client.query('INSERT INTO schema_migrations (id) VALUES ($1)', [
          migration.id,
        ]);
      }
    }
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}
