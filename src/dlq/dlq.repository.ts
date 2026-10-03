import { FailureEnvelope } from './failure-envelope';
export interface DlqRecord {
  id: string;
  eventId: string | null;
  tenantId: string | null;
  envelope: FailureEnvelope;
  replayStatus: 'pending' | 'reserved' | 'published' | 'failed';
  createdAt: string;
}
export abstract class DlqRepository {
  abstract store(
    envelope: FailureEnvelope,
    location: { topic: string; partition: number; offset: string },
  ): Promise<void>;
  abstract list(
    tenantId: string,
    limit: number,
    offset: number,
  ): Promise<{ items: DlqRecord[]; total: number }>;
  abstract detail(tenantId: string, eventId: string): Promise<DlqRecord | null>;
  abstract reserve(record: DlqRecord, actorId: string): Promise<string | null>;
  abstract complete(
    recordId: string,
    replayId: string,
    status: 'published' | 'failed',
  ): Promise<void>;
}
