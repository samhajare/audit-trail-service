import {
  ConflictException,
  Injectable,
  Logger,
  NotFoundException,
  ServiceUnavailableException,
} from '@nestjs/common';
import { auditEventSchema } from '../contracts/audit-event.schema';
import { assertNoCredentials } from '../database/assert-no-credentials';
import { logIdentifiers } from '../kafka/audit-ingestion.service';
import { DlqRepository } from './dlq.repository';
import { DlqReplayPublisher } from './dlq-replay.publisher';

@Injectable()
export class DlqService {
  private readonly logger = new Logger(DlqService.name);
  constructor(
    private readonly repository: DlqRepository,
    private readonly publisher: DlqReplayPublisher,
  ) {}
  private async read<T>(operation: () => Promise<T>): Promise<T> {
    try {
      return await operation();
    } catch {
      throw new ServiceUnavailableException('DLQ data is unavailable');
    }
  }
  async list(tenantId: string, page: number, limit: number) {
    const result = await this.read(() =>
      this.repository.list(tenantId, limit, (page - 1) * limit),
    );
    return {
      ...result,
      page,
      limit,
      totalPages: Math.ceil(result.total / limit),
    };
  }
  async detail(tenantId: string, eventId: string) {
    const record = await this.read(() =>
      this.repository.detail(tenantId, eventId),
    );
    if (!record) throw new NotFoundException('DLQ event not found');
    return record;
  }
  async replay(tenantId: string, eventId: string, subject: string) {
    const record = await this.detail(tenantId, eventId);
    const parsed = auditEventSchema.safeParse(record.envelope.originalEvent);
    if (
      !parsed.success ||
      parsed.data.tenantId !== tenantId ||
      parsed.data.eventId !== eventId ||
      record.envelope.sourceTopic !== this.publisher.sourceTopic
    )
      throw new ConflictException('DLQ event cannot be replayed');
    try {
      assertNoCredentials(parsed.data);
    } catch {
      throw new ConflictException('DLQ event cannot be replayed');
    }
    const replayId = await this.read(() =>
      this.repository.reserve(record, subject),
    );
    if (!replayId)
      throw new ConflictException('DLQ event replay already requested');
    const identifiers = {
      ...logIdentifiers(parsed.data),
      replayId,
      subject: subject.replace(/[\r\n\t]/g, ' ').slice(0, 256),
    };
    this.logger.log({ message: 'DLQ replay reserved', ...identifiers });
    try {
      await this.publisher.publish(parsed.data, replayId);
    } catch {
      await this.repository
        .complete(record.id, replayId, 'failed')
        .catch(() => {});
      this.logger.error({
        message: 'DLQ replay publication failed',
        ...identifiers,
      });
      throw new ServiceUnavailableException('DLQ replay publication failed');
    }
    await this.read(() =>
      this.repository.complete(record.id, replayId, 'published'),
    );
    this.logger.log({ message: 'DLQ replay published', ...identifiers });
    return { eventId, replayId, status: 'published' as const };
  }
}
