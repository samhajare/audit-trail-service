import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
  ServiceUnavailableException,
} from '@nestjs/common';
import { assertNoCredentials } from '../database/assert-no-credentials';
import { AuditRepository } from './audit.repository';
import {
  AuditEventsQueryDto,
  AuditStatisticsQueryDto,
} from './audit-query.dto';

@Injectable()
export class AuditService {
  private readonly logger = new Logger(AuditService.name);
  constructor(private readonly repository: AuditRepository) {}

  private async read<T>(
    tenantId: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    try {
      return await operation();
    } catch {
      this.logger.error({ message: 'Audit read failed', tenantId });
      throw new ServiceUnavailableException('Audit data is unavailable');
    }
  }

  async list(
    tenantId: string,
    query: AuditEventsQueryDto,
    correlationId?: string,
  ) {
    if (
      correlationId !== undefined &&
      query.correlationId !== undefined &&
      query.correlationId !== correlationId
    ) {
      throw new BadRequestException(
        'Query correlationId conflicts with the timeline path',
      );
    }

    const { page, limit, ...filters } = query;
    const result = await this.read(tenantId, async () => {
      const data = await this.repository.findPage(
        tenantId,
        {
          ...filters,
          ...(correlationId === undefined ? {} : { correlationId }),
          limit,
          offset: (page - 1) * limit,
        },
        correlationId === undefined ? 'created' : 'timeline',
      );
      data.items.forEach(assertNoCredentials);
      return data;
    });
    return {
      ...result,
      page,
      limit,
      totalPages: Math.ceil(result.total / limit),
    };
  }

  async detail(tenantId: string, id: string) {
    const event = await this.read(tenantId, async () => {
      const data = await this.repository.findById(tenantId, id);
      if (data) assertNoCredentials(data);
      return data;
    });
    if (!event) throw new NotFoundException('Audit event not found');
    return event;
  }

  statistics(tenantId: string, filters: AuditStatisticsQueryDto) {
    return this.read(tenantId, () =>
      this.repository.getStatistics(tenantId, filters),
    );
  }
}
