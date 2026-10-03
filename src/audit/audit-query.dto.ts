import { BadRequestException, Injectable, PipeTransform } from '@nestjs/common';
import { z } from 'zod';
import { auditFiltersSchema } from './audit-filters';

function integerQuery(defaultValue: number, max: number) {
  return z
    .string()
    .regex(/^[1-9]\d*$/)
    .transform(Number)
    .pipe(z.number().int().min(1).max(max))
    .default(defaultValue);
}

export const auditEventsQuerySchema = auditFiltersSchema
  .safeExtend({
    page: integerQuery(1, Number.MAX_SAFE_INTEGER),
    limit: integerQuery(25, 100),
  })
  .refine((query) => Number.isSafeInteger((query.page - 1) * query.limit), {
    message: 'Pagination offset exceeds the supported range',
    path: ['page'],
  });

export type AuditEventsQueryDto = z.infer<typeof auditEventsQuerySchema>;
export type AuditStatisticsQueryDto = z.infer<typeof auditFiltersSchema>;

@Injectable()
export class AuditQueryValidationPipe implements PipeTransform {
  constructor(private readonly schema: z.ZodType) {}

  transform(value: unknown) {
    const result = this.schema.safeParse(value);
    if (!result.success) {
      throw new BadRequestException({
        message: 'Invalid audit query',
        fields: [
          ...new Set(
            result.error.issues.map((issue) => issue.path.join('.') || 'query'),
          ),
        ],
      });
    }
    return result.data;
  }
}
