import { z } from 'zod';
import { EVENT_TYPES } from '../contracts/event-types';

const text = z
  .string()
  .min(1)
  .max(256)
  .refine((value) => value.trim().length > 0);
export const auditFiltersSchema = z
  .strictObject({
    eventType: z.enum(EVENT_TYPES).optional(),
    actor: text.optional(),
    resourceType: text.optional(),
    resourceId: text.optional(),
    service: text.optional(),
    severity: text.optional(),
    correlationId: text.optional(),
    from: z.iso.datetime({ offset: true }).optional(),
    to: z.iso.datetime({ offset: true }).optional(),
  })
  .refine(
    (filters) =>
      !filters.from ||
      !filters.to ||
      Date.parse(filters.from) <= Date.parse(filters.to),
    { message: 'from must not be later than to', path: ['from'] },
  );

export type AuditFilters = z.infer<typeof auditFiltersSchema>;
