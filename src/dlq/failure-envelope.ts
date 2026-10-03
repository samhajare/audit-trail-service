import { z } from 'zod';
export const failureEnvelopeSchema = z.strictObject({
  originalEvent: z.json(),
  failureReason: z.enum([
    'malformed_json',
    'invalid_schema',
    'unsupported_schema_version',
    'prohibited_credentials',
    'transient_persistence',
    'permanent_persistence',
    'invalid_retry_envelope',
  ]),
  retryCount: z.number().int().min(0).max(10),
  failedAt: z.iso.datetime(),
  sourceTopic: z.string().min(1).max(249),
  correlationId: z.string().nullable(),
  retryAt: z.iso.datetime().optional(),
  originalPayloadOmitted: z.boolean().optional(),
  originalPayloadSha256: z.string().optional(),
});
export type FailureEnvelope = z.infer<typeof failureEnvelopeSchema>;
