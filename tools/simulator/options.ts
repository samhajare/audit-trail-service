import { parseArgs } from 'node:util';
import { z } from 'zod';
import { EVENT_TYPES } from '../../src/contracts/event-types';

export const simulatorOptionsSchema = z
  .strictObject({
    scenario: z.enum(['events', 'correlated', 'duplicate']).default('events'),
    type: z.enum(['all', ...EVENT_TYPES]).default('all'),
    count: z.union([z.literal(1), z.literal(100), z.literal(1000)]).default(1),
    tenant: z.string().trim().min(1).default('demo-tenant'),
    correlation: z.string().trim().min(1).optional(),
  })
  .superRefine((options, context) => {
    if (options.scenario !== 'events' && options.count !== 1) {
      context.addIssue({
        code: 'custom',
        message: '--count applies only to the events scenario',
      });
    }
    if (options.scenario === 'correlated' && options.type !== 'all') {
      context.addIssue({
        code: 'custom',
        message: 'The correlated scenario includes all five event types',
      });
    }
  });

export type SimulatorOptions = z.infer<typeof simulatorOptionsSchema>;

export const SIMULATOR_HELP = `Audit event simulator (Kafka only)
Usage: npm run simulator -- [options]
  --scenario events|correlated|duplicate  Default: events
  --type all|${EVENT_TYPES.join('|')}  Default: all
  --count 1|100|1000                     Events scenario only; default: 1
  --tenant <tenantId>                    Default: demo-tenant
  --correlation <correlationId>          Optional shared correlation ID
  --help                                Show usage without connecting
`;

export function parseSimulatorArgs(args: string[]): SimulatorOptions | null {
  const { values } = parseArgs({
    args,
    allowPositionals: false,
    strict: true,
    options: {
      scenario: { type: 'string' },
      type: { type: 'string' },
      count: { type: 'string' },
      tenant: { type: 'string' },
      correlation: { type: 'string' },
      help: { type: 'boolean' },
    },
  });
  if (values.help) return null;
  if (
    values.count !== undefined &&
    !['1', '100', '1000'].includes(values.count)
  ) {
    throw new Error('--count must be 1, 100, or 1000');
  }
  return simulatorOptionsSchema.parse({
    scenario: values.scenario,
    type: values.type,
    tenant: values.tenant,
    correlation: values.correlation,
    count: values.count === undefined ? undefined : Number(values.count),
  });
}
