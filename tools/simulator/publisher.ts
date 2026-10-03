import type { Producer } from 'kafkajs';
import type { AuditEvent } from '../../src/contracts/audit-event';
import { auditEventSchema } from '../../src/contracts/audit-event.schema';
import type { SimulatorOptions } from './options';

/** Only Kafka is used here. Input validation completes before any publishing. */
export async function publishEvents(
  producer: Pick<Producer, 'connect' | 'send' | 'disconnect'>,
  topic: string,
  events: readonly AuditEvent[],
  onPublished: (event: AuditEvent) => void = () => {},
  scenario: SimulatorOptions['scenario'] = 'events',
): Promise<number> {
  const validated = events.map((event) => auditEventSchema.parse(event));
  try {
    await producer.connect();
    for (let offset = 0; offset < validated.length; offset += 100) {
      const batch = validated.slice(offset, offset + 100);
      await producer.send({
        topic,
        acks: -1,
        messages: batch.map((event) => ({
          key: event.correlationId,
          // The explicit DLQ demo changes only the version of a validated synthetic event.
          value: JSON.stringify(
            scenario === 'dlq' ? { ...event, schemaVersion: '2.0' } : event,
          ),
        })),
      });
      batch.forEach(onPublished);
    }
    return validated.length;
  } finally {
    await producer.disconnect();
  }
}
