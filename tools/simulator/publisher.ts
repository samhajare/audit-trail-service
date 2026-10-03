import type { Producer } from 'kafkajs';
import type { AuditEvent } from '../../src/contracts/audit-event';
import { auditEventSchema } from '../../src/contracts/audit-event.schema';

/** Only Kafka is used here. Input validation completes before any publishing. */
export async function publishEvents(
  producer: Pick<Producer, 'connect' | 'send' | 'disconnect'>,
  topic: string,
  events: readonly AuditEvent[],
  onPublished: (event: AuditEvent) => void = () => {},
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
          value: JSON.stringify(event),
        })),
      });
      batch.forEach(onPublished);
    }
    return validated.length;
  } finally {
    await producer.disconnect();
  }
}
