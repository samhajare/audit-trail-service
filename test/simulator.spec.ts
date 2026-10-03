import { EVENT_TYPES } from '../src/contracts/event-types';
import { auditEventSchema } from '../src/contracts/audit-event.schema';
import { generateEvents } from '../tools/simulator/events';
import { parseSimulatorArgs } from '../tools/simulator/options';
import { publishEvents } from '../tools/simulator/publisher';

function scenario(args: string[] = []) {
  const options = parseSimulatorArgs(args);
  if (!options) throw new Error('Expected options');
  return generateEvents(options);
}

describe('Kafka simulator scenarios', () => {
  it.each(EVENT_TYPES)('generates a valid %s event', (type) => {
    const events = scenario(['--type', type]);
    expect(events).toHaveLength(1);
    expect(events[0]?.eventType).toBe(type);
    expect(auditEventSchema.safeParse(events[0]).success).toBe(true);
  });
  it.each(['1', '100', '1000'])(
    'generates %s unique events with schema 1.0',
    (count) => {
      const events = scenario(['--count', count, '--tenant', 'example-tenant']);
      expect(events).toHaveLength(Number(count));
      expect(new Set(events.map((event) => event.eventId)).size).toBe(
        Number(count),
      );
      for (const event of events) {
        expect(event.tenantId).toBe('example-tenant');
        expect(event.schemaVersion).toBe('1.0');
        expect(auditEventSchema.safeParse(event).success).toBe(true);
      }
      if (Number(count) >= 100)
        expect(new Set(events.map((event) => event.eventType)).size).toBe(5);
    },
  );
  it('generates an ordered correlated flow across all five types', () => {
    const events = scenario([
      '--scenario',
      'correlated',
      '--correlation',
      'demo-flow',
    ]);
    expect(events.map((event) => event.eventType)).toEqual([...EVENT_TYPES]);
    expect(new Set(events.map((event) => event.correlationId))).toEqual(
      new Set(['demo-flow']),
    );
    expect(events.map((event) => event.timestamp)).toEqual(
      events.map((event) => event.timestamp).sort(),
    );
  });
  it('generates a shared correlation automatically', () => {
    expect(
      new Set(
        scenario(['--scenario', 'correlated']).map(
          (event) => event.correlationId,
        ),
      ).size,
    ).toBe(1);
  });
  it('duplicates the complete payload and eventId', () => {
    const events = scenario([
      '--scenario',
      'duplicate',
      '--type',
      'PAYMENT_REFUNDED',
    ]);
    expect(events).toHaveLength(2);
    expect(events[0]).toEqual(events[1]);
    expect(events[0]?.eventType).toBe('PAYMENT_REFUNDED');
  });
  it('generates independent payload objects', () => {
    const events = scenario(['--count', '100', '--type', 'CONFIG_CHANGED']);
    events[0]!.context.service = 'changed';
    expect(events[1]?.context.service).toBe('demo-config');
    expect(scenario(['--type', 'CONFIG_CHANGED'])[0]?.context.service).toBe(
      'demo-config',
    );
  });
  it('returns help without generating or connecting', () => {
    expect(parseSimulatorArgs(['--help'])).toBeNull();
  });
  it.each([
    ['--count', '2'],
    ['--count', '100.0'],
    ['--type', 'UNKNOWN'],
    ['--tenant', ' '],
    ['--correlation', ''],
    ['--scenario', 'retry', '--count', '100'],
    ['--scenario', 'dlq', '--count', '100'],
    ['--scenario', 'correlated', '--count', '100'],
    ['--scenario', 'correlated', '--type', 'USER_LOGIN'],
    ['--scenario', 'duplicate', '--count', '1000'],
    ['--unknown'],
    ['extra'],
  ])('rejects unsupported arguments %j', (...args) => {
    expect(() => parseSimulatorArgs(args)).toThrow();
  });
});

describe('Simulator Kafka publishing', () => {
  const producer = {
    connect: jest.fn(),
    send: jest.fn(),
    disconnect: jest.fn(),
  };
  beforeEach(() => {
    for (const method of Object.values(producer))
      method.mockReset().mockResolvedValue(undefined);
  });
  it('publishes a valid retry demo without injecting backend failures', async () => {
    await publishEvents(
      producer,
      'audit.events',
      scenario(['--scenario', 'retry']),
      undefined,
      'retry',
    );
    const event = JSON.parse(producer.send.mock.calls[0]![0].messages[0].value);
    expect(auditEventSchema.safeParse(event).success).toBe(true);
  });
  it('publishes the DLQ demo with an unsupported version through Kafka', async () => {
    await publishEvents(
      producer,
      'audit.events',
      scenario(['--scenario', 'dlq']),
      undefined,
      'dlq',
    );
    const event = JSON.parse(producer.send.mock.calls[0]![0].messages[0].value);
    expect(event.schemaVersion).toBe('2.0');
    expect(auditEventSchema.safeParse(event).success).toBe(false);
  });
  it('publishes 1000 events in acknowledged Kafka batches keyed by correlationId', async () => {
    const events = scenario(['--count', '1000']);
    const published = jest.fn();
    expect(
      await publishEvents(producer, 'audit.events', events, published),
    ).toBe(1000);
    expect(producer.send).toHaveBeenCalledTimes(10);
    expect(published).toHaveBeenCalledTimes(1000);
    const first = producer.send.mock.calls[0]?.[0];
    expect(first).toMatchObject({ topic: 'audit.events', acks: -1 });
    expect(first.messages[0]).toEqual({
      key: events[0]?.correlationId,
      value: JSON.stringify(events[0]),
    });
    expect(producer.disconnect).toHaveBeenCalledTimes(1);
  });
  it('sends exact duplicate payloads', async () => {
    await publishEvents(
      producer,
      'audit.events',
      scenario(['--scenario', 'duplicate']),
    );
    const sent = producer.send.mock.calls[0]?.[0].messages;
    expect(sent[0]).toEqual(sent[1]);
  });
  it('validates the entire input before opening a connection', async () => {
    const events = scenario();
    events[0]!.timestamp = 'invalid';
    await expect(
      publishEvents(producer, 'audit.events', events),
    ).rejects.toThrow();
    expect(producer.connect).not.toHaveBeenCalled();
    expect(producer.send).not.toHaveBeenCalled();
  });
  it('disconnects on publish failure without logging success', async () => {
    producer.send.mockRejectedValue(new Error('send failed'));
    const published = jest.fn();
    await expect(
      publishEvents(producer, 'audit.events', scenario(), published),
    ).rejects.toThrow('send failed');
    expect(producer.disconnect).toHaveBeenCalled();
    expect(published).not.toHaveBeenCalled();
  });
  it('attempts disconnect even after connection failure', async () => {
    producer.connect.mockRejectedValue(new Error('connect failed'));
    await expect(
      publishEvents(producer, 'audit.events', scenario()),
    ).rejects.toThrow('connect failed');
    expect(producer.disconnect).toHaveBeenCalled();
  });
});
