import { Logger } from '@nestjs/common';
import { AuditRepository } from '../src/audit/audit.repository';
import { EVENT_TYPES } from '../src/contracts/event-types';
import { ProhibitedCredentialFieldsError } from '../src/database/assert-no-credentials';
import { AuditIngestionService } from '../src/kafka/audit-ingestion.service';
import { auditEventFixture } from './fixtures/audit-event.fixture';

describe('Audit Kafka message processing', () => {
  const location = { topic: 'audit.events', partition: 0, offset: '7' };
  const create = jest.fn();
  const repository: AuditRepository = {
    create,
    findByEventId: jest.fn(),
    findMany: jest.fn(),
    findByCorrelationId: jest.fn(),
    getStatistics: jest.fn(),
  };
  let service: AuditIngestionService;

  beforeEach(() => {
    create.mockReset().mockResolvedValue({ status: 'created' });
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'error').mockImplementation(() => {});
    service = new AuditIngestionService(repository);
  });
  afterEach(() => jest.restoreAllMocks());

  it.each(EVENT_TYPES)(
    'persists supported event type %s through B2',
    async (eventType) => {
      const event = auditEventFixture({ eventType });
      expect(
        await service.handle(Buffer.from(JSON.stringify(event)), location),
      ).toBe('persisted');
      expect(create).toHaveBeenCalledWith(event);
      expect(Logger.prototype.log).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'persisted',
          eventId: event.eventId,
          tenantId: event.tenantId,
          correlationId: event.correlationId,
          eventType,
          service: 'identity',
          ...location,
        }),
      );
    },
  );
  it('handles duplicate delivery safely', async () => {
    create.mockResolvedValue({ status: 'duplicate' });
    expect(
      await service.handle(
        Buffer.from(JSON.stringify(auditEventFixture())),
        location,
      ),
    ).toBe('duplicate');
  });
  it.each([null, Buffer.from('{invalid'), Buffer.from([0xff])])(
    'rejects tombstones, malformed JSON, or invalid UTF-8 without persistence',
    async (value) => {
      expect(await service.handle(value, location)).toBe('rejected');
      expect(create).not.toHaveBeenCalled();
    },
  );
  it.each([
    null,
    [],
    { ...auditEventFixture(), eventId: undefined },
    { ...auditEventFixture(), tenantId: undefined },
    { ...auditEventFixture(), schemaVersion: '2.0' },
    { ...auditEventFixture(), eventType: 'UNKNOWN' },
    { ...auditEventFixture(), timestamp: 'invalid' },
  ])(
    'rejects invalid schema %j and continues with a valid message',
    async (payload) => {
      expect(
        await service.handle(Buffer.from(JSON.stringify(payload)), location),
      ).toBe('rejected');
      expect(create).not.toHaveBeenCalled();
      expect(
        await service.handle(
          Buffer.from(JSON.stringify(auditEventFixture())),
          location,
        ),
      ).toBe('persisted');
    },
  );
  it('rejects prohibited credential fields without logging payload values', async () => {
    create.mockRejectedValue(new ProhibitedCredentialFieldsError());
    const payload = auditEventFixture({
      metadata: { password: 'must-never-be-logged' },
    });
    expect(
      await service.handle(Buffer.from(JSON.stringify(payload)), location),
    ).toBe('rejected');
    expect(Logger.prototype.warn).toHaveBeenCalledWith(
      expect.objectContaining({ reason: 'prohibited_credentials' }),
    );
    const logs = jest.mocked(Logger.prototype.warn).mock.calls;
    expect(JSON.stringify(logs)).not.toContain('must-never-be-logged');
  });
  it('propagates persistence failures without logging driver details', async () => {
    create.mockRejectedValue(
      new Error('driver error containing must-never-be-logged'),
    );
    await expect(
      service.handle(
        Buffer.from(JSON.stringify(auditEventFixture())),
        location,
      ),
    ).rejects.toThrow('Audit persistence failed');
    expect(
      JSON.stringify(jest.mocked(Logger.prototype.error).mock.calls),
    ).not.toContain('must-never-be-logged');
  });
});
