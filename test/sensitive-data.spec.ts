import { SensitiveDataService } from '../src/common/masking/sensitive-data.service';

describe('Sensitive data policy', () => {
  const service = new SensitiveDataService();
  it('masks nested values and arrays without mutating storage or identifiers', () => {
    const original = {
      eventId: 'event',
      actor: { email: 'user@example.com' },
      changes: {
        after: {
          phone: '123456',
          cards: [{ 'Card-Number': '4111111111111111' }],
        },
      },
    };
    const before = structuredClone(original);
    expect(service.mask(original)).toEqual({
      eventId: 'event',
      actor: { email: '[MASKED]' },
      changes: {
        after: { phone: '[MASKED]', cards: [{ 'Card-Number': '[MASKED]' }] },
      },
    });
    expect(original).toEqual(before);
  });
  it('reveals only the explicitly allowed actor email for privileged readers', () => {
    expect(
      service.mask(
        {
          actor: { email: 'actor@example.com' },
          metadata: {
            email: 'hidden@example.com',
            actor: { email: 'also-hidden@example.com' },
            phone: '123',
            cardNumber: '4111111111111111',
          },
        },
        true,
      ),
    ).toEqual({
      actor: { email: 'actor@example.com' },
      metadata: {
        email: '[MASKED]',
        actor: { email: '[MASKED]' },
        phone: '[MASKED]',
        cardNumber: '[MASKED]',
      },
    });
  });
  it.each([
    'password',
    'access_token',
    'refreshToken',
    'SECRET',
    'api-key',
    'Authorization',
    'private_key',
    'token',
  ])('never reveals credential field %s even with permission', (key) => {
    const value = service.mask(
      { items: [{ [key]: { nested: 'must-never-be-returned' } }] },
      true,
    );
    expect(JSON.stringify(value)).not.toContain('must-never-be-returned');
    expect(JSON.stringify(value)).toContain('[REDACTED]');
  });
  it('does not reveal non-string values in an allowlisted email field', () => {
    expect(
      service.mask({ actor: { email: ['hidden@example.com'] } }, true),
    ).toEqual({ actor: { email: '[MASKED]' } });
  });
  it.each([
    { items: [{ actor: { email: 'allowed@example.com' } }] },
    {
      envelope: { originalEvent: { actor: { email: 'allowed@example.com' } } },
    },
    {
      items: [
        {
          envelope: {
            originalEvent: { actor: { email: 'allowed@example.com' } },
          },
        },
      ],
    },
    { data: { actor: { email: 'allowed@example.com' } } },
  ])('applies allowlisted disclosure to REST/DLQ/SSE shapes', (value) => {
    expect(JSON.stringify(service.mask(value))).not.toContain(
      'allowed@example.com',
    );
    expect(service.mask(value, true)).toEqual(value);
  });
});
