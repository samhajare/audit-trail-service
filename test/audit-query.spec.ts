import {
  AuditQueryValidationPipe,
  auditEventsQuerySchema,
} from '../src/audit/audit-query.dto';

describe('Audit query DTO validation', () => {
  it('provides bounded pagination defaults', () => {
    expect(auditEventsQuerySchema.parse({})).toEqual({ page: 1, limit: 25 });
  });
  it('parses integer query strings and timezone-aware ranges', () => {
    expect(
      auditEventsQuerySchema.parse({
        page: '2',
        limit: '100',
        from: '2026-10-04T10:00:00+05:30',
        to: '2026-10-04T05:00:00Z',
      }),
    ).toMatchObject({ page: 2, limit: 100 });
  });
  it.each([
    { page: '0' },
    { page: '-1' },
    { page: '1.5' },
    { page: '1e2' },
    { page: '9007199254740991', limit: '100' },
    { limit: '101' },
    { limit: '' },
    { page: ['1', '2'] },
    { eventType: 'UNKNOWN' },
    { actor: ' ' },
    { actor: ['a', 'b'] },
    { tenantId: 'tenant-b' },
    { from: '2026-02-30T00:00:00Z' },
    { to: '2026-10-04' },
    { from: '2026-10-04T02:00:00Z', to: '2026-10-04T01:00:00Z' },
  ])('rejects invalid query %j', (query) => {
    expect(auditEventsQuerySchema.safeParse(query).success).toBe(false);
  });
  it('reports fields without reflecting input values', () => {
    const pipe = new AuditQueryValidationPipe(auditEventsQuerySchema);
    expect(() =>
      pipe.transform({ eventType: 'must-not-be-reflected' }),
    ).toThrow('Invalid audit query');
  });
});
