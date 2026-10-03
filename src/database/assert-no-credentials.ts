const prohibitedKeys = new Set([
  'password',
  'passwords',
  'passwd',
  'accesstoken',
  'accesstokens',
  'refreshtoken',
  'refreshtokens',
  'token',
  'tokens',
  'secret',
  'secrets',
  'apikey',
  'apikeys',
  'authorization',
  'authorizationheader',
  'authorizationheaders',
  'privatekey',
  'privatekeys',
]);

export class ProhibitedCredentialFieldsError extends Error {
  constructor() {
    super('Audit payload contains prohibited credential fields');
    this.name = 'ProhibitedCredentialFieldsError';
  }
}

export function isCredentialField(key: string): boolean {
  return prohibitedKeys.has(key.toLowerCase().replace(/[^a-z]/g, ''));
}

/** Storage boundary protection, not response masking or privileged disclosure. */
export function assertNoCredentials(value: unknown): void {
  if (value === null || typeof value !== 'object') return;
  if (Array.isArray(value)) {
    value.forEach(assertNoCredentials);
    return;
  }
  for (const [key, nested] of Object.entries(value)) {
    if (isCredentialField(key)) {
      throw new ProhibitedCredentialFieldsError();
    }
    assertNoCredentials(nested);
  }
}
