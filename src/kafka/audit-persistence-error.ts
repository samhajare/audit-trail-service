import { KafkaJSNonRetriableError } from 'kafkajs';

/** Safe classification: never retain or forward a driver's message or cause. */
export class AuditPersistenceError extends KafkaJSNonRetriableError {
  constructor(readonly retryable: boolean) {
    super('Audit persistence failed');
  }
}
export function isTransientPersistenceError(error: unknown): boolean {
  if (!error || typeof error !== 'object') return false;
  const { code, message } = error as { code?: unknown; message?: unknown };
  if (typeof code === 'string') {
    return (
      code.startsWith('08') ||
      [
        '40001',
        '40P01',
        '53300',
        '53400',
        '57P01',
        '57P02',
        '57P03',
        '57014',
        'ECONNREFUSED',
        'ECONNRESET',
        'ETIMEDOUT',
        'EPIPE',
        'ENETUNREACH',
        'EHOSTUNREACH',
        'EAI_AGAIN',
      ].includes(code)
    );
  }
  return [
    'Connection terminated unexpectedly',
    'Connection terminated due to connection timeout',
    'timeout exceeded when trying to connect',
    'Query read timeout',
  ].includes(String(message));
}
