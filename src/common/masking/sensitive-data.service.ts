import { Injectable } from '@nestjs/common';
import { isCredentialField } from '../../database/assert-no-credentials';

/** Server-owned policy. Permission reveals only the canonical event actor email. */
export const SENSITIVE_DATA_POLICY = {
  maskedFields: [
    'email',
    'emailaddress',
    'phone',
    'phonenumber',
    'mobilephone',
    'cardnumber',
    'pan',
  ],
  revealPaths: [
    ['actor', 'email'],
    ['items', '*', 'actor', 'email'],
    ['envelope', 'originalEvent', 'actor', 'email'],
    ['items', '*', 'envelope', 'originalEvent', 'actor', 'email'],
    ['data', 'actor', 'email'],
  ],
} as const;

@Injectable()
export class SensitiveDataService {
  mask(value: unknown, canViewSensitive = false): unknown {
    const visit = (input: unknown, path: string[]): unknown => {
      if (input === null || typeof input !== 'object') return input;
      if (Array.isArray(input))
        return input.map((item) => visit(item, [...path, '*']));
      return Object.fromEntries(
        Object.entries(input).map(([key, nested]) => {
          const nextPath = [...path, key];
          if (isCredentialField(key)) return [key, '[REDACTED]'];
          const normalized = key.toLowerCase().replace(/[^a-z]/g, '');
          const sensitive = SENSITIVE_DATA_POLICY.maskedFields.some(
            (field) => field === normalized,
          );
          const allowed =
            canViewSensitive &&
            typeof nested === 'string' &&
            SENSITIVE_DATA_POLICY.revealPaths.some(
              (allowedPath) =>
                allowedPath.length === nextPath.length &&
                allowedPath.every((part, index) => part === nextPath[index]),
            );
          return [
            key,
            sensitive && !allowed ? '[MASKED]' : visit(nested, nextPath),
          ];
        }),
      );
    };
    return visit(value, []);
  }
}
