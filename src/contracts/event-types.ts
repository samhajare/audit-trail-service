export const EVENT_TYPES = [
  'USER_LOGIN',
  'USER_ROLE_CHANGED',
  'DATA_EXPORTED',
  'CONFIG_CHANGED',
  'PAYMENT_REFUNDED',
] as const;

export type EventType = (typeof EVENT_TYPES)[number];
