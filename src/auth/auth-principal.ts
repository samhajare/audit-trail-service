export const AUDIT_ROLES = [
  'AUDIT_VIEWER',
  'AUDIT_ANALYST',
  'AUDIT_ADMIN',
] as const;
export const AUDIT_PERMISSIONS = [
  'audit:read',
  'audit:export',
  'audit:view-sensitive',
  'audit:replay',
  'audit:manage',
] as const;
export type AuditPermission = (typeof AUDIT_PERMISSIONS)[number];
export interface AuthPrincipal {
  expiresAt: number;
  subject: string;
  tenantId: string;
  permissions: readonly string[];
}
export interface AuthenticatedRequest {
  headers: { authorization?: string };
  principal?: AuthPrincipal;
}
