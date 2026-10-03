export function validateEnvironment(env: Record<string, unknown>) {
  const rawPort = env.APP_PORT ?? '3000';
  const port = Number(rawPort);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('APP_PORT must be an integer between 1 and 65535');
  }

  const appEnv = env.APP_ENV ?? 'development';
  if (!['development', 'test', 'production'].includes(String(appEnv))) {
    throw new Error('APP_ENV must be development, test, or production');
  }

  return { ...env, APP_PORT: port, APP_ENV: appEnv };
}
