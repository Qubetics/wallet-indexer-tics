export const REQUIRED_SECRETS: string[] = [
  "FIREBASE_SERVICE_ACCOUNT",
];

export function assertRequiredSecrets(cfg: any, required: string[]): void {
  const missing = required.filter((path) => {
    const value = path.split(".").reduce((obj: any, k: string) => (obj == null ? undefined : obj[k]), cfg);
    return value === undefined || value === null || value === "";
  });
  if (missing.length) {
    throw new Error(`Missing ZooKeeper config keys: ${missing.join(", ")}`);
  }
}
