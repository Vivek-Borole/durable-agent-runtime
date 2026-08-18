import { randomUUID } from "node:crypto";

type Entry = { credential: string; expiresAt: number };

// Provider credentials intentionally live only in this process. The opaque
// handle is safe to persist with a run; consuming it deletes the secret.
export class CredentialBroker {
  private readonly entries = new Map<string, Entry>();

  constructor(private readonly ttlMs = 5 * 60_000) {}

  issue(credential: string): string {
    this.purge();
    const handle = randomUUID();
    this.entries.set(handle, { credential, expiresAt: Date.now() + this.ttlMs });
    return handle;
  }

  consume(handle: string): string | undefined {
    this.purge();
    const entry = this.entries.get(handle);
    this.entries.delete(handle);
    return entry?.credential;
  }

  discard(handle: string): void {
    this.entries.delete(handle);
  }

  private purge(): void {
    const now = Date.now();
    for (const [handle, entry] of this.entries) if (entry.expiresAt <= now) this.entries.delete(handle);
  }
}
