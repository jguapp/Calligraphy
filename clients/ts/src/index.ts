/**
 * @caligraphy/client — the TypeScript client Booklet (or any Node service)
 * uses to talk to Caligraphy.
 *
 * Deliberately dependency-free: `fetch` for transport, `node:crypto` for
 * callback signature verification. A queue client that brings its own
 * HTTP stack and retry framework is a queue client someone has to audit.
 *
 * The shape mirrors Caligraphy's actual guarantees:
 *  - `submit` with an idempotencyKey is safe to retry blindly — a replay
 *    returns the ORIGINAL job (Caligraphy dedupes on (type, key)).
 *  - `waitForResult` polls; terminal states resolve (including failures —
 *    a failed JOB is a successful POLL), and only transport errors reject
 *    after retries.
 *  - `verifySignature` implements the receiver half of Caligraphy's
 *    HMAC-signed callbacks, constant-time, with a replay-window check.
 */
import { createHmac, timingSafeEqual } from "node:crypto";

export type JobStatus =
  | "PENDING"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "RETRYING"
  | "DEAD_LETTER"
  | "CANCELLED";

export interface Job {
  id: string;
  type: string;
  queue: string;
  status: JobStatus;
  priority: "high" | "default" | "low";
  payload?: unknown;
  result?: unknown;
  error?: string;
  idempotencyKey?: string;
  attemptCount: number;
  maxAttempts: number;
  createdAt: string;
  scheduledAt: string;
  startedAt?: string;
  completedAt?: string;
}

export interface SubmitOptions {
  queue?: string;
  priority?: "high" | "default" | "low";
  maxAttempts?: number;
  idempotencyKey?: string;
  delaySeconds?: number;
  scheduledAt?: Date;
}

export interface JobResult {
  id: string;
  status: JobStatus;
  result?: unknown;
  error?: string;
}

export class CaligraphyError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly code: string,
  ) {
    super(message);
    this.name = "CaligraphyError";
  }
}

export interface CaligraphyClientOptions {
  baseUrl: string;
  token: string;
  /** Per-request timeout. Default 10s. */
  timeoutMs?: number;
  fetch?: typeof fetch;
}

export class CaligraphyClient {
  private readonly base: string;
  private readonly token: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: CaligraphyClientOptions) {
    this.base = opts.baseUrl.replace(/\/$/, "");
    this.token = opts.token;
    this.timeoutMs = opts.timeoutMs ?? 10_000;
    this.fetchImpl = opts.fetch ?? fetch;
  }

  /** Submit a job. With an idempotencyKey, safe to retry: a replay
   * returns the original job (HTTP 200 rather than 201, same id). */
  async submit(type: string, payload: unknown, options: SubmitOptions = {}): Promise<Job> {
    return this.request<Job>("POST", "/api/v1/jobs", {
      type,
      payload,
      options: {
        ...options,
        scheduledAt: options.scheduledAt?.toISOString(),
      },
    });
  }

  async getJob(id: string): Promise<Job> {
    return this.request<Job>("GET", `/api/v1/jobs/${encodeURIComponent(id)}`);
  }

  async getResult(id: string): Promise<JobResult> {
    return this.request<JobResult>("GET", `/api/v1/jobs/${encodeURIComponent(id)}/result`);
  }

  /** Cancel: outright for queued jobs, cooperative for running ones. */
  async cancel(id: string): Promise<void> {
    await this.request("DELETE", `/api/v1/jobs/${encodeURIComponent(id)}`);
  }

  /**
   * Poll until the job is terminal. Resolves for EVERY terminal state —
   * a job that failed is information, not a transport error; check
   * `status`. Polling starts fast and backs off to `maxIntervalMs`.
   */
  async waitForResult(
    id: string,
    opts: { timeoutMs?: number; maxIntervalMs?: number } = {},
  ): Promise<JobResult> {
    const deadline = Date.now() + (opts.timeoutMs ?? 120_000);
    let interval = 250;
    const maxInterval = opts.maxIntervalMs ?? 3_000;
    for (;;) {
      const res = await this.getResult(id);
      if (
        res.status === "COMPLETED" ||
        res.status === "FAILED" ||
        res.status === "DEAD_LETTER" ||
        res.status === "CANCELLED"
      ) {
        return res;
      }
      if (Date.now() > deadline) {
        throw new CaligraphyError(`timed out waiting for job ${id} (last status ${res.status})`, 0, "timeout");
      }
      await new Promise((r) => setTimeout(r, interval));
      interval = Math.min(interval * 2, maxInterval);
    }
  }

  private async request<T = unknown>(method: string, path: string, body?: unknown): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const resp = await this.fetchImpl(this.base + path, {
        method,
        headers: {
          Authorization: `Bearer ${this.token}`,
          ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
        },
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      const text = await resp.text();
      const data = text ? JSON.parse(text) : {};
      if (!resp.ok && resp.status !== 202) {
        throw new CaligraphyError(data.message ?? resp.statusText, resp.status, data.error ?? "error");
      }
      return data as T;
    } finally {
      clearTimeout(timer);
    }
  }
}

/**
 * Verify a Caligraphy callback delivery (the receiver half of http.callback).
 *
 * Caligraphy signs `${timestamp}.${rawBody}` with HMAC-SHA256 and sends:
 *   X-Caligraphy-Signature: v1=<hex>
 *   X-Caligraphy-Timestamp: <unix seconds>
 *
 * Verify against the RAW request body bytes — parse after, never before:
 * any JSON re-serialization can reorder keys and change the bytes.
 * `toleranceSeconds` bounds replay: a captured request goes stale.
 */
export function verifySignature(params: {
  secret: string;
  rawBody: string | Buffer;
  signatureHeader: string | undefined;
  timestampHeader: string | undefined;
  toleranceSeconds?: number;
}): boolean {
  const { secret, rawBody, signatureHeader, timestampHeader } = params;
  if (!signatureHeader || !timestampHeader) return false;

  const ts = Number(timestampHeader);
  if (!Number.isFinite(ts)) return false;
  const tolerance = params.toleranceSeconds ?? 300;
  if (Math.abs(Date.now() / 1000 - ts) > tolerance) return false;

  const presented = signatureHeader.startsWith("v1=") ? signatureHeader.slice(3) : signatureHeader;
  const mac = createHmac("sha256", secret);
  mac.update(`${ts}.`);
  mac.update(rawBody);
  const expected = mac.digest("hex");

  const a = Buffer.from(presented, "hex");
  const b = Buffer.from(expected, "hex");
  return a.length === b.length && timingSafeEqual(a, b);
}
