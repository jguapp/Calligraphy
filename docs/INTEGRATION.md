# Integrating Booklet (or any app) with Forge

Forge exists because Booklet's own source comments describe four gaps —
extraction blocking the save request, embedding indexing as
fire-and-forget, webhook delivery with no retry, and no scheduler at all.
This guide is the drop-in path for closing them. **Nothing in this guide
requires changes to Forge**, and no changes have been made to Booklet's
repository — this documents exactly what Booklet would add, on Booklet's
side, when it adopts Forge.

The boundary rule that keeps both codebases healthy: **Booklet talks to
Forge only through the HTTP API.** No shared database, no shared Redis
keys, no imported packages. Forge stays generic infrastructure; Booklet
stays a product. Either can be rewritten without the other noticing.

```
Booklet API ──POST /api/v1/jobs──────────▶ Forge
     ▲                                       │
     │            (Forge executes: its own   │
     │             handler, or POSTs back    │
     │             via http.callback)        │
     └──────HMAC-signed callback ────────────┘
             …or Booklet polls GET /jobs/:id/result
```

## Setup

Compose-network deployments put both stacks on one network and point
Booklet at `http://forge-api:8080`. Environment on Booklet's side:

```bash
FORGE_URL=http://forge-api:8080
FORGE_TOKEN=fg_…            # one of forge's FORGE_API_TOKENS
FORGE_CALLBACK_SECRET=…     # same value as forge's FORGE_CALLBACK_SECRET
```

Client: `clients/ts` in this repo (`@forge/client`) — dependency-free,
`fetch` + `node:crypto`.

## Pattern 1 — native Forge handler (`article.analysis`)

For work Forge can own end-to-end. Booklet submits the article's
*already-extracted* text and retrieves structured analysis (TextRank
keywords, extractive summary, readability, language):

```ts
import { ForgeClient } from "@forge/client";

const forge = new ForgeClient({ baseUrl: env.FORGE_URL, token: env.FORGE_TOKEN });

// In the article-save path, after extraction succeeds. The idempotency
// key means a Booklet retry (or a double-submit from a webhook replay)
// can never analyze the same article twice.
const job = await forge.submit(
  "article.analysis",
  { articleId: article.id, text: article.extractedText },
  { idempotencyKey: `analysis:${article.id}` },
);

// Later — a poll from a status endpoint, or awaited where acceptable:
const res = await forge.waitForResult(job.id, { timeoutMs: 60_000 });
if (res.status === "COMPLETED") {
  // res.result: { keywords, summary, fleschReadingEase, language, … }
}
```

This closes Booklet gap #2's shape (durable background computation with
retries instead of `void promise.catch(console.error)`): if a worker
dies mid-analysis, the job is reaped and re-run; if it fails transiently,
it retries with backoff; if it fails permanently, it's visible in the
DLQ with its attempt history — not a line lost in stdout.

## Pattern 2 — `http.callback` (Forge as the retry engine)

For work whose logic must stay in Booklet (extraction needs Readability
and JSDOM; webhook delivery needs Booklet's per-user records). Forge
holds the job, the schedule, the retries, and the DLQ; at execution time
it POSTs the payload back to a Booklet endpoint:

```ts
// Submitting: "call me back with this payload, retrying until I 2xx"
await forge.submit(
  "http.callback",
  {
    url: "http://booklet-api:4000/internal/forge/extract",
    body: { articleId: article.id, url: article.url },
    event: "article.extract",
  },
  { idempotencyKey: `extract:${article.id}`, maxAttempts: 5 },
);
```

The receiving endpoint (new, small, internal) verifies the signature
against the **raw body** and returns status codes that mean what Forge
documents: `2xx` done, other `4xx` permanent (don't retry), `5xx`/`429`
transient (retry with backoff):

```ts
import { verifySignature } from "@forge/client";

app.post("/internal/forge/extract", { config: { rawBody: true } }, async (req, reply) => {
  const ok = verifySignature({
    secret: env.FORGE_CALLBACK_SECRET,
    rawBody: req.rawBody,                              // BYTES, not re-serialized JSON
    signatureHeader: req.headers["x-forge-signature"],
    timestampHeader: req.headers["x-forge-timestamp"],
  });
  if (!ok) return reply.code(401).send();

  const { articleId, url } = req.body;
  try {
    await runExtraction(articleId, url);               // Booklet's existing service code
    return reply.code(200).send({ ok: true });
  } catch (err) {
    if (err instanceof ExtractionError) {
      return reply.code(422).send({ error: err.message }); // permanent: bad page, don't retry
    }
    return reply.code(503).send();                     // transient: Forge retries with backoff
  }
});
```

**Idempotency note, because at-least-once means it**: the handler above
may run twice for one job (Forge's documented guarantee is at-least-once
execution, exactly-once *result persistence*). Booklet's extraction
writes are naturally idempotent (re-extracting an article converges), and
the `X-Forge-Job-Id` / `X-Forge-Attempt` headers are available where a
receiving endpoint needs its own dedup.

## Mapping the four Booklet gaps

| Booklet gap (its own comment) | Forge pattern |
|---|---|
| Extraction blocks `POST /api/articles` for up to ~15s + 30 image fetches | `http.callback` → extraction endpoint; save returns instantly with `extractionStatus: PENDING`, reader polls or gets pushed |
| Embeddings: *"failures are logged and dropped… backfill script re-attempts"* | `http.callback` → embedding endpoint with `maxAttempts: 5`; failures land in the DLQ, not the void |
| Webhooks: *"at-least-once, no retry queue yet"* | `http.callback` per delivery — Forge **is** the "real retry-with-backoff" that comment defers |
| Feeds: *"no background worker to poll feeds on a schedule"* | submit with `delaySeconds`/`scheduledAt`; the completing callback submits the next poll (a self-perpetuating schedule with per-tick retry semantics) |

## What Booklet must NOT do

- Reach into Forge's Postgres or Redis. The API is the whole contract.
- Treat a callback 2xx as "exactly once happened". It means *at least
  once, and the result is recorded once*.
- Skip signature verification because "it's on the internal network".
  The check is three lines and makes network topology a defense in depth
  rather than the only defense.
