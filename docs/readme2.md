# NaaS — Notification as a Service

A multi-tenant Notification-as-a-Service platform built with NestJS, PostgreSQL, Redis, and Socket.IO. Send notifications across Email, SMS, Push, Webhook, and In-App channels through a single unified API.

---

## Features

- **Multi-channel delivery** — EMAIL, SMS, PUSH, WEBHOOK, IN_APP through one trigger endpoint
- **Multi-tenant isolation** — every resource (contacts, templates, credentials, notifications) is scoped to a tenant workspace
- **Template engine** — Handlebars/Liquid-style `{{variable}}` compilation at runtime
- **Reliable async processing** — BullMQ job queue with automatic retry and exponential backoff
- **Idempotent API** — replay-safe POST requests via `x-idempotency-key` header
- **Per-tenant rate limiting** — sliding window algorithm via Redis Sorted Sets, enforced across all instances
- **Real-time in-app feed** — Redis Pub/Sub + Socket.IO WebSocket gateway with isolated tenant rooms
- **Dual-cache feed engine** — O(log N) feed reads via Redis Sorted Sets, no database polling
- **Horizontal scaling** — fully stateless app servers; all shared state lives in Redis and PostgreSQL
- **Audit log** — append-only `NotificationEvent` ledger recording every lifecycle milestone

---

## Tech Stack

| Layer | Technology |
|---|---|
| Framework | NestJS (TypeScript) |
| Database | PostgreSQL via Prisma ORM |
| Cache & Queue | Redis (ioredis) |
| Job Queue | BullMQ |
| Real-time | Socket.IO |
| Auth | SHA-256 API keys + short-lived JWTs |

---

## Architecture Overview

```
POST /v1/notifications/trigger
        │
        ▼
DistributedRedisLimiterGuard     ← sliding window rate limit per tenant
        │
        ▼
IdempotencyInterceptor           ← duplicate request prevention via Redis Hash
        │
        ▼
Validation + Template Compile    ← contact lookup, template fetch, {{var}} injection
        │
        ▼
PostgreSQL: PENDING              ← notification row created, audit event appended
        │
        ▼
BullMQ Job Enqueue               ← job added to notification_delivery queue
        │
        ▼
201 → { success, jobId }         ← response returned immediately

        ↓  (async, worker picks up job)

NotificationProcessor (WorkerHost)
        │
        ├── provider.send()      ← SendGrid / Twilio / FCM / mock
        ├── PostgreSQL: SENT
        │
        └── IN_APP only:
              redis.publish('platform:notifications')
                    │
                    ▼
            Redis Pub/Sub ──→ All server instances
                                    │
                              WebSocket Gateway
                              socket.to(room).emit('notification_received')
                                    │
                              Browser (live feed update)
```

---

## API Reference

### Authentication

All endpoints require an `x-api-key` header. Keys are scoped to a tenant workspace. Raw keys are never stored — only SHA-256 digests.

```
x-api-key: ntf_live_your_key_here
```

### Trigger a Notification

```
POST /v1/notifications/trigger
```

**Headers**

| Header | Required | Description |
|---|---|---|
| `x-api-key` | Yes | Tenant API key |
| `x-idempotency-key` | Yes | Client-generated unique key per logical operation (UUID recommended) |

**Body**

```json
{
  "workflow": "welcome-alert",
  "recipientId": "user_dev_99",
  "data": {
    "name": "Kunal",
    "action": "account verified"
  }
}
```

| Field | Type | Description |
|---|---|---|
| `workflow` | string | Template slug — matches `Template.slug` for this tenant |
| `recipientId` | string | Contact's `externalId` within the tenant workspace |
| `data` | object | Variables injected into `{{variable}}` placeholders in the template |

**Response `201`**

```json
{
  "success": true,
  "jobId": "42",
  "message": "Notification successfully queued for delivery"
}
```

**Errors**

| Code | Reason |
|---|---|
| `400` | Missing `x-idempotency-key` header, or key reused with different request body |
| `404` | Contact not found or inactive, or template/workflow not found |
| `409` | Idempotency lock active — duplicate request is currently being processed |
| `429` | Rate limit exceeded for this tenant |

---

### Get Notification Feed

```
GET /v1/notifications/feed?recipientId=user_dev_99&unreadOnly=false
```

Returns a paginated notification feed for a user. Served from Redis cache (O(log N)) with automatic PostgreSQL fallback on cache miss.

**Query params**

| Param | Type | Default | Description |
|---|---|---|---|
| `recipientId` | string | required | Target user's `externalId` |
| `unreadOnly` | boolean | `false` | Return only unread notifications when `true` |

---

### Mark Notification as Read

```
PATCH /v1/notifications/:id/read
```

Sets `isRead = true` in PostgreSQL, removes the entry from the `feed:unread` cache, and re-inserts the updated copy into `feed:all` at the same score position.

---

### Mark All Notifications as Read

```
POST /v1/notifications/read-all
```

Bulk update — sets all unread notifications for the user to `isRead = true`. Both Redis cache keys (`feed:all`, `feed:unread`) are deleted and rebuilt on the next feed request.

**Body**

```json
{
  "recipientId": "user_dev_99"
}
```

---

## Idempotency

Every `POST /v1/notifications/trigger` request **must** include an `x-idempotency-key` header. This is a client-generated unique string (UUID recommended) that identifies one logical send operation.

**Behaviour:**

| Scenario | Response |
|---|---|
| First request with this key | Processes normally, caches the response |
| Replay with same key + same body | Returns the original cached response immediately — no duplicate job enqueued |
| Replay with same key + different body | `400 Bad Request` — key reuse detected |
| Concurrent duplicate (still processing) | `409 Conflict` — lock is active |

Keys expire after **24 hours**.

```
// Example
x-idempotency-key: 7f3e2c1a-4b5d-4e6f-9a0b-1c2d3e4f5a6b
```

---

## Rate Limiting

Requests to `POST /v1/notifications/trigger` are rate-limited per tenant using a **sliding window algorithm** backed by Redis Sorted Sets.

- Limit state lives in Redis — correctly enforced across all horizontally scaled instances
- Unlike a fixed window, there is no burst spike at clock boundaries
- On limit breach: `429 Too Many Requests`
- On Redis failure: fail-open (requests pass through, auth still runs)

Configure `RATE_LIMIT_CONFIG.WINDOW_DURATION_MS` and `RATE_LIMIT_CONFIG.MAX_REQUESTS` in `notification.constants.ts`.

---

## Real-Time WebSocket Feed

Clients connect via Socket.IO and receive live `notification_received` events for IN_APP notifications.

### Connection

```javascript
import { io } from 'socket.io-client';

const socket = io('http://your-naas-host', {
  auth: {
    token: '<signed_jwt_from_your_backend>'
  }
});

socket.on('notification_received', (notification) => {
  console.log('New notification:', notification);
});
```

### JWT

The JWT must be signed by your backend (using the shared `JWT_SECRET`) and contain:

```json
{
  "tenantId": "your-tenant-id",
  "userId": "user_dev_99"
}
```

Your backend signs the JWT server-side and passes it to the browser. The raw API key is never exposed to the client.

### Horizontal scaling

The WebSocket gateway uses Redis Pub/Sub as a cross-instance broadcast layer. Any server can process a notification trigger — the instance holding the user's socket connection will always receive the event and deliver it to the browser.

---

## Job Queue

Notification delivery runs through a **BullMQ** job queue backed by Redis. This decouples the HTTP response from provider dispatch and provides:

- **Automatic retry** — 3 attempts with exponential backoff (5s → 10s → 20s)
- **Crash recovery** — stalled jobs are re-queued on server restart
- **Observability** — queue state is inspectable via Bull Board or similar dashboards
- **Backpressure** — queue absorbs traffic spikes without overwhelming providers

The API returns a `jobId` in the trigger response which can be used to track job state.

---

## Cache Design

### Feed cache (Redis Sorted Sets)

Two Sorted Sets are maintained per user, scored by notification timestamp:

| Key | Contents |
|---|---|
| `feed:all:{tenantId}:{recipientId}` | All notifications |
| `feed:unread:{tenantId}:{recipientId}` | Unread notifications only |

- Max **100 entries** per key — oldest trimmed on every insert
- **7-day rolling TTL** — inactive user caches expire automatically
- Feed reads return in **under 2ms** from cache (O(log N))

### Idempotency cache (Redis Hash)

```
idempotency:{tenantId}:{x-idempotency-key}
```

Hash fields: `status`, `request_hash`, `response_body`. TTL: 24 hours.

### API key cache (Redis String)

```
apikey:{sha256_of_key}  →  tenantId
```

Avoids a database query on every inbound request. TTL: 24 hours.

### Rate limit state (Redis Sorted Set)

```
ratelimit:{tenantId}
```

Sliding window of request timestamps. Trimmed on every request. Rolling TTL.

---

## Data Model Summary

| Model | Purpose |
|---|---|
| `Tenant` | Root workspace anchor — all resources belong to a tenant |
| `ApiKey` | SHA-256 hashed credentials for server-to-server auth |
| `Contact` | End-user routing profiles — email, phone, device tokens |
| `Template` | Notification layouts with `{{variable}}` slots, scoped to channel |
| `ChannelConfig` | Encrypted provider credentials per channel per tenant |
| `Notification` | Per-send instance — compiled body, delivery state, timestamps |
| `NotificationEvent` | Append-only audit log — every status transition recorded |

---

## Project Structure

```
src/
├── notification/
│   ├── constants/
│   │   └── notification.constants.ts   ← all Redis keys, queue names, event names
│   ├── guards/
│   │   └── distributed-redis-limiter.guard.ts
│   ├── interceptors/
│   │   └── idempotency.interceptor.ts
│   ├── provider/
│   │   └── provider.factory.ts         ← resolves SendGrid / Twilio / FCM / mock
│   ├── notification.controller.ts
│   ├── notification.gateway.ts         ← Socket.IO + Redis Pub/Sub subscriber
│   ├── notification.module.ts
│   ├── notification.processor.ts       ← BullMQ WorkerHost
│   └── notification.service.ts
├── redis/
│   └── redis.module.ts                 ← REDIS_CLIENT + REDIS_SUBSCRIBER providers
├── prisma/
│   └── prisma.service.ts
└── main.ts
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string |
| `REDIS_HOST` | Redis hostname (default: `localhost`) |
| `REDIS_PORT` | Redis port (default: `6379`) |
| `JWT_SECRET` | Secret used to sign and verify WebSocket JWTs |

---

## Getting Started

```bash
# Install dependencies
npm install

# Run database migrations
npx prisma migrate dev

# Start the server
npm run start:dev
```

Redis and PostgreSQL must be running. Configure connection details in `.env`.

---

## Internal Documentation

For full architecture details — infrastructure diagrams, schema definitions, Redis key design, BullMQ worker internals, rate limiting algorithm, idempotency lifecycle, WebSocket gateway, security model, and implementation decisions — see [`docs/system-architecture.md`](./docs/system-architecture.md).