# NaaS — Notification-as-a-Service

A multi-tenant notification platform that lets any client application send **email, SMS, push, and in-app notifications** through a single API. Built to understand how real notification infrastructure works at scale — async processing, real-time delivery, high-performance caching, horizontally scalable WebSocket broadcasting, and distributed rate limiting.

> This is a solo project. Built from scratch, no boilerplate.

---

## What it does

- One API call triggers a notification to any channel
- Templates are pre-defined per tenant with dynamic variable support (`{{name}}`, etc.)
- In-app notifications stream to the browser **in real-time over WebSockets**
- A Redis-backed feed returns notification history in under 2ms
- **WebSocket broadcasting works across multiple server instances** via Redis Pub/Sub
- **Per-tenant rate limiting enforced across all instances** via Redis sliding window
- Every notification has a full **audit trail** of lifecycle events

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Node.js |
| Framework | NestJS |
| Database | PostgreSQL via Prisma ORM |
| Cache | Redis Sorted Sets (ioredis) |
| Pub/Sub | Redis Pub/Sub (dedicated subscriber connection) |
| Real-time | Socket.IO |
| Auth | SHA-256 API key hashing + signed JWT for WebSocket |
| Rate Limiting | Redis Sorted Set sliding window, per tenant |

---

## Getting Started

### Prerequisites

- Node.js 18+
- PostgreSQL
- Redis

### Setup

```bash
git clone https://github.com/your-username/naas
cd naas
npm install
```

Create a `.env` file:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/naas
REDIS_URL=redis://localhost:6379
JWT_SECRET=your_secret_here
PORT=3001
WS_PORT=3002
```

Run database migrations:

```bash
npx prisma migrate dev
```

Start the server:

```bash
npm run dev
```

### Testing horizontal scaling locally

Simulate multiple server instances on your machine without a load balancer:

```bash
# Terminal 1
PORT=3001 npm run start:dev

# Terminal 2
PORT=3002 npm run start:dev

# Terminal 3
PORT=3003 npm run start:dev
```

Connect a WebSocket client to port `3001`, send a trigger request to port `3002`, and observe the notification arrive on the `3001` socket. That proves Redis Pub/Sub is working across instances.

To verify distributed rate limiting, hammer any instance with requests beyond the configured limit — the 429 will fire regardless of which instance receives each individual request, because the counter lives in shared Redis.

---

## Project Structure

```
src/
├── notification/
│   ├── constants/
│   │   └── notification.constants.ts   # Redis channel names, WS events, cache key factory, rate limit config
│   ├── events/
│   │   └── notification-event.ts       # Typed event payload definitions
│   ├── guards/
│   │   └── distributed-redis-limiter.guard.ts  # Sliding window rate limiter
│   ├── notification.gateway.ts         # WebSocket gateway + Redis Pub/Sub subscriber
│   ├── notification.service.ts         # Trigger logic + Redis publisher
│   └── notification.controller.ts      # HTTP route handlers
├── redis/
│   └── redis.module.ts                 # Redis client + dedicated subscriber client
└── prisma/
    └── schema.prisma
```

---

## How it Works

### The core flow

```
POST /v1/notifications/trigger
        │
        ├── 0. DistributedRedisLimiterGuard
        │       ├── hash API key → look up tenantId (Redis cache → DB fallback)
        │       ├── ZREMRANGEBYSCORE: evict stale entries from sliding window
        │       ├── ZADD: record this request with timestamp score
        │       ├── ZCARD: count requests in window
        │       └── if count > MAX_REQUESTS → 429 Too Many Requests
        │
        ├── 1. Hash API key → resolve tenant
        ├── 2. Look up contact by externalId
        ├── 3. Fetch template by (slug + channel)
        ├── 4. Compile {{variables}} into body
        ├── 5. Write Notification row (status: PENDING)
        └── 6. Return 201 immediately ← fast response, caller never waits
                │
                └── background worker picks up async
                        ├── status → PROCESSING
                        ├── simulate provider dispatch
                        ├── status → SENT
                        └── redis.publish('platform:notifications', payload)
                                │
                                └── Redis broadcasts to ALL server instances
                                        │
                                        └── whichever instance holds the
                                            user's socket pushes to browser
```

### Why async?

The HTTP response returns before the notification is actually sent. This keeps API response times fast regardless of whether the downstream provider (SendGrid, Twilio, etc.) is slow. The client gets a notification `id` immediately and can track status via the audit log.

### Why dual Redis cache?

Every in-app notification is written into two Redis Sorted Sets simultaneously, scored by Unix millisecond timestamp:

- `feed:all:{tenantId}:{userId}` — every notification, read and unread
- `feed:unread:{tenantId}:{userId}` — only unread items, for badge counts

A feed request never touches PostgreSQL on a cache hit — it's a single sorted set range query in O(log N). The cache holds the 100 most recent notifications per user with a 7-day rolling TTL.

### Why Redis Pub/Sub for WebSocket broadcasting?

When running a single server, an in-process event emitter works fine. Under a load balancer with multiple instances it breaks:

- A user's WebSocket connection is persistent — it lives on whichever server they first connected to
- An HTTP trigger request goes to whichever server the load balancer picks — could be any instance
- If the trigger is processed on Server B but the user's socket is on Server A, the in-memory event never crosses that boundary

Redis Pub/Sub sits outside all servers. Any instance publishes to it, every instance receives the broadcast. Whichever instance holds the user's socket pushes to the browser. The others silently ignore it.

This required two separate Redis connections:

```
redisClient      → regular operations (GET, SET, ZADD, pipeline, publish)
redisSubscriber  → dedicated to subscribe mode only
```

A connection in subscribe mode is blocked — Redis won't let it run any other commands. So the subscriber is always a dedicated separate client.

### Why Redis sliding window for rate limiting?

A fixed window counter (`INCR` + `EXPIRE`) resets at a hard clock boundary. A tenant can drain their full quota at second 59 of a window and then immediately fire again at second 0 of the next — effectively doubling throughput at every boundary.

A sliding window stores each request as a timestamped entry in a Redis Sorted Set. On every request it evicts entries older than the window and counts what remains. There is no reset boundary — the window rolls continuously with real time.

Because the counter lives in Redis (not instance memory), it is correctly accumulated across all horizontally scaled server instances. A tenant splitting 100 requests across three servers still sees a single counter.

The guard is intentionally **fail-open**: if Redis is temporarily unavailable, requests pass through rather than taking down the API. Authentication still rejects invalid keys. Rate limiting is a protection layer, not an authentication layer.

### WebSocket auth

Exposing an API key to the browser would be a security hole. Instead:

1. The tenant's own backend signs a short-lived JWT `{ tenantId, userId, expires }`
2. The browser passes this token to the WebSocket gateway on connect
3. The gateway verifies the signature and places the client into an isolated room: `tenantId:userId`

A stolen token only exposes that specific user's own feed. Tampering with the payload breaks the cryptographic signature — the gateway closes the connection immediately.

---

## API Reference

Base URL: `http://localhost:3001`

All requests require:

```
x-api-key: your_raw_api_key
```

Requests exceeding the per-tenant rate limit receive:

```
HTTP 429 Too Many Requests
{
  "statusCode": 429,
  "error": "Too many Request",
  "message": "Rate Limit threshold exceeded. Dynamic shield blocked request"
}
```

---

### Notifications

#### Trigger a notification

```
POST /v1/notifications/trigger
```

Compiles a template, creates a notification record, and dispatches it asynchronously. Returns immediately — processing happens in the background.

**Guards:** `DistributedRedisLimiterGuard` — enforces per-tenant sliding window rate limit before any business logic runs.

**Request body**

```json
{
  "workflow": "welcome-alert",
  "recipientId": "user_dev_99",
  "data": {
    "name": "Kunal"
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `workflow` | string | Yes | The template slug to use. |
| `recipientId` | string | Yes | Your internal user ID (maps to a Contact). |
| `data` | object | No | Variables to inject into the template body. |

**Response** `201 Created`

```json
{
  "id": "notif_abc123",
  "status": "PENDING"
}
```

**Errors**

| Code | Reason |
|---|---|
| `401` | Invalid or missing API key. |
| `404` | No contact found for this `recipientId`. |
| `404` | No active template found for this `workflow`. |
| `429` | Per-tenant rate limit exceeded. |

---

#### Get notification feed

```
GET /v1/notifications/feed
```

Returns a user's notification history. Hits Redis cache first — falls back to PostgreSQL on miss and warms the cache for next time.

**Query params**

| Param | Type | Required | Description |
|---|---|---|---|
| `recipientId` | string | Yes | Your internal user ID. |
| `unreadOnly` | boolean | No | `true` returns only unread notifications. Default `false`. |

**Response** `200 OK`

```json
[
  {
    "id": "notif_abc123",
    "body": "Hi Kunal, thanks for signing up.",
    "channel": "IN_APP",
    "isRead": false,
    "createdAt": "2024-01-01T00:00:00Z"
  }
]
```

---

#### Mark a notification as read

```
PATCH /v1/notifications/:id/read
```

Marks a single notification as read. Updates PostgreSQL, then surgically updates both Redis cache sets — removes from `feed:unread` and replaces the stale entry in `feed:all` with an updated `isRead: true` version at the same score position.

**Response** `200 OK`

```json
{
  "id": "notif_abc123",
  "isRead": true,
  "readAt": "2024-01-01T00:01:00Z"
}
```

**Errors**

| Code | Reason |
|---|---|
| `404` | Notification not found or belongs to a different tenant. |

---

#### Mark all notifications as read

```
POST /v1/notifications/read-all
```

Bulk marks all unread notifications as read for a user. Rather than updating individual cache entries, drops both cache keys entirely — rebuilt fresh on the next feed request.

**Request body**

```json
{
  "recipientId": "user_dev_99"
}
```

**Response** `200 OK`

```json
{
  "updated": 12
}
```

---

## Real-Time (WebSocket)

Connect via Socket.IO on port `3002`.

```javascript
const socket = io('http://localhost:3002', {
  auth: { token: 'your_signed_jwt' }
});

socket.on('notification_received', (notification) => {
  console.log('New notification:', notification);
});
```

The server places you into a private room on connect (`tenantId:userId`). You only receive events for your own user — no cross-tenant or cross-user data leaks.

---

## Database Schema

Full schema lives in `prisma/schema.prisma`. Key models:

| Model | Purpose |
|---|---|
| `Tenant` | Root workspace. Every other model belongs to a tenant. |
| `ApiKey` | Hashed credentials for server-to-server auth. |
| `Contact` | End-users mapped to a tenant by `externalId`. |
| `Template` | Notification layouts with Handlebars variable support. |
| `ChannelConfig` | Per-tenant provider credentials (SendGrid, Twilio, FCM). |
| `Notification` | Every notification instance with full status tracking. |
| `NotificationEvent` | Append-only audit log of every lifecycle event. |

---

## Redis Key Reference

| Key Pattern | Type | TTL | Purpose |
|---|---|---|---|
| `feed:all:{tenantId}:{userId}` | ZSET | 7 days rolling | Full notification feed per user. |
| `feed:unread:{tenantId}:{userId}` | ZSET | 7 days rolling | Unread-only feed for badge counts. |
| `apikey:{keyHash}` | String | 24h | Maps API key hash → tenantId, avoids DB lookup on hot paths. |
| `ratelimit:{tenantId}` | ZSET | Rolling (window size) | Sliding window request log per tenant. |

---

## What's Next

The current implementation is architected correctly for the core flow. These are the gaps to close before production use:

**Job Queue (BullMQ)**
Background processing is a forked async function right now. Replacing it with BullMQ adds proper retries, concurrency limits, per-tenant rate limiting at the queue level, and a dead-letter queue for permanently failed jobs. The async architecture is already in place — this is a drop-in upgrade.

**Real Provider Adapters**
Dispatch is currently simulated with a timeout. The `ChannelConfig` model already stores per-tenant credentials for SendGrid, Twilio, and FCM. Plugging in real adapters is the next step.

**API key revocation cache invalidation**
When an API key is revoked, the `apikey:{keyHash}` cache entry survives for up to 24 hours. The downstream auth interceptor will still reject the revoked key, but the rate limit counter will continue attributing requests to the old tenant until the TTL expires. The fix is to `DEL apikey:{keyHash}` in Redis at the moment of revocation.

**Configurable rate limits per tenant**
`RATE_LIMIT_CONFIG` is currently a global constant. A future version could store per-tenant overrides in PostgreSQL (or Redis itself), fetched and cached alongside the tenant ID lookup so high-volume tenants can be given higher limits without a code change.

---

## What I Learned Building This

- How to structure a multi-tenant system where every query is scoped by `tenantId` without leaking data across workspaces
- Why you maintain separate read and unread cache sets — filtering a single set on every request defeats the purpose of caching
- The exact failure mode of in-memory event emitters under horizontal scaling, and why Redis Pub/Sub is the standard fix
- Why a Redis subscriber connection must be dedicated and separate — a subscribed connection is blocked from all other commands
- How WebSocket room isolation works and why JWTs are the right auth mechanism for real-time connections vs passing API keys to the browser
- The nuke-and-rebuild invalidation pattern for bulk operations — cheaper than surgically updating dozens of individual cache entries
- Why a sliding window rate limiter beats a fixed window counter — no boundary spike, no artificial doubling of quota at the reset moment
- How to implement distributed rate limiting correctly — storing state in Redis rather than instance memory so the limit holds across all scaled servers
- The fail-open pattern for infrastructure guards — rate limiting should protect the system, not become a single point of failure

---

*Templates, Contacts, and ChannelConfig management endpoints coming next.*