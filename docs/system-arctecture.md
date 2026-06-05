# System Architecture — Internal Reference

> This document is for understanding how the system is built internally.
> It is not customer-facing. Update it every time a significant implementation decision is made.

---

## Table of Contents

1. [Infrastructure Overview](#1-infrastructure-overview)
2. [Database Schema](#2-database-schema)
   - [Tenant](#21-tenant)
   - [ApiKey](#22-apikey)
   - [Contact](#23-contact)
   - [Template](#24-template)
   - [ChannelConfig](#25-channelconfig)
   - [Notification](#26-notification)
   - [NotificationEvent](#27-notificationevent-audit-log)
   - [Enumerations](#28-enumerations)
3. [Core Functional Workflows](#3-core-functional-workflows)
4. [Real-Time Streaming & Cache Synchronization](#4-real-time-streaming--cache-synchronization)
5. [State Invalidation & Mutation Flows](#5-state-invalidation--mutation-flows)
6. [Redis Pub/Sub — Horizontal Scaling Backbone](#6-redis-pubsub--horizontal-scaling-backbone)
7. [Redis Client Architecture](#7-redis-client-architecture)
8. [Cache Layer Design](#8-cache-layer-design)
9. [WebSocket Gateway Internals](#9-websocket-gateway-internals)
10. [Security Architecture](#10-security-architecture)
11. [Constants & Key Management](#11-constants--key-management)
12. [Known Issues & Bugs](#12-known-issues--bugs)
13. [How to Extend This Documentation](#13-how-to-extend-this-documentation)
14. [Implementation Log](#14-implementation-log)

---

## 1. Infrastructure Overview

### Single Instance (current local setup)

```
┌────────────────────────────────────────┐
│       Inbound HTTP API Trigger         │
└───────────────────┬────────────────────┘
                    │  (Sync Key Validation + Template Compilation)
                    ▼
┌────────────────────────────────────────┐
│   PostgreSQL — Notification PENDING    │
└───────────────────┬────────────────────┘
                    │  (Fork Async — Return 201 immediately)
                    ▼
┌────────────────────────────────────────┐
│    Async Background Processing         │
│    PROCESSING → SENT / FAILED          │
└───────────────────┬────────────────────┘
                    │  (redis.publish → platform:notifications)
                    ▼
┌────────────────────────────────────────┐
│    Redis Pub/Sub Broadcast             │
└──────────┬─────────────────┬───────────┘
           │                 │
           ▼                 ▼
┌──────────────────┐  ┌──────────────────────────┐
│  Redis ZSET      │  │  WebSocket Gateway        │
│  Dual-Cache      │  │  Isolated Tenant Rooms    │
│  feed:all        │  │  ${tenantId}:${userId}    │
│  feed:unread     │  └──────────────────────────┘
└──────────────────┘
```

### Multi-Instance (horizontal scaling)

```
                          Internet
                             │
                    ┌────────▼────────┐
                    │  Load Balancer  │
                    └────────┬────────┘
           ┌─────────────────┼─────────────────┐
           │                 │                 │
    ┌──────▼──────┐   ┌──────▼──────┐   ┌──────▼──────┐
    │  Server A   │   │  Server B   │   │  Server C   │
    │  NestJS     │   │  NestJS     │   │  NestJS     │
    └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
           │                 │                 │
           └─────────────────┼─────────────────┘
                             │
              ┌──────────────┼──────────────┐
              │                             │
    ┌─────────▼──────────┐       ┌──────────▼──────────┐
    │       Redis        │       │      PostgreSQL      │
    │  - Sorted Set cache│       │  - Persistent data   │
    │  - Pub/Sub channel │       │  - Source of truth   │
    │  (external, shared)│       │  (external, shared)  │
    └────────────────────┘       └─────────────────────┘
```

Every server instance is stateless and identical. Redis and PostgreSQL are external shared systems — no data lives inside any app server's memory permanently.

### System Layers

| Layer | Description |
|---|---|
| **API Ingest** | Receives multi-tenant notifications via secure HTTP REST. Validates API keys, compiles templates, maps contact profiles, and records tracking tokens. |
| **Async Processing** | Forks tasks away from the HTTP loop. Updates operational state and dispatches to channel providers without blocking the client response. |
| **Redis Pub/Sub** | Replaces the in-process event emitter as the broadcast layer. Any server can publish; every server receives and delivers to the right WebSocket room. |
| **WebSocket Gateway** | Manages full-duplex TCP connections via Socket.IO. Validates tenant credentials on handshake, isolates users into sandboxed rooms, and pushes live updates. |
| **Dual-Cache Engine** | Maintains Redis Sorted Sets for `feed:all` and `feed:unread` timelines. Resolves index queries in O(log N) without touching PostgreSQL. |

---

## 2. Database Schema

> Schema is defined in `prisma/schema.prisma` using **Prisma ORM over PostgreSQL**.

### 2.1 Tenant

Root anchor for all workspace resources. Every other model foreign-keys back to this table.

| Field | Type | Notes |
|---|---|---|
| `id` | `String` UUID PK | Auto-generated unique workspace identifier. |
| `name` | `String` | Human-readable business name. |
| `slug` | `String` `@unique` | URL-safe workspace identifier used in routing and keys. |
| `isActive` | `Boolean` default `true` | Global operational toggle. |
| `createdAt` | `DateTime` default `now()` | Record creation timestamp. |
| `updatedAt` | `DateTime` `@updatedAt` | Auto-updated on every write. |

```prisma
model Tenant {
  id        String   @id @default(uuid())
  name      String
  slug      String   @unique
  isActive  Boolean  @default(true)
  createdAt DateTime @default(now())
  updatedAt DateTime @updatedAt

  apiKeys       ApiKey[]
  notifications Notification[]
  templates     Template[]
  channels      ChannelConfig[]
  contacts      Contact[]

  @@map("tenants")
}
```

---

### 2.2 ApiKey

Stores cryptographic credential profiles for server-to-server authentication. Raw keys are **never** persisted — only a SHA-256 digest.

| Field | Type | Notes |
|---|---|---|
| `id` | `String` UUID PK | Unique key asset identifier. |
| `tenantId` | `String` FK → Tenant | Workspace association. |
| `name` | `String` | Human label, e.g. `"Production Core Key"`. |
| `keyHash` | `String` `@unique` | SHA-256 digest of the raw token. Never store the raw value. |
| `prefix` | `String` | Short prefix shown to users, e.g. `ntf_live_`. Never exposes the full key. |
| `scopes` | `String[]` | Array of permission scopes for fine-grained access control. |
| `isActive` | `Boolean` default `true` | Revocation toggle. |
| `expiresAt` | `DateTime?` optional | Optional expiry for key rotation policies. |
| `lastUsedAt` | `DateTime?` optional | Timestamp of the most recent successful authentication. |
| `createdAt` | `DateTime` default `now()` | Key generation timestamp. |

```prisma
model ApiKey {
  id         String    @id @default(uuid())
  tenantId   String
  name       String
  keyHash    String    @unique
  prefix     String
  scopes     String[]
  isActive   Boolean   @default(true)
  expiresAt  DateTime?
  lastUsedAt DateTime?
  createdAt  DateTime  @default(now())

  tenant Tenant @relation(fields: [tenantId], references: [id], onDelete: Cascade)

  @@index([tenantId])
  @@map("api_keys")
}
```

---

### 2.3 Contact

Maintains individual end-user routing channels mapped to a specific tenant workspace.

| Field | Type | Notes |
|---|---|---|
| `id` | `String` UUID PK | System routing index locator. |
| `tenantId` | `String` FK → Tenant | Workspace mapping link. |
| `externalId` | `String` | The tenant's own internal user ID, e.g. `user_dev_99`. |
| `email` | `String?` optional | Target email address. |
| `phone` | `String?` optional | Target SMS number. |
| `deviceTokens` | `String[]` | Array of FCM/APNS push device tokens for the PUSH channel. |
| `metadata` | `Json` default `{}` | Arbitrary key-value bag for tenant-specific enrichment. |
| `isActive` | `Boolean` default `true` | Delivery suppression toggle. |
| `createdAt` | `DateTime` default `now()` | Account registration timestamp. |
| `updatedAt` | `DateTime` `@updatedAt` | Auto-updated on every write. |

> **Constraint:** `@@unique([tenantId, externalId])` — prevents duplicate contacts per workspace without global ID collisions.

```prisma
model Contact {
  id           String   @id @default(uuid())
  tenantId     String
  externalId   String
  email        String?
  phone        String?
  deviceTokens String[]
  metadata     Json     @default("{}")
  isActive     Boolean  @default(true)
  createdAt    DateTime @default(now())
  updatedAt    DateTime @updatedAt

  tenant        Tenant         @relation(fields: [tenantId], references: [id], onDelete: Cascade)
  notifications Notification[]

  @@unique([tenantId, externalId])
  @@index([tenantId])
  @@index([email])
  @@map("contacts")
}
```

---

### 2.4 Template

Stores notification layouts mapped to event slugs. Supports **Handlebars / Liquid** syntax for dynamic variable injection via `{{variable}}` keys.

| Field | Type | Notes |
|---|---|---|
| `id` | `String` UUID PK | Unique template artifact identifier. |
| `tenantId` | `String` FK → Tenant | Workspace mapping link. |
| `name` | `String` | Human-readable template name. |
| `slug` | `String` | Event routing moniker, e.g. `welcome-alert`. |
| `channel` | `ChannelType` enum | One of `EMAIL`, `SMS`, `PUSH`, `WEBHOOK`, `IN_APP`. |
| `subject` | `String?` optional | Header line for email templates. |
| `body` | `String` | Content layout with variable keys, e.g. `{{name}}`. |
| `variables` | `Json` default `[]` | Schema declaration of expected variables for validation. |
| `isActive` | `Boolean` default `true` | Status activation flag. |
| `version` | `Int` default `1` | Monotonic counter for template change tracking. |
| `createdAt` | `DateTime` default `now()` | Template creation timestamp. |
| `updatedAt` | `DateTime` `@updatedAt` | Auto-updated on every write. |

> **Constraint:** `@@unique([tenantId, slug, channel])` — allows multiple tenants to use identical slugs without namespace collisions. `channel` is part of the uniqueness key, meaning you can have a `welcome-alert` template for both `EMAIL` and `IN_APP` under the same tenant.

```prisma
model Template {
  id        String      @id @default(uuid())
  tenantId  String
  name      String
  slug      String
  channel   ChannelType
  subject   String?
  body      String
  variables Json        @default("[]")
  isActive  Boolean     @default(true)
  version   Int         @default(1)
  createdAt DateTime    @default(now())
  updatedAt DateTime    @updatedAt

  tenant        Tenant         @relation(fields: [tenantId], references: [id], onDelete: Cascade)
  notifications Notification[]

  @@unique([tenantId, slug, channel])
  @@index([tenantId])
  @@map("templates")
}
```

---

### 2.5 ChannelConfig

Stores per-tenant provider credentials for each delivery channel. **Credentials must be encrypted at the application layer before persistence** — the ORM does not handle encryption.

| Field | Type | Notes |
|---|---|---|
| `id` | `String` UUID PK | Unique config identifier. |
| `tenantId` | `String` FK → Tenant | Workspace mapping link. |
| `channel` | `ChannelType` enum | The delivery channel this config applies to. |
| `provider` | `String` | Provider slug: `"sendgrid"`, `"smtp"`, `"twilio"`, `"fcm"`. |
| `credentials` | `Json` | Encrypted provider credentials — API keys, SMTP host/port, service accounts. |
| `isActive` | `Boolean` default `true` | Enable/disable this channel config. |
| `createdAt` | `DateTime` default `now()` | Config creation timestamp. |
| `updatedAt` | `DateTime` `@updatedAt` | Auto-updated on every write. |

> **Constraint:** `@@unique([tenantId, channel])` — one active provider config per channel per workspace.

```prisma
model ChannelConfig {
  id          String      @id @default(uuid())
  tenantId    String
  channel     ChannelType
  provider    String
  credentials Json
  isActive    Boolean     @default(true)
  createdAt   DateTime    @default(now())
  updatedAt   DateTime    @updatedAt

  tenant Tenant @relation(fields: [tenantId], references: [id], onDelete: Cascade)

  @@unique([tenantId, channel])
  @@index([tenantId])
  @@map("channel_configs")
}
```

---

### 2.6 Notification

Tracks every individual notification instance generated by trigger actions.

| Field | Type | Notes |
|---|---|---|
| `id` | `String` UUID PK | System tracing ticket identifier. |
| `tenantId` | `String` FK → Tenant | Workspace tracking link. |
| `contactId` | `String` FK → Contact | Recipient delivery link. **Required.** |
| `templateId` | `String` FK → Template | Source template link. **Required.** |
| `channel` | `ChannelType` enum | Delivery channel for this notification. |
| `status` | `NotificationStatus` default `PENDING` | Current workflow state. |
| `priority` | `Priority` default `NORMAL` | Dispatch priority: `LOW`, `NORMAL`, `HIGH`, `CRITICAL`. |
| `to` | `String` | Resolved runtime delivery address (email, phone number, etc.). |
| `subject` | `String?` optional | Compiled subject line. |
| `body` | `String` | Compiled dynamic content string. |
| `data` | `Json` default `{}` | Original request variable payload passed into the trigger. |
| `idempotencyKey` | `String?` `@unique` optional | Client-supplied deduplication key to prevent duplicate sends. |
| `isRead` | `Boolean` default `false` | In-app user read acknowledgment state. |
| `isSeen` | `Boolean` default `false` | Whether the notification appeared in the feed view without being clicked. |
| `readAt` | `DateTime?` optional | Timestamp when the notification was explicitly marked read. |
| `seenAt` | `DateTime?` optional | Timestamp when the notification was seen in the feed. |
| `scheduledAt` | `DateTime?` optional | Future dispatch time for scheduled notifications. |
| `sentAt` | `DateTime?` optional | Timestamp when dispatched to the provider. |
| `deliveredAt` | `DateTime?` optional | Timestamp of confirmed provider delivery. |
| `failedAt` | `DateTime?` optional | Timestamp of terminal failure. |
| `expiresAt` | `DateTime?` optional | TTL for in-app notifications; expired items are hidden from the feed. |
| `retryCount` | `Int` default `0` | Number of delivery attempts made so far. |
| `maxRetries` | `Int` default `3` | Maximum attempts before the notification is marked `FAILED`. |
| `errorMessage` | `String?` optional | Last error detail for diagnostics. |
| `createdAt` | `DateTime` default `now()` | Ingestion timestamp. |
| `updatedAt` | `DateTime` `@updatedAt` | State modification tracking time. |

> **High-performance composite index:**
> `@@index([tenantId, contactId, channel, status, isRead, createdAt])` — supports feed queries filtering by read state, channel, and time range without full table scans.

```prisma
model Notification {
  id             String             @id @default(uuid())
  tenantId       String
  contactId      String
  templateId     String
  channel        ChannelType
  status         NotificationStatus @default(PENDING)
  priority       Priority           @default(NORMAL)
  to             String
  subject        String?
  body           String
  data           Json               @default("{}")
  idempotencyKey String?            @unique
  isRead         Boolean            @default(false)
  isSeen         Boolean            @default(false)
  readAt         DateTime?
  seenAt         DateTime?
  scheduledAt    DateTime?
  sentAt         DateTime?
  deliveredAt    DateTime?
  failedAt       DateTime?
  expiresAt      DateTime?
  retryCount     Int                @default(0)
  maxRetries     Int                @default(3)
  errorMessage   String?
  createdAt      DateTime           @default(now())
  updatedAt      DateTime           @updatedAt

  tenant   Tenant              @relation(fields: [tenantId], references: [id])
  contact  Contact             @relation(fields: [contactId], references: [id])
  template Template            @relation(fields: [templateId], references: [id])
  events   NotificationEvent[]

  @@index([tenantId])
  @@index([status])
  @@index([scheduledAt])
  @@index([contactId, isRead])
  @@index([tenantId, status])
  @@index([tenantId, createdAt])
  @@index([tenantId, contactId, channel, status, isRead, createdAt])
  @@map("notifications")
}
```

---

### 2.7 NotificationEvent (Audit Log)

An **append-only immutable ledger** recording fine-grained lifecycle telemetry. Rows are never mutated — only new rows are inserted.

| Field | Type | Notes |
|---|---|---|
| `id` | `String` UUID PK | Unique audit line locator. |
| `notificationId` | `String` FK → Notification | Parent notification lifecycle link. |
| `event` | `EventType` enum | Concrete lifecycle milestone. See enum table below. |
| `metadata` | `Json` default `{}` | Diagnostic payload: error traces, vendor IDs, delivery receipts. |
| `occurredAt` | `DateTime` default `now()` | Event generation timestamp. **Field is `occurredAt`, not `createdAt`.** |

```prisma
model NotificationEvent {
  id             String    @id @default(uuid())
  notificationId String
  event          EventType
  metadata       Json      @default("{}")
  occurredAt     DateTime  @default(now())

  notification Notification @relation(fields: [notificationId], references: [id], onDelete: Cascade)

  @@index([notificationId])
  @@map("notification_events")
}
```

---

### 2.8 Enumerations

#### ChannelType

| Value | Description |
|---|---|
| `EMAIL` | Email delivery via SMTP or transactional provider (SendGrid, etc.). |
| `SMS` | SMS delivery via telephony provider (Twilio, etc.). |
| `PUSH` | Mobile push notification via FCM / APNS. |
| `WEBHOOK` | HTTP POST to a tenant-configured endpoint. |
| `IN_APP` | In-application notification feed entry. |

#### NotificationStatus

| Value | Description |
|---|---|
| `PENDING` | Notification created; awaiting processing. |
| `QUEUED` | Added to the async dispatch queue. |
| `PROCESSING` | Background worker is actively processing. |
| `SENT` | Dispatched to the provider successfully. |
| `DELIVERED` | Provider confirmed delivery to the recipient. |
| `FAILED` | Terminal failure after exhausting retries. |
| `CANCELLED` | Manually or programmatically cancelled before dispatch. |
| `SCHEDULED` | Awaiting a future `scheduledAt` dispatch time. |

#### Priority

| Value | Description |
|---|---|
| `LOW` | Non-urgent; may be batched or delayed. |
| `NORMAL` | Standard priority (default). |
| `HIGH` | Expedited processing; skips normal queue order. |
| `CRITICAL` | Immediate delivery; bypasses all queue throttling. |

#### EventType (Audit Log)

| Value | Description |
|---|---|
| `CREATED` | Notification row inserted; initial audit entry. |
| `QUEUED` | Added to the async dispatch queue. |
| `PROCESSING` | Background worker started processing. |
| `SENT` | Successfully dispatched to the provider. |
| `DELIVERED` | Provider confirmed delivery. |
| `OPENED` | Recipient opened the notification (email open tracking). |
| `CLICKED` | Recipient clicked a link inside the notification. |
| `BOUNCED` | Email bounced or SMS permanently failed. |
| `FAILED` | Delivery failed; error detail in `metadata`. |
| `RETRYING` | Retry attempt initiated after a transient failure. |
| `CANCELLED` | Notification cancelled before dispatch. |
| `READ` | In-app notification marked as read by the recipient. |
| `SEEN` | In-app notification scrolled into view without explicit read action. |

---

## 3. Core Functional Workflows

### 3.1 Inbound Ingestion Flow

**Endpoint:** `POST /v1/notifications/trigger`

The following steps execute **synchronously** before the HTTP response is returned:

1. **Tenant Identification** — Interceptor hashes the `x-api-key` header via SHA-256 and matches it against the `ApiKey` table to extract `tenantId`.
2. **Recipient Verification** — Queries the `Contact` table by `(tenantId, externalId)`. Inactive or missing contacts abort the operation immediately with a `404`.
3. **Template Extraction** — Queries the `Template` table by `(tenantId, slug, channel)`. All three fields are required due to the composite unique constraint.
4. **Dynamic Compilation** — Regex-replaces `{{variable}}` keys in the template body with runtime values from the request `data` payload.
5. **Persistence** — A `Notification` row is written with `status: PENDING`. A `NotificationEvent` of type `CREATED` is appended.
6. **Fast Response** — A `201 Created` response with the notification `id` is returned. Background processing is forked asynchronously.

### 3.2 Async Background Processing

After the HTTP response is sent, a background worker handles provider dispatch:

```
processor.processNotification(notification)
    │
    ├── prisma.notification.update({ status: PROCESSING })
    ├── prisma.notificationEvent.create({ event: PROCESSING_STARTED })
    │
    ├── simulate provider dispatch (setTimeout)
    │     └── real adapters (SendGrid/Twilio/FCM) plug in here
    │
    ├── prisma.notification.update({ status: SENT, sentAt: now() })
    ├── prisma.notificationEvent.create({ event: DELIVERED })
    │
    └── if channel === IN_APP:
            └── redis.publish('platform:notifications', JSON.stringify({
                    tenantId,
                    recipientId,
                    notification
                }))
```

**State resolution:**
- **Success** → status `SENT`, `sentAt` + `deliveredAt` recorded, audit events `SENT` + `DELIVERED` appended.
- **Transient failure** → `retryCount` incremented, `RETRYING` audit event appended, job re-queued.
- **Terminal failure** → after `maxRetries` exhaustion, status `FAILED`, `failedAt` set, `errorMessage` recorded.

---

## 4. Real-Time Streaming & Cache Synchronization

### 4.1 WebSocket Streaming (Socket.IO)

1. **Handshake** — Client browser connects and passes a short-lived signed JWT. Gateway verifies the signature and extracts `tenantId` + `userId`.
2. **Room Isolation** — Authorized clients join a sandboxed room: `${tenantId}:${recipientId}`.
3. **Live Push** — When the Redis subscriber catches a `platform:notifications` message, the gateway targets the exact room and streams the payload instantly.

### 4.2 Write-Through Dual-Cache Insertion (Redis Sorted Sets)

Two parallel sorted sets are maintained per user. The score is the **Unix epoch millisecond timestamp** of the notification.

| Redis Key | Contents |
|---|---|
| `feed:all:{tenantId}:{recipientId}` | All notifications — both read and unread. |
| `feed:unread:{tenantId}:{recipientId}` | Strictly unread notifications for badge counts. |

**Retention controls:**
- Only the **100 newest** items are kept per key — trimmed on every insert.
- All keys carry a **rolling 7-day TTL** to prevent unbounded memory growth.

### 4.3 Cache-Aside Read Strategy

**Endpoint:** `GET /v1/notifications/feed`

| Path | Behaviour |
|---|---|
| **Cache Hit** O(log N) | Reverse-ranked ZSET query returns results in under **2ms**. No database access. |
| **Cache Miss** | Falls through to PostgreSQL, returns sorted results, then asynchronously warms the cache. |

Key selection based on `unreadOnly` query param:
- `unreadOnly=true` → `feed:unread:{tenantId}:{recipientId}`
- `unreadOnly=false` → `feed:all:{tenantId}:{recipientId}`

---

## 5. State Invalidation & Mutation Flows

### 5.1 Single Notification Read

**Endpoint:** `PATCH /v1/notifications/:id/read`

| Layer | Action |
|---|---|
| **PostgreSQL** | Sets `isRead = true`, writes `readAt = now()`, appends `READ` audit event. |
| **Unread Cache** | Removes the entry from `feed:unread` via selective `ZREM`. |
| **All Cache** | Removes the stale `isRead: false` copy from `feed:all`, re-inserts updated copy with `isRead: true` at the **exact same score position**. |

### 5.2 Bulk Mark-All-Read

**Endpoint:** `POST /v1/notifications/read-all`

| Layer | Action |
|---|---|
| **PostgreSQL** | Single `UPDATE` sets `isRead = true` and `readAt = now()` for all unread notifications for the user. |
| **Cache — Nuke & Rebuild** O(1) | Both `feed:all` and `feed:unread` keys are deleted (`DEL`). Rebuilt fresh on next feed request via cache-miss path. |

> Nuking both keys avoids iterating over up to 100 individual cache entries, which would cause write amplification under high notification volume.

---

## 6. Redis Pub/Sub — Horizontal Scaling Backbone

### The problem

```
WITHOUT Pub/Sub — multi-instance (BROKEN):

User Browser ──WebSocket──→ Server A RAM
                            (socket lives here permanently)

Trigger HTTP ──────────────→ Load Balancer ──→ Server B
                                                    │
                                              in-memory emitter
                                              fires in Server B's RAM
                                              Server A never hears it ❌
                                              browser sees nothing
```

```
WITH Pub/Sub — multi-instance (WORKING):

Trigger HTTP ──────────────→ Load Balancer ──→ Server B
                                                    │
                                              redis.publish(payload)
                                                    │
                                                  Redis
                                                    │
                                     ┌──────────────┼──────────────┐
                                     │              │              │
                                  Server A       Server B       Server C
                                  receives       receives       receives
                                  (has socket)
                                     │
                                  pushes to browser ✅
```

### How it works

- **Publisher** (`notification.service.ts`) — calls `redis.publish('platform:notifications', payload)` after every IN_APP notification is processed. Any server instance can be the publisher.
- **Subscriber** (`notification.gateway.ts`) — every server instance subscribes to `platform:notifications` in `onModuleInit()`. The subscription is persistent for the life of the process.
- **Delivery** — whichever instance holds the user's WebSocket room delivers the event. Instances without the socket silently ignore it.

### Channel name

```typescript
REDIS_CHANNELS.PLATFORM_NOTIFICATIONS = 'platform:notifications'
```

One global channel for all tenants. The payload includes `tenantId` and `recipientId` so each instance can target the correct Socket.IO room without cross-tenant leakage.

### Pub/Sub payload shape

```json
{
  "tenantId": "tenant_abc123",
  "recipientId": "user_dev_99",
  "notification": { ...full notification object }
}
```

---

## 7. Redis Client Architecture

Three Redis connections exist per server instance. Each has a specific non-overlapping role.

```
redis.module.ts
│
├── REDIS_CLIENT      → general purpose
│     ├── Cache reads/writes  (ZADD, ZRANGE, ZREM, pipeline)
│     └── Publishing          (redis.publish) ← non-blocking, allowed here
│
└── REDIS_SUBSCRIBER  → subscribe mode only
      └── subscribe('platform:notifications')
          └── on('message', handler)
```

**Why two clients?**

Once a Redis connection calls `SUBSCRIBE`, Redis locks it into subscriber mode — it cannot run any other command. No `GET`, no `SET`, no `ZADD`. If you tried to share one connection for caching and subscribing, every cache operation would throw an error.

Publishing is **not** a blocking operation and does not require a dedicated connection — the regular client handles it alongside all cache operations.

**Injection tokens:**
- `REDIS_CLIENT` — injected into `NotificationService` and `NotificationGateway`
- `REDIS_SUBSCRIBER` — injected into `NotificationGateway` only

---

## 8. Cache Layer Design

### Key structure

All cache keys are generated through `CacheKeyFactory` in `notification.constants.ts`. Never hardcode key strings anywhere else in the codebase.

```typescript
CacheKeyFactory.getAllFeedKey(tenantId, recipientId)
// → 'feed:all:{tenantId}:{recipientId}'

CacheKeyFactory.getUnreadFeedKey(tenantId, recipientId)
// → 'feed:unread:{tenantId}:{recipientId}'
```

### Data structure — Redis Sorted Set (ZSET)

```
Key:    feed:all:{tenantId}:{recipientId}
Score:  Unix epoch milliseconds  ← notification.createdAt timestamp
Value:  JSON.stringify(notification)
```

Using timestamp as score means `ZREVRANGE` returns notifications newest-first automatically — no sorting at the application layer.

### Write path — Redis pipeline (atomic batch)

```
1. ZADD feed:all     score  serializedNotification
2. ZADD feed:unread  score  serializedNotification
3. ZREMRANGEBYRANK feed:all    0  -101   ← trim to 100 newest
4. ZREMRANGEBYRANK feed:unread 0  -101   ← trim to 100 newest
5. EXPIRE feed:all    604800             ← 7-day TTL
6. EXPIRE feed:unread 604800             ← 7-day TTL
```

All 6 commands execute in a single round-trip to Redis.

### Retention limits

| Setting | Value | Reason |
|---|---|---|
| Max entries per key | 100 | Prevents unbounded memory growth per user |
| TTL | 7 days (604800s) | Clears cold/inactive user caches automatically |
| TTL reset on | Every new notification write | Rolling window — active users never expire |

---

## 9. WebSocket Gateway Internals

### Connection lifecycle

```
Client connects to Socket.IO
    │
    └── handleConnection(client: Socket)
            ├── extract JWT from client.handshake.auth.token
            ├── verify signature using JWT_SECRET
            ├── extract { tenantId, userId }
            ├── client.join(`${tenantId}:${userId}`)
            └── log connection

Client disconnects
    └── handleDisconnect(client)
            └── Socket.IO auto-removes client from all rooms
```

### Room naming convention

```
`${tenantId}:${recipientId}`

Example: 'tenant_abc123:user_dev_99'
```

One room per user per tenant. Tenant A's events never reach Tenant B's rooms.

### Event names

```typescript
WS_EVENTS.NOTIFICATION_RECEIVED = 'notification_received'
```

Client-side listener:
```javascript
socket.on('notification_received', (notification) => { ... })
```

---

## 10. Security Architecture

### Two-Key Token Exchange

Direct browser-to-backend communication would expose private API keys. The platform prevents this with a two-step token exchange:

```
Tenant Backend                NaaS WebSocket Gateway         End-User Browser
──────────────                ──────────────────────         ────────────────
1. Store keyHash securely
2. User loads dashboard
3. Sign short-lived JWT:
   { tenantId, userId,
     expires: 3600 }
4. Send JWT to browser ──────────────────────────────────→  Browser holds JWT
                                                                    │
                              5. Browser connects ─────────────────→
                              6. Verify JWT signature
                              7. Join room [tenantId:userId]
                              8. Stream events ────────────────────→ Browser
```

**Security guarantee:** A stolen JWT grants read access only to that specific user's stream. Modifying `tenantId` or `userId` in the payload breaks the signature — the gateway rejects the connection immediately.

### Channel Credential Security

`ChannelConfig.credentials` stores provider secrets (SendGrid API keys, Twilio auth tokens, FCM service accounts). The Prisma schema marks this as `Json` — **encryption and decryption are the responsibility of the service layer**, not the ORM.

Recommended approach: AES-256-GCM before `prisma.channelConfig.create()`, decrypt after fetching before passing to the provider adapter.

---

## 11. Constants & Key Management

Everything that could be a magic string lives in `src/notification/constants/notification.constants.ts`:

```typescript
// Redis Pub/Sub channel
REDIS_CHANNELS.PLATFORM_NOTIFICATIONS = 'platform:notifications'

// WebSocket event name emitted to clients
WS_EVENTS.NOTIFICATION_RECEIVED = 'notification_received'

// Cache key generators
CacheKeyFactory.getAllFeedKey(tenantId, recipientId)    // → feed:all:{t}:{r}
CacheKeyFactory.getUnreadFeedKey(tenantId, recipientId) // → feed:unread:{t}:{r}
```

**Rule:** Never hardcode a Redis key string, channel name, or WebSocket event name outside this file. If you need a new constant, add it here first, then import it.

---

## 12. Known Issues & Bugs

### Bug — `cacheKeyUnread` was identical to `cacheKeyAll`

**File:** `notification.gateway.ts` — inside the `onModuleInit` Redis message handler

**What happened:**
```typescript
// WRONG — both lines call getAllFeedKey
const cacheKeyAll    = CacheKeyFactory.getAllFeedKey(tenantId, recipientId);
const cacheKeyUnread = CacheKeyFactory.getAllFeedKey(tenantId, recipientId); // ← bug
```

**Effect:** Both `ZADD` operations wrote to `feed:all`. The `feed:unread` key was never written. Unread badge counts always returned empty.

**Fix:**
```typescript
const cacheKeyAll    = CacheKeyFactory.getAllFeedKey(tenantId, recipientId);
const cacheKeyUnread = CacheKeyFactory.getUnreadFeedKey(tenantId, recipientId); // ← correct
```

**Status:** Fix pending — apply before testing the feed endpoint.

---

### Previous — in-memory event emitter used for WebSocket broadcast

**What happened:** `eventEmitter.emit(NOTIFICATION_CREATED_EVENT, payload)` only works within a single process. Under a load balancer it fires on the wrong server instance.

**Fix:** Replaced with `redis.publish('platform:notifications', payload)`. Old code preserved as comments in `notification.gateway.ts` and `notification.service.ts` for reference.

**Status:** Fixed.

---

## 13. How to Extend This Documentation

**Every time you implement a new feature, update this file before closing the PR.** Use the checklist below — not every item applies to every change, but check all of them.

---

### New API Endpoint

Add a subsection under §3 (or a new section) using this structure:

```
### VERB /v1/path

**Purpose:** One sentence.
**Auth:** x-api-key header / signed JWT / public

**Request**
| Field | Type | Required | Description |
|---|---|---|---|

**Response** 201 / 200
| Field | Type | Description |
|---|---|---|

**Errors**
- 400 — reason
- 404 — reason

**Cache behaviour:** which Redis keys are written, read, or invalidated.
**Events emitted:** e.g. redis.publish on platform:notifications.
```

---

### New Database Model

Add a subsection under §2 following this structure:

```
### 2.X ModelName

One paragraph: what this model tracks and why it exists.

| Field | Type | Notes |
|---|---|---|

> Constraint: explain any unique/composite index.

(paste Prisma model block)
```

Update §2.8 Enumerations if new enums were added. Add the relation field to the relevant parent model.

---

### New Field on Existing Model

1. Add a row to the model's field table in §2.
2. If it introduces a new index, note it in the Constraints callout.
3. Update the Prisma model code block.
4. If the field changes a workflow, update §3 or §5.

---

### New Enum Value

1. Add a row to the relevant enum table in §2.8.
2. If it introduces a new state transition, update the workflow in §3.

---

### New Channel

- [ ] Add value to `ChannelType` enum in §2.8.
- [ ] Add provider slug to `ChannelConfig.provider` notes in §2.5.
- [ ] Document the expected `credentials` shape for that provider.
- [ ] Note any channel-specific constraints (character limits, threading, etc.).

---

### New Background Job / Worker

Add a subsection under §3:

```
### 3.X Job Name

**Triggered by:** cron / event / manual API call
**Steps:**
1. ...
2. ...
**State transitions:** which statuses change
**Audit events written:** which EventType values are appended
**Failure handling:** retry logic, dead-letter, alerting
```

---

### Cache Behaviour Change

1. Update the relevant table in §4.2 or §8.
2. Document new key format: `feed:<scope>:{tenantId}:{recipientId}`.
3. Note TTL and retention cap changes.
4. If invalidation strategy changes for read/read-all, update §5.

---

### Security Change

1. Update the flow diagram in §10.
2. Describe what a stolen/replayed token can access after the change.
3. If new credentials are introduced, document encryption strategy in §10.

---

### PR Checklist

Copy into every PR description:

```
- [ ] New/changed API endpoints documented in §3
- [ ] New/changed DB fields added to correct model table in §2
- [ ] New/changed enum values added to §2.8
- [ ] Prisma model code blocks updated to match schema.prisma
- [ ] New workflows or state transitions documented in §3 or §5
- [ ] Cache behaviour documented or updated in §4 / §8
- [ ] Security implications noted in §10 if relevant
- [ ] Constants added to notification.constants.ts and documented in §11
- [ ] Known bugs or issues added to §12
- [ ] Implementation log entry added to §14
```

---

## 14. Implementation Log

> Add an entry every time a significant feature or architectural decision is shipped.

---

### Core Notification Pipeline

**What was built:**
- `POST /v1/notifications/trigger` — sync ingestion, async processing fork
- `GET /v1/notifications/feed` — Redis cache-aside read with PostgreSQL fallback
- `PATCH /v1/notifications/:id/read` — surgical single-notification cache invalidation
- `POST /v1/notifications/read-all` — bulk read with nuke-and-rebuild cache strategy
- Dual Redis Sorted Set cache: `feed:all` and `feed:unread` per user
- WebSocket gateway with JWT auth and isolated tenant rooms

**Key decisions:**
- HTTP response returns before processing finishes — keeps API latency low regardless of provider speed
- Dual cache sets instead of one filtered set — unread badge counts become O(1) rather than O(N)
- Nuke-and-rebuild for bulk operations — avoids write amplification at the cost of one cache miss

---

### Redis Pub/Sub — Horizontal Scaling Backbone

**What was built:**
- Dedicated `REDIS_SUBSCRIBER` client in `redis.module.ts`
- `NotificationGateway.onModuleInit()` subscribes to `platform:notifications` on startup
- `NotificationService` publishes to `platform:notifications` after every IN_APP notification is processed
- Write-through cache update moved into the Pub/Sub message handler so all instances stay cache-consistent
- `CacheKeyFactory` introduced to centralize all Redis key generation
- `REDIS_CHANNELS` and `WS_EVENTS` constants introduced to eliminate magic strings

**Why:**
In-memory event emitters don't cross server process boundaries. Under a load balancer, the server that processes a notification and the server holding the user's WebSocket connection are often different instances. Redis Pub/Sub is a shared external broadcast layer — all instances receive every event and the one holding the socket delivers it.

**Files changed:**
- `src/notification/constants/notification.constants.ts` — new file
- `src/notification/notification.gateway.ts` — added `onModuleInit`, Redis subscriber, cache write logic
- `src/notification/notification.service.ts` — replaced `eventEmitter.emit` with `redis.publish`, fixed cache key bug, renamed `sendInMapNotification` → `sendInAppNotification`
- `src/redis/redis.module.ts` — added `REDIS_SUBSCRIBER` provider

**Known limitation:**
Write-through cache currently executes on every server instance that receives the Pub/Sub message — all three will run the pipeline. This is harmless since `ZADD` is idempotent for the same score and value, but it's wasteful. A future optimization would use a distributed lock or a Redis-level deduplication key to ensure only one instance writes the cache per event.

---

*Last updated: update this line whenever you add an entry above.*