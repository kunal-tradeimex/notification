# Notification-as-a-Service (NaaS) — Enterprise Platform

> **Multi-Tenant · Async Event-Driven · Real-Time WebSocket · Redis-Cached**
>
> This document is the single source of truth for architecture, database schema, workflows, security, and testing. Update it every time a new feature is implemented — see [§8 How to Extend This Documentation](#8-how-to-extend-this-documentation).

---

## Table of Contents

1. [Global Architecture & Data Flow](#1-global-architecture--data-flow)
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
6. [Security Architecture](#6-security-architecture)
7. [Postman Integration Test Guide](#7-postman-integration-test-guide)
8. [How to Extend This Documentation](#8-how-to-extend-this-documentation)

---

## 1. Global Architecture & Data Flow

The platform is built on an **Asynchronous Event-Driven Multi-Tenant Architecture**. It decouples synchronous API ingestion from provider distribution and uses real-time persistent streaming to avoid traditional pull-based overhead.

### System Layers

| Layer | Description |
|---|---|
| **API Ingest** | Receives multi-tenant notifications via secure HTTP REST. Validates API keys, compiles templates, maps contact profiles, and records tracking tokens. |
| **Async Processing** | Forks tasks away from the HTTP loop. Updates operational state and dispatches to channel providers without blocking the client response. |
| **Internal Event Loop** | Decoupled in-memory emitter that broadcasts transaction updates across application layers via the `notification.created` channel. |
| **WebSocket Gateway** | Manages full-duplex TCP connections via Socket.IO. Validates tenant credentials on handshake, isolates users into sandboxed rooms, and pushes live updates. |
| **Dual-Cache Engine** | Maintains Redis Sorted Sets for `feed:all` and `feed:unread` timelines. Resolves index queries in O(log N) without touching PostgreSQL. |

### End-to-End Data Flow

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
                    │  (Emit notification.created)
                    ▼
┌────────────────────────────────────────┐
│    Core In-Process Event Emitter       │
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

> **Constraint:** `@@unique([tenantId, slug, channel])` — allows multiple tenants to use identical slugs without namespace collisions. Note: `channel` is part of the uniqueness key, meaning you can have a `welcome-alert` template for both `EMAIL` and `IN_APP` under the same tenant.

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
| `provider` | `String` | Provider slug, e.g. `"sendgrid"`, `"smtp"`, `"twilio"`, `"fcm"`. |
| `credentials` | `Json` | Encrypted provider credentials — API keys, SMTP host/port, service accounts, etc. |
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
| `occurredAt` | `DateTime` default `now()` | Event generation timestamp. **Note: field is `occurredAt`, not `createdAt`.** |

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

1. **Tenant Identification** — The interceptor hashes the `x-api-key` header via SHA-256 and matches it against the `ApiKey` table to extract `tenantId`.
2. **Recipient Verification** — Queries the `Contact` table by `(tenantId, externalId)`. Inactive or missing contacts abort the operation immediately with a `404`.
3. **Template Extraction** — Queries the `Template` table by `(tenantId, slug, channel)`. All three fields are required due to the composite unique constraint.
4. **Dynamic Compilation** — Regex-replaces `{{variable}}` keys in the template body with runtime values from the request `data` payload.
5. **Persistence** — A `Notification` row is written with `status: PENDING`. A `NotificationEvent` of type `CREATED` is appended.
6. **Fast Response** — A `201 Created` response with the notification `id` is returned to the client. Background processing is forked asynchronously.

### 3.2 Async Background Processing

After the HTTP response is sent, a background worker handles provider dispatch:

1. **State → `PROCESSING`** — Worker updates the `Notification` status and appends a `PROCESSING` audit event.
2. **Channel Dispatch** — The channel-specific adapter sends the notification to the configured provider resolved from `ChannelConfig`.
3. **State Resolution:**
   - **Success** → status becomes `SENT`, timestamps `sentAt` and `deliveredAt` are recorded, audit events `SENT` + `DELIVERED` are appended.
   - **Transient failure** → `retryCount` is incremented, a `RETRYING` audit event is appended, and the job is re-queued.
   - **Terminal failure** → after `maxRetries` exhaustion, status becomes `FAILED`, `failedAt` is set, `errorMessage` is recorded.

---

## 4. Real-Time Streaming & Cache Synchronization

When an `IN_APP` notification is finalized, the `notification.created` event is broadcast on the internal event emitter. Two subsystems listen to this event simultaneously.

### 4.1 WebSocket Streaming (Socket.IO — port `3002`)

1. **Handshake** — Client browser connects and passes a short-lived signed JWT. The gateway verifies the signature and extracts `tenantId` + `userId`.
2. **Room Isolation** — Authorized clients join a sandboxed room: `${tenantId}:${recipientId}`.
3. **Live Push** — On `notification.created`, the gateway targets the exact room and streams the payload over the open TCP channel instantly.

### 4.2 Write-Through Dual-Cache Insertion (Redis Sorted Sets)

Two parallel sorted sets are maintained per user. The score is the **Unix epoch millisecond timestamp** of the notification.

| Redis Key | Contents |
|---|---|
| `feed:all:{tenantId}:{recipientId}` | All notifications — both read and unread. |
| `feed:unread:{tenantId}:{recipientId}` | Strictly unread notifications for badge counts. |

**Retention controls:**
- Only the **100 newest** items are kept per key. Older entries are trimmed on every insert.
- All keys carry a **rolling 7-day TTL** to prevent unbounded memory growth.

### 4.3 Cache-Aside Read Strategy

**Endpoint:** `GET /v1/notifications/feed`

| Path | Behaviour |
|---|---|
| **Cache Hit** O(log N) | Reverse-ranked ZSET query returns results in under **2ms**. No database access. |
| **Cache Miss** | Falls through to PostgreSQL, returns sorted results, then asynchronously warms the cache for subsequent requests. |

The key is selected based on the `unreadOnly` boolean query parameter:
- `unreadOnly=true` → reads from `feed:unread:*`
- `unreadOnly=false` → reads from `feed:all:*`

---

## 5. State Invalidation & Mutation Flows

### 5.1 Single Notification Read

**Endpoint:** `PATCH /v1/notifications/:id/read`

| Layer | Action |
|---|---|
| **PostgreSQL** | Sets `isRead = true`, writes `readAt = now()`, appends `READ` audit event. |
| **Unread Cache** | Removes the entry from `feed:unread` via selective `ZREM`. |
| **All Cache** | Removes the stale `isRead: false` copy from `feed:all`, then re-inserts an updated copy with `isRead: true` at the **exact same score position**. |

### 5.2 Bulk Mark-All-Read

**Endpoint:** `POST /v1/notifications/read-all`

| Layer | Action |
|---|---|
| **PostgreSQL** | Single `UPDATE` sets `isRead = true` and `readAt = now()` for all unread notifications for the user. |
| **Cache — Nuke & Rebuild** O(1) | Both `feed:all` and `feed:unread` keys are deleted (`DEL`). On the next feed request, a fresh cache is built from the database automatically via the cache-miss path. |

> This strategy avoids iterating over potentially dozens of individual cache entries, which would cause write amplification under high notification volume.

---

## 6. Security Architecture

### 6.1 Two-Key Token Exchange

Direct browser-to-backend communication would expose private API keys. The platform prevents this with a two-step token exchange:

```
Tenant Backend                NaaS WebSocket Gateway         End-User Browser
──────────────                ──────────────────────         ────────────────
1. Store keyHash securely
2. User loads dashboard ──→  (never touches NaaS directly)
3. Sign short-lived JWT:
   { tenantId, userId,
     expires: 3600 }
4. Send JWT to browser ──────────────────────────────────→  Browser holds JWT
                                                             ↓
                              5. Browser connects ─────────→ Gateway receives JWT
                              6. Verify signature
                              7. Join room [tenantId:userId]
                              8. Stream events ────────────→ Browser receives feed
```

**Security guarantee:** A stolen JWT grants read access only to that specific user's stream. Modifying `tenantId` or `userId` in the payload breaks the cryptographic signature — the gateway rejects the connection immediately.

### 6.2 Channel Credential Security

`ChannelConfig.credentials` stores provider secrets (SendGrid API keys, Twilio auth tokens, FCM service accounts). The Prisma schema marks this field as `Json` — **encryption and decryption are the responsibility of the service layer**, not the ORM.

Recommended approach: encrypt with AES-256-GCM before calling `prisma.channelConfig.create()`, and decrypt after fetching before passing to the provider adapter.

---

## 7. Postman Integration Test Guide

Replace `<key>` with your raw API key in all tabs before sending.

| Tab | Mode | URL | Headers | Body / Params |
|---|---|---|---|---|
| **1 — WS Listener** | Socket.IO | `http://localhost:3002` | `x-api-key: <key>` | Params: `recipientId = user_dev_99` |
| **2 — Trigger** | `POST` | `http://localhost:3001/v1/notifications/trigger` | `x-api-key: <key>` | See body below |
| **3 — Feed Fetch** | `GET` | `http://localhost:3001/v1/notifications/feed` | `x-api-key: <key>` | Params: `recipientId = user_dev_99`, `unreadOnly = true` |
| **4 — Mark Read** | `PATCH` | `http://localhost:3001/v1/notifications/:id/read` | `x-api-key: <key>` | ID passed in URL path |
| **5 — Bulk Read** | `POST` | `http://localhost:3001/v1/notifications/read-all` | `x-api-key: <key>` | See body below |

**Tab 2 — Trigger body:**
```json
{
  "workflow": "welcome-alert",
  "recipientId": "user_dev_99",
  "data": {
    "name": "Kunal"
  }
}
```

**Tab 5 — Bulk Read body:**
```json
{
  "recipientId": "user_dev_99"
}
```

### Recommended Test Sequence

1. **Connect WS (Tab 1)** — Open the Socket.IO listener first to observe real-time events arriving live.
2. **Trigger (Tab 2)** — Fire the notification. Observe the `notification.new` event appear on Tab 1 within milliseconds.
3. **Feed Fetch (Tab 3)** — Retrieve the feed with `unreadOnly = true`. Confirm the new notification is present.
4. **Mark Read (Tab 4)** — Copy the `id` from the Tab 3 response and mark it read. Confirm the cache is updated on next feed fetch.
5. **Bulk Read (Tab 5)** — Send read-all. Confirm a fresh feed fetch (Tab 3) returns zero unread items.

---

## 8. How to Extend This Documentation

**Every time you implement a new feature, update this README before merging the PR.** Use the checklist below as a guide — not every item applies to every feature, but go through all of them.

---

### 8.1 New API Endpoint

Add a row to the relevant section (or create a new section) with the following structure:

```
### VERB /v1/path/to/endpoint

**Purpose:** One sentence describing what this endpoint does.

**Authentication:** `x-api-key` header / signed JWT / public

**Request**

| Field | Type | Required | Description |
|---|---|---|---|
| `field` | `string` | Yes | What it is. |

**Response** `201 Created` / `200 OK`

| Field | Type | Description |
|---|---|---|
| `id` | `string` UUID | The created resource ID. |

**Error cases:**
- `400` — Missing required field X.
- `404` — Contact not found for the given `recipientId`.
- `409` — Duplicate `idempotencyKey`.

**Cache behaviour:** (if applicable) Describe which Redis keys are written, read, or invalidated.

**Events emitted:** (if applicable) e.g. `notification.created` on the internal emitter.
```

---

### 8.2 New Database Model

Add a subsection under [§2 Database Schema](#2-database-schema) following this exact structure:

```
### 2.X ModelName

One paragraph explaining what this model tracks and why it exists.

| Field | Type | Notes |
|---|---|---|
| `id` | `String` UUID PK | ... |
| ... | ... | ... |

> **Constraint:** (if any unique/composite index) explain what it enforces.

(paste the Prisma model block here)
```

Then update the `Enumerations` section if you added new enums, and add the new model's relation to the relevant existing model (e.g. add the relation field to Tenant).

---

### 8.3 New Field on an Existing Model

1. Find the model's field table in [§2](#2-database-schema).
2. Add a new row for the field with its type and a clear description.
3. If the field changes query behaviour (e.g. a new index), note it in the "Constraints" callout below the table.
4. Update the Prisma model code block to include the new field.
5. If the field affects a workflow (e.g. a new status flag), update [§3](#3-core-functional-workflows) or [§5](#5-state-invalidation--mutation-flows) accordingly.

---

### 8.4 New Enum Value

1. Find the enum table in [§2.8 Enumerations](#28-enumerations).
2. Add a row with the value name and a precise description of when it is set.
3. If the new value introduces a new state transition, add or update the relevant workflow diagram/list in [§3](#3-core-functional-workflows).

---

### 8.5 New Channel

When adding support for a new delivery channel (e.g. `SLACK`, `DISCORD`, `WHATSAPP`):

- [ ] Add the new value to the `ChannelType` enum table in [§2.8](#28-enumerations).
- [ ] Add the new provider string to the `ChannelConfig.provider` field notes in [§2.5](#25-channelconfig).
- [ ] Document the expected shape of `ChannelConfig.credentials` for the new provider.
- [ ] Add a Postman test tab for the new channel in [§7](#7-postman-integration-test-guide).
- [ ] Note any channel-specific behaviour (e.g. character limits for SMS, threading for Slack).

---

### 8.6 New Background Job / Worker

Add a subsection under [§3](#3-core-functional-workflows) with this structure:

```
### 3.X Job Name (trigger condition)

**Triggered by:** (e.g. cron every 5 min, event `X.created`, manual API call)

**Steps:**
1. What the worker does first.
2. What it does next.
3. How it resolves (success + failure paths).

**State transitions:** (which Notification/model statuses change)

**Audit events written:** (which EventType values are appended)

**Failure handling:** (retry logic, dead-letter behaviour, alerting)
```

---

### 8.7 Cache Behaviour Change

If you change what is written to, read from, or invalidated in Redis:

1. Update the relevant table in [§4.2](#42-write-through-dual-cache-insertion-redis-sorted-sets).
2. Document the new key format using the pattern `feed:<scope>:{tenantId}:{recipientId}`.
3. Note TTL and retention cap changes if applicable.
4. If the invalidation strategy changes for read/mark-all, update [§5](#5-state-invalidation--mutation-flows).

---

### 8.8 Security Change

If you change authentication, token exchange, or credential handling:

1. Update the flow diagram in [§6.1](#61-two-key-token-exchange).
2. Describe what a stolen/replayed token can and cannot access after the change.
3. If a new credential type is introduced, document its encryption strategy in [§6.2](#62-channel-credential-security).

---

### 8.9 Documentation Update Checklist (for every PR)

Copy this into your PR description and check off each item:

```
- [ ] New/changed API endpoints documented in §3 or a new section
- [ ] New/changed database fields added to the correct model table in §2
- [ ] New/changed enum values added to §2.8
- [ ] Prisma model code blocks updated to match schema.prisma
- [ ] New workflows or state transitions documented in §3 or §5
- [ ] Cache behaviour documented or updated in §4
- [ ] Security implications noted in §6 if relevant
- [ ] New Postman test tab added to §7 if applicable
- [ ] README version / date updated at the top of the file
```

---

*Last updated: refer to git log for the authoritative change history.*