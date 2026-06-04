NaaS — Notification-as-a-Service
A multi-tenant notification platform that lets any client application send email, SMS, push, and in-app notifications through a single API. Built to understand how real notification infrastructure works at scale — async processing, real-time delivery, and high-performance caching.

This is a solo project. Built from scratch, no boilerplate.


What it does

One API call triggers a notification to any channel
Templates are pre-defined per tenant with dynamic variable support ({{name}}, etc.)
In-app notifications stream to the browser in real-time over WebSockets
A Redis-backed feed returns notification history in under 2ms
Every notification has a full audit trail of lifecycle events


Tech Stack
LayerTechnologyRuntimeNode.jsFrameworkExpressDatabasePostgreSQL via Prisma ORMCacheRedis (Sorted Sets)Real-timeSocket.IOAuthSHA-256 API key hashing + signed JWT for WebSocket

Getting Started
Prerequisites

Node.js 18+
PostgreSQL
Redis

Setup
bashgit clone https://github.com/your-username/naas
cd naas
npm install
Create a .env file:
envDATABASE_URL=postgresql://user:password@localhost:5432/naas
REDIS_URL=redis://localhost:6379
JWT_SECRET=your_secret_here
PORT=3001
WS_PORT=3002
Run database migrations:
bashnpx prisma migrate dev
Start the server:
bashnpm run dev

Project Structure
src/
├── modules/
│   └── notifications/      # trigger, feed, mark-read logic
├── middleware/
│   └── auth.ts             # API key validation
├── services/
│   ├── cache.service.ts    # Redis sorted set operations
│   ├── compiler.service.ts # Handlebars template compilation
│   └── socket.service.ts   # WebSocket gateway
├── events/
│   └── emitter.ts          # Internal in-process event bus
└── prisma/
    └── schema.prisma

How it Works
The core flow
POST /v1/notifications/trigger
        │
        ├── 1. Hash API key → resolve tenant
        ├── 2. Look up contact by externalId
        ├── 3. Fetch template by slug + channel
        ├── 4. Compile {{variables}} into body
        ├── 5. Write Notification row (status: PENDING)
        └── 6. Return 201 immediately ← fast response to caller
                │
                └── background worker picks up async
                        ├── status → PROCESSING
                        ├── dispatch to provider
                        ├── status → SENT
                        └── emit notification.created
                                │
                                ├── Redis cache updated
                                └── WebSocket pushes to browser
Why async?
The HTTP response returns before the notification is actually sent. This keeps API response times under 50ms regardless of whether the downstream provider (SendGrid, Twilio, etc.) is slow. The client gets a notification id immediately and can track status via the audit log.
Why dual Redis cache?
Every in-app notification is written into two Redis Sorted Sets simultaneously, scored by timestamp:

feed:all:{tenantId}:{userId} — every notification, read and unread
feed:unread:{tenantId}:{userId} — only unread, for badge counts

This means a feed request never touches PostgreSQL on a cache hit — it's a single sorted set range query, returning results in O(log N). The cache holds the 100 most recent notifications per user with a 7-day TTL.
WebSocket auth
Exposing an API key to the browser would be a security hole. Instead:

The tenant's own backend signs a short-lived JWT { tenantId, userId, expires }
The browser passes this token to the WebSocket gateway
The gateway verifies the signature and places the client into an isolated room: tenantId:userId

A stolen token only exposes that user's own feed. Tampering with the payload breaks the signature.

API Reference
Base URL: http://localhost:3001
All requests require:
x-api-key: your_raw_api_key

Notifications
Trigger a notification
POST /v1/notifications/trigger
Compiles a template, creates a notification record, and dispatches it asynchronously.
Request body
json{
  "workflow": "welcome-alert",
  "recipientId": "user_dev_99",
  "data": {
    "name": "Kunal"
  }
}
FieldTypeRequiredDescriptionworkflowstringYesThe template slug to use.recipientIdstringYesYour internal user ID (maps to a Contact).dataobjectNoVariables to inject into the template body.
Response 201 Created
json{
  "id": "notif_abc123",
  "status": "PENDING"
}
Errors
CodeReason401Invalid or missing API key.404No contact found for this recipientId.404No active template found for this workflow.

Get notification feed
GET /v1/notifications/feed
Returns a user's notification history. Hits Redis cache first — falls back to PostgreSQL on miss.
Query params
ParamTypeRequiredDescriptionrecipientIdstringYesYour internal user ID.unreadOnlybooleanNotrue returns only unread notifications. Default false.
Response 200 OK
json[
  {
    "id": "notif_abc123",
    "body": "Hi Kunal, thanks for signing up.",
    "channel": "IN_APP",
    "isRead": false,
    "createdAt": "2024-01-01T00:00:00Z"
  }
]

Mark a notification as read
PATCH /v1/notifications/:id/read
Marks a single notification as read. Updates PostgreSQL, surgically updates both Redis cache sets.
Response 200 OK
json{
  "id": "notif_abc123",
  "isRead": true,
  "readAt": "2024-01-01T00:01:00Z"
}
Errors
CodeReason404Notification not found or doesn't belong to this tenant.

Mark all notifications as read
POST /v1/notifications/read-all
Bulk marks all unread notifications as read for a user. Drops both cache keys entirely — rebuilt fresh on next feed request.
Request body
json{
  "recipientId": "user_dev_99"
}
Response 200 OK
json{
  "updated": 12
}

Real-Time (WebSocket)
Connect via Socket.IO on port 3002.
javascriptconst socket = io('http://localhost:3002', {
  auth: { token: 'your_signed_jwt' }
});

socket.on('notification.new', (notification) => {
  console.log('New notification:', notification);
});
The server places you into a private room on connect. You only receive events for your own user — no other tenant or user's data leaks through.

Database Schema
Full schema lives in prisma/schema.prisma. Key models:
ModelPurposeTenantRoot workspace. Every other model belongs to a tenant.ApiKeyHashed credentials for server-to-server auth.ContactEnd-users mapped to a tenant by externalId.TemplateNotification layouts with Handlebars variable support.ChannelConfigPer-tenant provider credentials (SendGrid, Twilio, FCM).NotificationEvery notification instance with full status tracking.NotificationEventAppend-only audit log of every lifecycle event.

## What's Next
The current implementation is architected for correctness and 
learning. These are the gaps to close before production use:

**Job Queue (BullMQ)**  
Background processing is currently a forked async function. 
Replacing it with BullMQ adds retries, concurrency control, 
rate limiting per tenant, and a dead-letter queue for failed jobs.

**Real Provider Adapters**  
Dispatch is simulated with a timeout. The next step is plugging 
in real adapters — SendGrid for email, Twilio for SMS, FCM for push. 
The channel + ChannelConfig model is already designed for this.

**Per-Tenant Rate Limiting**  
No rate limiting exists on API endpoints today. 
Production would need per-tenant request limits, 
likely via Redis counters against the tenant's API key.

What I learned building this

How to structure a multi-tenant system where every query is scoped to a tenant without leaking data across workspaces
Why you separate read and unread caches instead of filtering a single one — the unread set makes badge counts O(1)
How WebSocket room isolation works and why JWTs are the right auth mechanism for real-time connections (vs passing API keys to the browser)
The nuke-and-rebuild cache invalidation pattern for bulk operations — cheaper than updating dozens of individual cache entries

