# SYSTEM CONTEXT MANIFESTO: NOTIFICATION-AS-A-SERVICE (NaaS)

### 1. Project Core Architectural Overview
* **Goal:** Building a multi-tenant Notification-as-a-Service (NaaS) infrastructure platform (similar to functional architectures like Knock or Novu).
* **Tech Stack:** NestJS (Backend Framework), Prisma (ORM), PostgreSQL (Database System), Handlebars (Template Compilation Engine).
* **Core Philosophy:** Low-latency runtime execution, strict multi-tenant isolation, data immutability for audits, and programmatic design following SOLID principles.

---

### 2. Database Schema Configuration
The PostgreSQL ledger is structured into three decoupled structural segments to ensure data integrity and complete isolation across discrete organizations:

1. IDENTITY LAYER
   * Tenant: High-level system container. Every single row of data across all tables belongs to a distinct Tenant workspace.
   * ApiKey: Security gatekeeper. Stores unique developer credentials as one-way SHA-256 cryptographic hashes. Resolves incoming requests to their respective tenant workspaces.

2. CONFIGURATION LAYER
   * Contact: The target recipient profiles. Maps the customer's proprietary database IDs to an internal schema via the "externalId" property. Holds delivery endpoints (email, phone).
   * Template: Structural blueprints for messages. Contains channel designations, raw subject lines, and raw body layouts featuring semantic placeholders (e.g., {{name}}).
   * ChannelConfig: Multi-tenant credentials hub. Manages vendor routing connection keys (e.g., SendGrid tokens, Twilio credentials) per tenant.

3. EXECUTION LAYER
   * Notification: Immutable delivery audit records tracking granular infrastructure lifecycles (PENDING, PROCESSING, SENT, FAILED), along with compiled text bodies, recipient snapshots, and read tracking states.
   * NotificationEvent: Historic transaction timeline logs tracking milestones sequentially (CREATED, SENT, FAILED).

---

### 3. Current Implementation State

We have built, verified, and secured the core messaging engine flow. The codebase is cleanly separated into decoupled modules:

#### A. Input Request Protection (src/main.ts)
A global NestJS ValidationPipe intercepts inbound traffic. Configured with strict validation constraints (whitelist: true, forbidNonWhitelisted: true), it blocks unauthorized request parameters at the framework edge and throws standardized 400 Bad Request exceptions before processing runtime queries.

#### B. Data Transfer Object Validation (src/notification/interfaces/)
* TriggerNotificationPayload: A class-validator enforced schema blueprint validating incoming JSON request payloads:
  - workflow: Required, non-empty string representing a template slug.
  - recipientId: Required, non-empty string mapping to a contact's external identity.
  - data: Required, key-value record object providing contextual template variables.

#### C. Service Abstraction Layer
* Implements the Dependency Inversion Principle using a explicit INotificationService interface contract blueprint to decouple implementation dependencies from execution logic.

#### D. Primary Processing Core (src/notification/notification.service.ts)
* Listens on the POST /v1/notifications/trigger API endpoint.
* Extracted plain-text "x-api-key" headers are immediately converted into SHA-256 hashes inside a crypto hashing pipeline to securely look up and verify the current active tenant context.
* Validates cross-relational data points, ensuring requested workflows and contact recipient targets exist and explicitly belong to the authenticated tenant.
* Offloads string interpolation logic to a standalone TemplateCompilerService utility running Handlebars execution completely independently of database actions.
* Writes an initial trackable payload entry to the Notification table as PENDING, creates a CREATED entry in the NotificationEvent history table, and fires off a non-blocking background task.

#### E. Asynchronous Processing & Mock Worker System (src/notification/notification.processor.ts)
The architecture uses a decoupled Request-Worker Pattern to prevent external vendor API latencies (like network delays from SendGrid or Twilio) from stalling primary client API trigger runtimes:
* NotificationProcessor: Intercepts notification tasks completely out-of-band, updating states cleanly from PENDING -> PROCESSING -> SENT or FAILED.
* ProviderFactory: Handles the Open/Closed Principle. Dynamically reads the database context and constructs target transmission handlers.
* MockEmailProvider & MockSmsProvider: Simulates text/email message delivery during local development by writing clean log blocks out to the active running terminal console window, keeping tests 100% free from real-world usage fees.

---

### 4. Codebase Version Control State
The repository contains five sequential atomic commit milestones:
1. "feat(db): implement multi-tenant notification schema and seed script"
2. "feat(notification): implement multi-tenant trigger endpoint with SOLID compiler service"
3. "refactor(notification): abstract service contracts using INotificationService interface and DTO types"
4. "feat(notification): add runtime payload validation using class-validator and global ValidationPipe"
5. "feat(notification): implement asynchronous provider factory and mock background processing engine"

---

### 5. Immediate Next Steps / Roadmap Tasks
The system infrastructure is now safe, fully multi-tenant aware, and performing optimally. Future feature modules to implement include:
1. The In-App Notification Feed Engine: Building endpoints (GET /v1/notifications/feed) to handle an in-app inbox center, enabling users to fetch notifications, mark them as read, or mark them as seen.
2. Production Provider Integration: Expanding the ProviderFactory to handle real production environments (e.g., integrating actual SendGrid SMTP configurations or Twilio API payloads).
3. Template Management Endpoints: Developing CRUD endpoints so tenants can securely create, preview, and update their message templates dynamically via an admin API.





🧭 Option 1: The In-App Notification Feed Engine
Right now, your system focuses on sending things out (Email/SMS). But many modern platforms (like Facebook, GitHub, or SaaS dashboards) need an In-App Notification Center (the little bell icon in the top right corner).

The Requirement:
You need to build endpoints that a frontend application can call to show a user their notifications.

What you need to design:
GET /v1/notifications/feed: An endpoint that takes a recipientId (the external ID) and returns a list of notifications where the channel is IN_APP.

SOLID Challenge: How will you handle pagination so you don't load 10,000 notifications at once?

PATCH /v1/notifications/:id/read: An endpoint to mark a specific notification as read (isRead: true).

PATCH /v1/notifications/read-all: An endpoint to mark all notifications for a specific recipient as read at once.

🛠️ Option 2: Production Provider Integration
Right now, your ProviderFactory intercepts everything and routes it to the MockEmailProvider or MockSmsProvider. It's time to make this system capable of hitting the real world when configured to do so.

The Requirement:
Extend your provider ecosystem so that if a tenant's ChannelConfig specifies a real provider, the system actually attempts a real network call.

What you need to design:
Integrate an actual email library (like @sendgrid/mail or standard nodemailer) inside a new production email provider class.

Integrate an actual SMS library (like the twilio SDK) inside a new production SMS provider class.

Update your ProviderFactory logic: Instead of forcing the mock provider every time, read the provider name from the database ChannelConfig (e.g., "sendgrid", "twilio", or "mock") and return the correct instance dynamically.

📋 Option 3: Tenant Template Management API
Right now, your testing template (welcome-email) is hardcoded into your database via the seed script. A true SaaS platform needs to let its tenants create, view, and update their own templates dynamically through an API.

The Requirement:
Build a CRUD (Create, Read, Update, Delete) API specifically for managing templates.

What you need to design:
POST /v1/templates: Allows a tenant (authenticated via their x-api-key) to create a new template layout specifying the layout name, slug, channel type, subject, and body.

GET /v1/templates: Lists all active templates belonging only to that authenticated tenant.

PUT /v1/templates/:slug: Allows them to update the HTML body or subject line of an existing workflow template instantly.

SOLID Challenge: Create a validation mechanism ensuring they don't accidentally save invalid Handlebars syntax that would crash the TemplateCompilerService later during a trigger!