# Logistics & Fleet Tracking

A backend for a **multi-hub logistics fleet**. It covers users, vehicles, driver–vehicle pairs, a hub network (pincode master + a Dijkstra path planner over a Mapbox-built hub graph), shipments with end-to-end planned paths and delay-tolerant ETAs, route optimization, live GPS ingestion, geofences, fuel logs, maintenance schedules, notifications, reports, and analytics.

The API server and the background worker run as two independent processes against the same Postgres + PostGIS database and Redis, streaming realtime updates to clients over Socket.IO.

> Tech: Node 22 + TypeScript (ESM) · Express 5 · Drizzle ORM · PostgreSQL 18 + PostGIS · Redis 8 · BullMQ · Socket.IO 4 · Mapbox (Directions + Matrix, **required**) · Cloudinary · Zod 4 · Winston · bcrypt (cost 12) · pnpm · **Bull Board** queue monitoring.

---

## Table of contents

1. [Architecture](#architecture)
2. [Quickstart](#quickstart)
3. [Running it](#running-it)
4. [Domain model](#domain-model)
5. [API surface](#api-surface)
6. [Realtime (Socket.IO)](#realtime-socketio)
7. [Background jobs (BullMQ)](#background-jobs-bullmq)
8. [Tracking pipeline](#tracking-pipeline)
9. [Routing & path planning](#routing--path-planning)
10. [Security model](#security-model)
11. [Operational notes](#operational-notes)
12. [Project layout](#project-layout)

---

## Architecture

```
┌────────┐   HTTP / WS    ┌─────────────────────┐   pub/sub   ┌───────────────────────┐
│ Client │ ─────────────▶ │  API server         │ ──────────▶ │  Redis (BullMQ +      │
│ (web,  │ ◀───────────── │  src/server.ts      │ ◀────────── │   adapter + cache +   │
│ mobile)│   Socket.IO    │  Express + Socket.IO│             │   tracking buffers)   │
└────────┘                └─────────┬───────────┘             └───────────┬───────────┘
                                    │ Drizzle                              │
                                    ▼                                      ▼
                          ┌──────────────────────┐              ┌──────────────────────┐
                          │  Postgres + PostGIS  │              │  Worker process      │
                          │  (single source of   │ ◀─────────── │  src/worker.ts       │
                          │  truth)              │   Drizzle    │  BullMQ workers      │
                          └──────────────────────┘              └──────────────────────┘
```

- **API process (`src/server.ts`)** - terminates HTTP/Socket.IO, validates input (Zod), enforces RBAC, writes to Postgres, enqueues jobs, and broadcasts WS events.
- **Worker process (`src/worker.ts`)** - runs all BullMQ workers (mail, audit, route optimize, tracking flush/retry/stuck-scan, notification dispatch). Exposes its own `/livez` and `/readyz` HTTP endpoints on `WORKER_HEALTH_PORT` (default 8081). Workers can also emit Socket.IO events through the shared Redis adapter.
- **Postgres + PostGIS** - holds all canonical state. Spatial columns (`geometry(Point,4326)`, `geometry(Polygon,4326)`) with GIST indexes power proximity queries and geofence containment via `ST_Contains`.
- **Redis** - plays four roles: BullMQ queues (`fleet-tracking:bull` prefix), Socket.IO adapter for cross-process broadcast, hot caches (latest vehicle ping, active driver→vehicle, active route per vehicle, geofence membership), and rate-limit counters.

Process boundaries are intentional: scaling the API horizontally is independent of scaling the worker pool, and a slow Mapbox call cannot stall the HTTP request path.

## Quickstart

Requires Node ≥ 22 and pnpm ≥ 11. Postgres needs the **PostGIS** and **`pg_uuidv7`** extensions enabled (the bundled `postgis/postgis` Docker image works; `pg_uuidv7` is created by the first migration).

```bash
# 1. Boot stack dependencies
docker compose up -d postgres redis

# 2. Install deps
pnpm install

# 3. Configure - copy the template and fill in secrets
cp .env.example .env

# 4. Apply schema migrations
pnpm db:migrate

# 5. (Optional) Bootstrap the first admin user
BOOTSTRAP_ADMIN_EMAIL=admin@example.com BOOTSTRAP_ADMIN_PASSWORD='Sup3r$ecret' pnpm bootstrap

# 6. Run API and worker in two terminals
pnpm dev:server
pnpm dev:worker
```

API will listen on `http://localhost:8080` (`PORT`). Worker health on `http://localhost:8081/readyz`.

Import the Postman collection from [`postman/API_documentation.postman_collection.json`](postman/API_documentation.postman_collection.json) and adjust `baseUrl` if needed.

## Running it

```bash
# Development (hot reload via tsx watch)
pnpm dev:server   # API
pnpm dev:worker   # Worker

# Type-check
pnpm typecheck

# Build & start (compiled JS in dist/)
pnpm build
pnpm start:server
pnpm start:worker

# Schema
pnpm db:generate   # drizzle-kit: emit migration from schema diff
pnpm db:migrate    # drizzle-kit: apply migrations
pnpm db:push       # drizzle-kit: push schema directly (dev only)
pnpm db:studio     # drizzle-kit: visual table explorer
```

API health endpoints:

- `GET /readyz` - DB + Redis ping, 200/503
- `GET /health` - 307 → `/readyz`

Worker health endpoints (on `WORKER_HEALTH_PORT`):

- `GET /livez` - process is alive
- `GET /readyz` - DB + Redis + each BullMQ worker `isRunning()` + shutdown flag

## Domain model

Schemas live under [`src/db/schema/`](src/db/schema). The hot relationships:

- **users** - (`admin | manager | driver`) → optional **driver_profiles** (1:1 for drivers)
- **vehicles** - (`idle | on_trip | maintenance`)
- **driver_vehicle_assignments** - driver↔vehicle pair with lifecycle:
  - `unassigned` = pair exists, not currently driving
  - `assigned` = pair is on a live `in_progress` route
  - `ended` = archived
  - At most one _live_ (`assigned`/`unassigned`) row per driver and per vehicle (enforced by partial unique indexes)
- **routes** - trips between two hubs. Owned by an assignment (nullable initially). Stores origin/destination snapshots (address + PostGIS point) plus Mapbox-computed polyline / distance / duration / waypoints / provider.
- **shipments** - (`created | assigned | in_transit | delivered | cancelled`). Carry `origin_pincode`, `dest_pincode`, a frozen `planned_path` JSONB (hub-by-hub itinerary), `shipment_eta` + `baseline_shipment_eta` (delay-tolerant ETA math), `current_leg_idx`, `delay_window_seconds`, `cumulative_delay_seconds`. Point to a route for the current leg.
- **shipment_status_history** - append-only audit of every status transition.
- **shipment_hub_events** - granular timeline of hub-level events (`arrived_at_hub`, `departed_hub`, `picked_up_from_origin`, `delivered_at_destination`, etc.) per shipment leg.
- **hubs** - distribution centres with code, name, representative pincode, PostGIS point, `primary | secondary` kind, soft-deletable.
- **pincodes_master** - India-Post-sourced master table (~155 K rows). Immutable reference.
- **hub_district_mappings** - ops layer: "Hub-Pune serves all pincodes whose district = Pune." Triggers `hub.pincodes.rematerialize` to repopulate the next table.
- **hub_serviceable_pincodes** - materialised mapping (`hubId × pincode`) of which hub services which pincode. `district` (rematerialized) and `manual` (admin-curated) sources; manual rows survive rebuilds.
- **hub_connections** - directional edges in the hub graph with `distance_km`, `typical_duration_s`, `cost_inr`, `is_active`, `manual_override`. Auto-built via Mapbox Matrix; manual rows survive rebuilds.
- **pincode_routes** - cached pre-computed Dijkstra paths keyed by hub representative pincodes (`from_pincode × to_pincode`). `manual` (admin preset) and `computed` sources.
- **vehicle_current_location** - last GPS fix per vehicle (unique on `vehicle_id`).
- **vehicle_location_history** - append-only history with a partial unique index on `(vehicle_id, client_id)` so offline batches stay idempotent.
- **geofences** / **geofence_events** - PostGIS Polygons + enter/exit log (geofences soft-deletable).
- **fuel_logs** - per-vehicle fills with cost/litres/odometer.
- **maintenance_schedules** - vehicle service plans (`due_at` and/or `due_odometer_km`).
- **notifications** + **notifications_reads** - fanned out per user OR per role; reads are recorded per user, so multiple recipients of a role-targeted notification each get their own read state.
- **report_files** - pointers to generated CSV/PDF reports (in Cloudinary or local FS) with a 7-day TTL.
- **refresh_tokens** - opaque hashed refresh tokens with a family ID for reuse-attack detection.
- **audit_log** - global audit trail written async via the audit worker.

UUIDs are generated server-side via `uuidv7()` (Postgres extension), so they sort by creation time - handy for keyset paging if you switch off `OFFSET`.

## API surface

All endpoints are mounted under `/api/v1`. **111 HTTP endpoints + 2 health endpoints + Bull Board UI**. See [`postman/API_documentation.postman_collection.json`](postman/API_documentation.postman_collection.json) for live examples with bodies, query params, and RBAC notes.

### Response envelope

Success:

```json
{
  "success": true,
  "message": "...",
  "data": {
    /* ... */
  }
}
```

Error (from [`src/middleware/errorHandler.ts`](src/middleware/errorHandler.ts)):

```json
{
  "success": false,
  "code": "VALIDATION_ERROR",
  "message": "Validation failed",
  // "details" is present only on validation errors
  "details": [
    {
      "source": "body",
      "field": "body.email",
      "code": "invalid_string",
      "message": "..."
    }
  ],
  "timestamp": "2026-05-26T08:00:00.000Z"
}
```

Status codes are reused consistently:

- `400 BAD_REQUEST` (PG 23503/23502)
- `401 UNAUTHORIZED_ERROR` (incl. invalid OTP)
- `403 FORBIDDEN`
- `404 NOT_FOUND`
- `409 CONFLICT` (PG 23505, business conflicts)
- `422 VALIDATION_ERROR`
- `429 RATE_LIMIT_EXCEEDED`
- `5xx INTERNAL_SERVER_ERROR / DATABASE_ERROR`

## Realtime (Socket.IO)

Mounted on the same HTTP server as the API. Clients pass the access token via `socket.handshake.auth.token`; verification reuses `verifyAccessToken` (HS256, same secret as `/auth`).

After connection:

- Every socket auto-joins `user:{id}`.
- `admin` and `manager` auto-join `manager:all`.

Subscriptions are explicit per-resource and are ACL'd server-side:

| Client emits           | Payload          | Rooms joined            | ACL                                                                        |
| ---------------------- | ---------------- | ----------------------- | -------------------------------------------------------------------------- |
| `subscribe:vehicle`    | `{ vehicleId }`  | `vehicle:{vehicleId}`   | manager/admin always; driver only if currently assigned                    |
| `unsubscribe:vehicle`  | `{ vehicleId }`  | leaves room             | open                                                                       |
| `subscribe:shipment`   | `{ shipmentId }` | `shipment:{shipmentId}` | manager/admin always; driver only if their pair is on the shipment's route |
| `unsubscribe:shipment` | `{ shipmentId }` | leaves room             | open                                                                       |

Server-emitted events:

| Event                             | Room(s)                            | Sent on                                                        |
| --------------------------------- | ---------------------------------- | -------------------------------------------------------------- |
| `location:update`                 | `vehicle:{id}`                     | Live ping accepted (also primed from Redis cache on subscribe) |
| `shipment:created`                | `manager:all`                      | New shipment                                                   |
| `shipment:assigned`               | `shipment:{id}`, `user:{driverId}` | Shipment attached to a driver via route                        |
| `shipment:status`                 | `shipment:{id}`                    | Status transition                                              |
| `shipment:route` / `shipment:eta` | `shipment:{id}`                    | Route optimize completed                                       |
| `notification:new`                | `user:{recipient}`                 | Notification dispatched (deduped by `notif:dedup:*` key)       |
| `notification:read`               | `user:{me}`                        | Self acknowledged a notification                               |
| `geofence:alert`                  | `vehicle:{id}`, `manager:all`      | Enter/exit transition detected                                 |

The Redis adapter (`@socket.io/redis-adapter`) means the worker process can also broadcast: e.g., when the route optimize worker finishes, `emitShipmentRoute` from inside the worker reaches all browsers via Redis pub/sub. To make this work, the worker boots a "publisher-only" `SocketIOServer` instance.

## Background jobs (BullMQ)

Each queue has its own dedicated `ioredis` connection (per BullMQ's hard requirement that workers and queues use separate connections - see [`src/jobs/connection.ts`](src/jobs/connection.ts)). All queues share the prefix `fleet-tracking:bull`.

| Queue / job name              | Concurrency                          | Retries     | Used for                                                                                                                                  |
| ----------------------------- | ------------------------------------ | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `mail.send`                   | 5                                    | 5, exp 2 s | Outbound SMTP (OTPs, password change confirmations)                                                                                       |
| `audit.write`                 | 10                                   | 5, exp 1 s | Append rows to `audit_log`                                                                                                                |
| `route.optimize`              | 5                                    | 3, exp 2 s | Mapbox Directions call + DB save + WS broadcast. Idempotent via Redis "inflight" key                                                      |
| `tracking.flush`              | 15 (lock 60 s, stalledInterval 30 s) | 3, exp 2 s | Snapshot of buffered live pings -> `vehicle_location_history` + `vehicle_current_location` upsert                                         |
| `tracking.retry_flush`        | 5                                    | 5, exp 2 s | Driver-supplied offline batch -> same write path, idempotent on `(vehicle_id, client_id)`                                                 |
| `tracking.stuck-scan`         | 1                                    | 2          | Cron (`0 * * * *`) scans `veh:*:counter` and force-flushes vehicles whose buffer head is older than `STUCK_BUFFER_AGE_SECONDS`             |
| `notification.dispatch`       | 10                                   | 5, exp 1 s | Insert into `notifications`, fan-out to `user:{id}` rooms, dedupe via `notif:dedup:{key}`                                                 |
| `report.generate`             | 2                                    | 2, exp 5 s | Render large CSV/PDF reports, upload to Cloudinary (or local FS), dispatch `report:ready` socket event + notification                     |
| `reports.cleanup-expired`     | 1                                    | 2          | Daily cron (02:00 UTC). Deletes report files past `REPORT_FILE_TTL_DAYS`                                                                  |
| `data.retention`              | 1                                    | 2          | Daily cron (03:00 UTC, `RETENTION_CLEANUP_CRON`). Batched DELETEs of expired refresh tokens, audit logs > `RETENTION_AUDIT_LOG_DAYS`, location history > `TRACKING_HISTORY_TTL_DAYS` |
| `hub.pincodes.rematerialize`  | 1                                    | 3, exp 2 s | Re-materialises `hub_serviceable_pincodes` from `pincodes_master` whenever a district mapping is added/removed                            |
| `hub.connections.rebuild`     | 1                                    | 5, exp 5 s | Mapbox Matrix API → **single bulk UPSERT** of all hub-to-hub connections for the changed hub (N+1 round-trips collapsed to 1 per chunk)   |
| `shipment.eta-sweep`          | 1                                    | 3, exp 30 s | Repeatable cron (`SHIPMENT_ETA_SWEEP_CRON`, default every 2 h). Projects shipment ETAs forward for stuck-in-transit shipments where the truck is overdue but the route hasn't completed yet |

All workers are colocated in the `worker` process and started in parallel by [`src/jobs/index.ts`](src/jobs/index.ts:55). Shutdown drains workers, closes queues, closes Redis, closes the DB pool, with a hard timeout from `SHUTDOWN_TIMEOUT_MS`.

`audit.write` failures are non-fatal at the call site: [`audit()`](src/utils/audit.ts:24) catches and logs `AUDIT_ENQUEUE_FAILED` so business operations are never blocked by an audit pipeline outage.

### Bull Board (queue monitoring)

Mounted at **`GET /admin/queues`** when both `BULLBOARD_USER` and `BULLBOARD_PASSWORD` env vars are set. Browser pops an HTTP Basic Auth prompt; credentials are reused for the UI's sub-requests automatically. If either env var is missing the route isn't registered, so production deploys that don't ship credentials get zero exposure.

The board surfaces all 13 BullMQ queues with counts, per-job payload/return/logs, retry/promote/clean actions, and pause/resume. Wired in [`src/jobs/bullBoard.ts`](src/jobs/bullBoard.ts) using `@bull-board/api` + `@bull-board/express`.

Behind a reverse proxy: keep `/admin/queues` on TLS (Basic Auth credentials travel in headers) and consider an extra IP allowlist for defense-in-depth.

## Tracking pipeline

The tracking pipeline is the most performance-sensitive path; here is what actually happens per driver ping (`POST /api/v1/tracking`):

1. **Express** → `authMiddleware(["driver"])` → `trackingBurstLimiter` (5/s) → `trackingSustainedLimiter` (5/10s) → `validate(ingestSchema)`. Validators enforce ±5 min past / ±30 s future skew, lat/lng bounds, etc.
2. **Resolve vehicle**: [`getActiveVehicleIdForDriver`](src/module/tracking/tracking.services.ts:48) hits Redis `fleet:driver-active-vehicle:{driverId}` (1 h TTL); falls back to a Postgres lookup and re-primes the cache.
3. **Anomaly check**: previous fix from `veh:{id}:last` + `:ts` keys, distance/dt against `ANOMALY_TELEPORT_M_PER_S`. In `drop` mode the ping is discarded; in `flag` mode it's annotated.
4. **Lua atomic in Redis** ([`INGEST_LUA`](src/module/tracking/tracking.helper.ts:153)):
   - Writes `veh:{id}:last` + `:ts` _only if_ not anomalous and newer.
   - `RPUSH` the serialized ping into `veh:{id}:buffer`, `LTRIM` to `BUFFER_HARD_CAP` if over.
   - `INCR veh:{id}:counter`; if it reaches `FLUSH_THRESHOLD`, return the entire buffer snapshot, `DEL` the buffer, and reset the counter.
5. If the Lua script returned a flush snapshot, the API enqueues `tracking.flush` with the pings. The worker calls `applyPingsToVehicle("live")`, which inserts history rows and upserts `vehicle_current_location` _only if_ `recordedAt > current.recordedAt` (preserves monotonicity under concurrency).
6. The API also (non-blocking) emits `location:update`, runs geofence detection, and runs off-route detection. The latter, on a violation, debounces re-optimize enqueues via `route_off-route_debounce_{routeId}`.

### Request flow & latency

The hot path never touches the DB — it does **three Redis round-trips** and returns. The heavy work (history writes, geofence math, route re-optimize) is pushed off the request: either to a BullMQ worker or to fire-and-forget tasks. That is what keeps the per-ping response at **~20–30 ms**.

```
POST /api/v1/tracking   (driver)
   │
   ▼
═══ SYNCHRONOUS PATH ════════════════════════════ awaited · ~20–30 ms ═══
   1  auth(["driver"]) · burst 5/s · sustained 5/10s · Zod validate
   2  resolve vehicleId
        Redis GET fleet:driver-active-vehicle:{driverId}        ← cache
          └ miss → Postgres lookup → re-prime cache (fire & forget)
   3  anomaly check
        Redis GET veh:{id}:last (+ :ts) → turf distance / Δt
          vs ANOMALY_TELEPORT_M_PER_S  →  drop | flag
   4  INGEST_LUA   (1 atomic Redis round-trip)
        SET :last/:ts (if clean & newer) · RPUSH :buffer · LTRIM cap · INCR :counter
          └ counter == FLUSH_THRESHOLD → return buffer snapshot + DEL
   5  if flushed → enqueue tracking.flush   (BullMQ — returns instantly)
   │
   ▼   ✅ HTTP response returns here
   │
═══ FIRE-AND-FORGET ═══════════════════════════ void … .catch · not awaited ═══
   •  emit Socket.IO  location:update → room vehicle:{id}
   •  geofence enter/exit detection
   •  off-route check → debounced route.optimize enqueue

═══ WORKER  (async, off the request path) ══════════════════════════════
   tracking.flush → applyPingsToVehicle("live")
        → INSERT vehicle_location_history   (batch of FLUSH_THRESHOLD)
        → UPSERT vehicle_current_location   (only if recordedAt newer)
```

Offline batch (`POST /api/v1/tracking/retry`) takes up to 240 strictly-monotonic pings per request (each with a UUID `clientId`), groups them by the historic assignment window each ping fell into (using `findDriverAssignmentsOverlappingWindow`), and enqueues `tracking.retry_flush` per-vehicle. Writes use `ON CONFLICT (vehicle_id, client_id) DO NOTHING` so reuploading is idempotent.

The hourly `tracking.stuck-scan` job exists for crash recovery: if the API enqueued pings into the Redis buffer but BullMQ ate the flush job (or the worker was OOM-killed mid-batch), the next scan picks up the stale buffer and flushes it directly to the DB.

## Routing & path planning

Three layers cooperate to figure out where a parcel goes:

1. **Mapbox Directions** — wired in [`src/integrations/mapbox/mapboxProvider.ts`](src/integrations/mapbox/mapboxProvider.ts). Hits `/directions/v5/mapbox/driving-traffic/...` with 15 s timeout and one retry. Validates the response with a Zod schema. Token is logged as `access_token=***` on failure. Called from `route.optimize` jobs to draw the truck's polyline for one leg of the trip. (Mock provider was removed — Mapbox is required.)
2. **Mapbox Matrix** — [`src/integrations/mapbox/matrix.ts`](src/integrations/mapbox/matrix.ts). Single API call returns a duration + distance matrix for up to 25 coordinates. Used by the `hub.connections.rebuild` job to compute every hub-to-hub edge in the graph in O(N²/25) calls instead of O(N²) Directions calls. The job builds a batch per chunk and writes them all in **one** bulk UPSERT — no per-row round-trips.
3. **Dijkstra path planner** — [`src/module/pathPlanner/`](src/module/pathPlanner). At shipment creation time, resolves origin/destination pincodes to hubs, looks up the cached precomputed path in `pincode_routes`, and falls back to Dijkstra over the hub graph (adjacency map cached in Redis for 5 min, weight = `typical_duration_s`). Result is upserted into `pincode_routes` for the next shipment.

Routes themselves are now strictly **hub-to-hub trips**. The `POST /routes` body accepts a hub UUID or hub code as a plain string for origin/destination/waypoints; the service resolves to the hubs table and snapshots `address` / `lat` / `lng` onto the route row so the polyline stays stable even if a hub is later moved.

### The hub graph (edges & vertices)

The planner models the network as a **directed weighted graph**:

- **Vertices** = rows in `hubs` (each hub is a node).
- **Edges** = rows in `hub_connections` — directed `from_hub → to_hub`, with `distance_km`, `typical_duration_s`, and `cost_inr`.
- **Edge weight** = `typical_duration_s` (the planner optimizes for time, not distance).
- Edges are auto-built by the `hub.connections.rebuild` job calling the **Mapbox Matrix** API (≤ 25 coords/call); `manual_override` edges survive rebuilds.

```
  vertices = hubs        edges = hub_connections (directed, weight = typical_duration_s)

        ┌───────────┐   1200s    ┌────────────┐    900s
        │ A · Mumbai│ ─────────▶ │ B · Nashik │ ─────────┐
        └─────┬─────┘            └────────────┘          │
              │ 1500s                                     ▼
              ▼                                    ┌────────────┐
        ┌───────────┐         1100s                │ D · Delhi  │
        │ C · Surat │ ───────────────────────────▶ └────────────┘
        └───────────┘
```

**How a path is calculated** (at shipment creation, `from_pincode → to_pincode`):

1. Resolve each pincode to its serving hub via `hub_serviceable_pincodes`.
2. **Cache hit?** Look up `pincode_routes (from_pincode × to_pincode)` — if a `manual` or `computed` path exists, use it and stop.
3. **Cache miss → Dijkstra**: run shortest-path (binary min-heap) over the hub graph. The adjacency map is built from `hub_connections` and cached in Redis for 5 min, so repeated planning doesn't re-read the table.
4. **Upsert** the resulting hub-by-hub path into `pincode_routes` so the next identical shipment is a cache hit.

For the graph above, planning **A → D** compares:

```
  A → B → D = 1200 + 900  = 2100s   ✅ chosen (lowest typical_duration_s)
  A → C → D = 1500 + 1100 = 2600s
```

The chosen path becomes the shipment's frozen `planned_path` (hub-by-hub itinerary); each leg then gets its own Mapbox Directions polyline via a `route.optimize` job.

## Security model

- **Passwords** - bcrypt (cost factor 12). A constant-time dummy hash is compared when a user lookup misses, so login timing doesn't leak whether the email exists.
- **Shipment codes** - generated via Node's `crypto.randomInt` over a 32-char unambiguous alphabet (no I/O/0/1) - no `Math.random` birthday-paradox collisions on the `uq_shipments_code` unique index.
- **JWT** - HS256, issuer-bound (`logistics-fleet-tracking`), separate secrets for access/refresh, both required ≥ 32 chars at boot.
- **Refresh tokens** - stored as a SHA-256 hash with a per-session family ID. Reuse detection is hard-coded: any second use of a known JTI revokes the entire family and logs `event=TOKEN_REUSE_ATTACK severity=HIGH`.
- **Cookies** - `httpOnly`, `secure` in production, `SameSite=Strict` in production (`Lax` in dev), scoped to `/api/v1/auth`.
- **CORS** - explicit allow-list via `CORS_ORIGINS`, credentials enabled.
- **Helmet** - default suite, `x-powered-by` disabled, `trust proxy = 1` (one hop assumed; if you put N hops in front, adjust).
- **Rate limiting** ([`src/middleware/rateLimit.ts`](src/middleware/rateLimit.ts)) - global 200/15min by IP, login 5/15min by email-or-IP, register 30/h by actor, OTP `1/min` + `5/h` by email-or-IP, change-password 5/h by user, tracking burst/sustained by driver, retry-batch 20/min by driver. Redis-backed when `NODE_ENV != test`.
- **OTP** - 6-digit, SHA-256 hashed in Redis (5 min TTL), max 3 wrong attempts, timing-safe comparison.
- **Audit log** - every privileged mutation calls `audit("entity.action", target, before, after, ctx)`; correlation IDs are propagated end-to-end.
- **Validation** - every route gates inputs with `validate()`; `strictObject` rejects unknown keys; assigned `req.body / params / valid` are post-coercion.
- **PII redaction** - Winston format strips `password`, `token`, `accesstoken`, `refreshtoken`, `authorization`, `cookie`, `secret`, `apikey`, `key`, `tokenhash`, `passwordhash` keys before serialization (case-insensitive).
- **Body limits** - JSON and urlencoded both capped at 256 KB.

## Operational notes

- **Graceful shutdown** - implemented for both processes (SIGTERM, SIGINT, uncaughtException, unhandledRejection). The worker stops accepting new HTTP traffic, waits for in-flight jobs (lockDuration 60s on the tracking flush worker), then closes Redis and DB pools.
- **Correlation IDs** - take `x-correlation-id` from incoming requests if it matches `^[A-Za-z0-9_-]{1,64}$`, otherwise generate a UUID. Propagated to logs, audit rows, and job envelopes.
- **Logging** - Winston JSON in production, colourized in dev. Every request emits one structured line on `finish` with method, URL, status, duration, correlation ID, and user ID.
- **Health** - `/readyz` returns 503 when DB or Redis is down; wire this into your load balancer and your Kubernetes readiness probe.
- **Worker readiness** - a worker is "ready" only when DB + Redis are reachable AND every individual BullMQ worker `isRunning()`. During shutdown the same endpoint returns 503 with `shuttingDown:true`, so a rolling restart drains traffic cleanly.

## Project layout

```
src/
  app.ts                  Express app composition + route mounting
  server.ts               HTTP server + Socket.IO gateway + graceful shutdown
  worker.ts               BullMQ workers + worker /readyz endpoint
  config/env.ts           All env-var parsing (throws on missing required)
  db/
    index.ts              pg.Pool + drizzle instance, connection check
    schema/               Drizzle schemas (one file per entity + enum.ts + helper.ts)
  integrations/mapbox/    Mapbox integration (mapboxProvider = Directions, matrix = Matrix API)
  jobs/
    connection.ts         Redis connection factory + queue prefix
    queues/               One file per BullMQ queue (mail/audit/route/tracking*/notification)
    workers/              One file per Worker, all wired in worker.ts
    handlers/             Job logic (audit/mail/routing/tracking/notification)
    index.ts              startWorkers / closeWorkers / closeQueues / getWorkerHealth
  middleware/             auth, correlationId, errorHandler, rateLimit, requestLogger, validate
  module/                 Feature folders: routes / controllers / services / repositories /
                          validators / helpers (assignments, auth, fuel, geofences,
                          maintenance, notifications, routes, shipments, tracking, users,
                          vehicles)
  realtime/
    gateway.ts            Socket.IO + Redis adapter for API and worker processes
    emit.ts               Strongly-typed broadcast helpers (one per event)
    handshake.ts          JWT auth middleware for sockets
    rooms.ts              canWatchVehicle / canWatchShipment ACL
    subscriptions.ts      subscribe/unsubscribe handlers with ACL checks
  utils/                  asyncHandler, audit, dbCallWrapper (PG error mapping -> AppError),
                          dedupe (Redis SETNX), errors, jobs, mail, password, rbac, redis,
                          response, token, transaction
  types/express.d.ts      req.user, req.correlationId, req.valid augmentations
  logger/index.ts         Winston logger with PII redaction
drizzle/                  Generated SQL migrations + meta
postman/                  Postman collection (kept in sync with the routes)
scripts/                  remove-comments.cjs, update-postman.cjs, verify-mapbox.ts
docker-compose.yml        Postgres+PostGIS and Redis services
drizzle.config.ts         drizzle-kit configuration
```

---

### Conventions worth knowing

- **Modules follow a strict folder shape**: `routes.ts -> controllers.ts -> services.ts -> repositories.ts -> validators.ts -> helper.ts`. Don't reach across modules at the repository layer except where already wired (`assignments.repositories` is imported by `tracking`, `fuel`, `geofences` for the `isDriverAssignedToVehicle` check).
- **All repository functions are wrapped in [`dbCallWrapper`](src/utils/dbCallWrapper.ts)**, which converts Postgres SQLSTATE codes into `ConflictError` / `BadRequestError` / `DatabaseError`. Don't catch pg errors inside services; they'll already be `AppError`s by the time they surface.
- **Soft delete** uses `deleted_at` on Users, Vehicles, Shipments. Every list/get path filters with `IS NULL`. Cascading deletes are restricted to leaf entities (`refresh_tokens`, `notifications_reads`); business deletes are blocked when there's an active dependency (driver on a trip, vehicle on a route, shipment past `created`).
- **PostGIS columns** are written via raw SQL (`ST_SetSRID(ST_MakePoint(lng, lat), 4326)`) and read back via `ST_Y` / `ST_X` to keep types numeric. Polygons round-trip through `ST_AsGeoJSON` / `ST_GeomFromGeoJSON` with `ST_IsValid` checks before insert.
