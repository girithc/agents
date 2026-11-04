# AgentHub

A Rust-native platform for running AI agents: persistent secrets, full
traceability per run, and a feedback loop that continually improves agents.
Interaction is via terminal today; a web UI can be added later against the
same HTTP API.

## Architecture

```
┌──────────────┐   HTTP (JSON + SSE)   ┌─────────────────────────────┐
│  agentctl    │ ────────────────────► │  crates/api (axum server)   │
│  (terminal)  │                       │  CRUD · runs · traces ·     │
└──────────────┘                       │  feedback · improve         │
                                       └──────────┬──────────────────┘
                                                  │
                                       ┌──────────▼──────────────────┐
                                       │  crates/runtime             │
                                       │  run loop · tool calls ·    │
                                       │  transcript persistence     │
                                       └──────────┬──────────────────┘
                                                  │ Arc<dyn Provider>
                                       ┌──────────▼──────────────────┐
                                       │  crates/providers (common)  │
                                       │  meta (api.meta.ai / Muse)  │
                                       │  mock (offline)             │
                                       │  …add more modules later    │
                                       └──────────┬──────────────────┘
                                                  │
                                       ┌──────────▼──────────────────┐
                                       │  crates/storage             │
                                       │  SQLite (WAL) · AES-256-GCM │
                                       │  secrets vault · traces     │
                                       └─────────────────────────────┘
```

Crate layout:

| Crate | Purpose |
|---|---|
| `crates/api` | axum HTTP server: CRUD + run inspection + SSE streaming. Binary: `agenthub-api`. |
| `crates/cli` | Terminal client `agentctl` (serve, run, repl, secrets, feedback, improve). |
| `crates/runtime` | Agent run loop: message assembly, tool-call loop, run transcripts. |
| `crates/providers` | **Common provider layer.** Each provider is an isolated module with its own HTTP client, wire-format mapping, and error type. |
| `crates/storage` | SQLite persistence, AES-256-GCM secrets vault, trace tables. |
| `crates/telemetry` | `tracing` setup + a custom layer that persists spans/events into SQLite per `run_id`. |

## Provider isolation

All provider integrations live in `crates/providers`, each in its own module:

- **Own HTTP client** — connection pools, timeouts, and DNS are never shared.
- **Own wire-format code** — request building and response parsing are local
  to the module; a schema change in one provider cannot break another.
- **Own error mapping** into a shared `ProviderError`.
- The runtime additionally wraps every provider call in a **timeout and a
  panic barrier**, so a hung or panicking provider becomes a normal error
  instead of wedging the server.

Currently registered: `meta` (Meta AI `api.meta.ai/v1/responses`,
`muse-spark-1.3-contributor`) and `mock` (deterministic, offline). Adding a
provider = new module + `impl Provider` + one `register(...)` call.

## Tracing

`tracing` is embedded at every layer: HTTP middleware, API handlers, the
runtime (`agent.run` spans), providers (`provider.complete` spans, incl.
`stream = true/false`), and tools (`tool.call`). A custom `DbLayer`
persists every span and event into SQLite attributed to `run_id`; spans
without their own `run_id` inherit it from their parent. Inspect with
`GET /v1/runs/{id}/traces` or `agentctl runs show <id> --traces`.

**Request correlation:** every HTTP request gets a unique id (or honors an
incoming `x-request-id`), echoed back in the `x-request-id` response
header, recorded in the request's trace span, and stamped on any run it
creates — so a run, its trace tree, and the originating HTTP request are
correlatable end to end.

Console output honors `RUST_LOG` (default `info`); set `AGENTHUB_LOG_JSON=1`
for JSON logs. Agent-activity lines use the governance format
`HH:MM:SS agent[model] action[reasoning]` — no level noise (WARN/ERROR keep
a level tag); the full structured fields are always persisted to SQLite.

## Quickstart

```bash
cargo build

# start the API server (data in ./data)
./target/debug/agentctl serve --port 8080

# another shell
export AGENTHUB_URL=http://127.0.0.1:8080

# 1. store a secret (encrypted at rest; never returned in plaintext by default)
printf '%s' "$MODEL_API_KEY" | ./target/debug/agentctl secret set meta.api_key

# 2. create an agent
./target/debug/agentctl agent create \
  --name muse-demo \
  --prompt "You are a helpful assistant. Be concise."

# 3. run it (streams tokens live)
./target/debug/agentctl run muse-demo "What is 17 * 23?"

# 4. chat interactively
./target/debug/agentctl repl muse-demo

# 5. inspect runs + traces
./target/debug/agentctl runs list
./target/debug/agentctl runs show <run-id> --traces

# 6. feedback loop
./target/debug/agentctl feedback <run-id> --rating 3 --comment "too slow"
./target/debug/agentctl improve muse-demo --dry-run   # propose new prompt
./target/debug/agentctl improve muse-demo             # apply + version bump
```

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | liveness |
| POST | `/v1/agents` | create agent |
| GET | `/v1/agents` | list agents |
| GET | `/v1/agents/{id}` | get agent (id or name) |
| PATCH | `/v1/agents/{id}` | update agent |
| DELETE | `/v1/agents/{id}` | delete agent |
| GET | `/v1/agents/{id}/versions` | prompt version history |
| POST | `/v1/agents/{id}/improve` | feedback-loop improvement (`{"dry_run": bool}`) |
| GET | `/v1/agents/{id}/feedback` | feedback summary + entries |
| POST | `/v1/agents/{id}/runs` | execute a run, returns run + messages |
| POST | `/v1/agents/{id}/runs/stream` | execute + stream SSE (`run`, `delta`, `done`, `error` events) |
| GET | `/v1/runs?agent_id=&kind=&limit=` | **all runs** (filter by agent and kind: `regular` / `improve`) |
| GET | `/v1/runs/{id}` | **inspect a run** with full transcript |
| GET | `/v1/runs/{id}/traces` | persisted span tree + events for the run |
| POST | `/v1/runs/{id}/feedback` | rate a run 1–5 (+ comment) |
| GET | `/v1/secrets` | list secret names (never values) |
| PUT | `/v1/secrets/{name}` | store/update a secret (encrypted) |
| GET | `/v1/secrets/{name}` | masked value; add `?reveal=true` to expose (logged as a warning) |
| DELETE | `/v1/secrets/{name}` | delete a secret |

## Secrets

- Stored in SQLite as `nonce ‖ ciphertext`, AES-256-GCM.
- Master key: `data/master.key`, 32 random bytes, created on first start with
  `0600` permissions.
- Providers declare which secret they need (`meta.api_key`); the runtime
  resolves it per run. A missing secret fails the run with a clear,
  actionable error.
- The API never returns secret values unless explicitly revealed
  (`?reveal=true`, which is logged).

## Feedback loop

1. Humans rate runs: `POST /v1/runs/{id}/feedback`.
2. `POST /v1/agents/{id}/improve` aggregates ratings, comments, and the last
   10 regular-run transcripts, and asks the agent's own provider/model to
   propose a rewritten system prompt.
3. `dry_run: false` saves the proposal as a new immutable version and bumps
   the agent (`agent_versions` keeps the full history).

The improvement itself is a **first-class run** with `kind = "improve"`: it
gets its own run id, full transcript (the improvement prompt + the model's
proposal), and trace tree (`agent.improve` → `provider.complete`), and shows
up in `GET /v1/runs` alongside regular runs — identified by kind rather than
treated differently.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `AGENTHUB_HOST` | `127.0.0.1` | bind host |
| `AGENTHUB_PORT` | `8080` | bind port |
| `AGENTHUB_DATA_DIR` | `./data` | SQLite DB + master key location |
| `AGENTHUB_META_BASE_URL` | `https://api.meta.ai` | Meta API endpoint |
| `AGENTHUB_URL` | `http://127.0.0.1:8080` | CLI → server base URL |
| `RUST_LOG` | `info,agenthub=debug` | log filter |
| `AGENTHUB_LOG_JSON` | off | JSON console logs if `1` |

## Tests

```bash
cargo test
```

Covers: vault encryption round-trip, agent/run/secret CRUD, Meta request
shape + Responses-style parsing + SSE event handling, the runtime run loop
(mock provider), run failure persistence, and trace span/event persistence
with run_id inheritance.

## Later

- Web UI consuming the same API (SSE endpoint is already UI-ready).
- More providers in `crates/providers` (OpenRouter, Groq, Ollama, …).
- Auth on the API, agent-level API keys.
- Background/async runs with a queue; tool-call streaming.
