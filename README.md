# AI CRM Event Bridge — Portfolio

A Node.js event-processing and AI reply workflow built for GoHighLevel integrations. This public portfolio snapshot demonstrates the implementation with synthetic account, contact and message identifiers. Production deployment receipts and private Git history are omitted.

## Business problem

Inbound leads need timely, relevant responses without duplicate messages, accidental follow-ups after an opt-out, or replies that conflict with a human conversation. The project separates event intake, verified CRM context, response generation, durable send state and human handoff.

## What the code demonstrates

- Webhook ingestion, signed callbacks, scoped authentication and persistent deduplication.
- Encrypted SQLite state, a durable outbox and bounded retries.
- GoHighLevel and OpenAI adapters, structured response validation and usage budgets.
- Opt-out checks, human takeover holds, fresh evidence checks and duplicate-send protection.
- A separate authentication facade and tests covering failure and recovery paths.

Built with AI coding assistance. The business requirements, workflow design and operational review reflect my work; this snapshot is provided for source review rather than as a hosted production demo.

## Run the backend tests

Use Node.js 24:

```sh
npm test
```

The backend has no third-party runtime dependencies. Tests use temporary databases, synthetic records and mocked network boundaries. They do not establish live delivery or production performance.

## Project guide

- `src/`: event bridge, CRM/model adapters and reply workflow.
- `test/`: automated implementation checks.
- `docs/`: example prompt/policy artifacts used by tests.
- `sites-facade/`: small authentication proxy.
- `sites-project/`: companion frontend/facade source; see its README.

Copy `.env.example` for local configuration. The portfolio uses synthetic identifiers and inactive defaults; replace configuration intentionally before any deployment. Credentials belong in your environment or deployment secret manager.

See `PORTFOLIO-NOTES.md` for the publication changes.

