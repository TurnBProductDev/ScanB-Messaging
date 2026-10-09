# ScanB Messaging

Conversations and tasks for the ScanB platform.

- **Direct messages and groups** between colleagues in the same customer.
- **Conversations about an item** — an insight or an AI summary, attached as a
  snapshot the Web App fetched and signed, so what colleagues read is what
  ScanB published rather than something a browser claimed.
- **Tasks** with an owner, a due date, a status workflow, reminders and
  escalation, each with its own conversation and its own history.

It is one of four repositories that ship as one product; see
`../ScanB-Platform/SYSTEM.md` for how they fit together, and
`docs/web-app-contract.md` for the contract with the Web App.

## Run it locally

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-dev.txt
cp .env.example .env         # then set MSG_CONTEXT_SIGNING_KEYS
.venv/Scripts/python -m uvicorn messaging.main:app --port 8010 --reload
```

Out of the box that runs with `MSG_DEV_MODE=true` (identity from an
`X-Dev-User` header), the in-memory store, and the sample people in
`dev/directory.dev.json`. Nothing persists and no Azure account is needed:

```bash
curl -H "X-Dev-User: priya@sample.test" http://localhost:8010/v1/me
```

Task reminders run in a second process: `python -m messaging.worker --once`.

## Test it

```bash
.venv/Scripts/python -m pytest -q                        # ~3s, no credentials
MSG_TEST_STORE=azure .venv/Scripts/python -m pytest -q   # the same suite on Azurite, ~30s
```

The second form is the one that matters before changing storage: it replays
every test against real Blob and Table semantics, including the version-checked
writes that keep two people sending at once from overwriting each other.

## Where things are

| Path | What |
|---|---|
| `messaging/api/routes.py` | the HTTP surface; thin |
| `messaging/services/` | every rule: access, conversations, tasks, reminders |
| `messaging/storage/base.py` | the storage contract; read this first |
| `messaging/storage/azure.py` | Blob + Table, and the layout, documented at the top |
| `messaging/context.py` | verified context (the Web App signs, this verifies) |
| `messaging/directory.py` | people and permissions, asked of the Web App |

`CLAUDE.md` lists the rules that must hold when changing any of it.
