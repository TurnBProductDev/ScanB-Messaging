# ScanB Messaging

Conversations and tasks for the ScanB platform: direct messages, groups,
conversations about an insight or report, and task assignment with status
tracking and reminders.

@../ScanB-Platform/SYSTEM.md

## Shape

```
messaging/
  api/routes.py        HTTP, thin: parse, resolve caller, call a service
  services/            every rule lives here
    common.py          Access (the two gates), Auditor, Notifier
    threads.py         conversations, messages, inbox, activity
    tasks.py           task workflow, lists, team view
    reminders.py       due-soon / overdue / stale / escalation
  storage/base.py      the storage contract (read this first)
  storage/memory.py    tests and local dev
  storage/azure.py     Blob + Table, the production store
  context.py           verified context (HMAC, signed by the Web App)
  directory.py         people and permissions, asked of the Web App
worker.py              reminder loop
```

## Rules that must hold

* **Identity comes from the token, never from input.** No endpoint takes a
  sender id. Services receive an `Actor`.
* **Two gates on every read.** Membership (else 404, never 403), and access
  to the attached item, re-checked on every view because permissions change.
* **Fail closed.** Directory unreachable means nobody resolves and no item
  shows. Never cache a failure.
* **Context is accepted only signed.** `context.py` and the Web App's
  `Service/impl/MessagingContextSigner.cs` must change together.
* **Audit carries ids, never message text.**
* **Both stores pass the same contract.** A store change needs
  `tests/integration/test_store_contract.py` green on memory and on Azurite.

## Gate

```
.venv/Scripts/python -m pytest -q                                   # unit, ~3s, no credentials
MSG_TEST_STORE=azure .venv/Scripts/python -m pytest -q              # everything again on Azurite, ~25s
```

Azurite: `docker start scanb-azurite` (created once; see tests/store_factory.py).

## Never

* commit or push unless asked;
* put a real connection string, signing key or client secret in a tracked file;
* write `\u` or `\x` escapes through a file-writing tool — it has turned them
  into raw control characters here before. Build such characters with `chr()`.
