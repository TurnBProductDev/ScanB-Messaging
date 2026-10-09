# Web App ↔ Messaging contract

Two directions. Both sides must change together.

## 1. Web App → Messaging (seam S4)

The Web App's `MessagesController` proxies the browser's requests with the
signed-in user's delegated token (scope `api://<messaging-api-id>/access_as_user`),
exactly as `FastApiService` does for the AI Bot. The browser never holds a
Messaging token and never calls Messaging directly, with one exception: the
Web PubSub URL returned by `POST /v1/realtime/negotiate`, which is receive-only.

Routes: see `messaging/api/routes.py` (`/v1/...`). Errors are flat JSON:
`{"code", "message", ...}`.

### Verified context token

Produced by the Web App (`MessagingContextSigner.cs`), verified by
`messaging/context.py`.

```
token   = b64url(payload_utf8_json) + "." + b64url(HMAC_SHA256(key, first_part_ascii_bytes))
payload = {
  "v": 1,
  "customerId": 12, "userId": 345,          # the signed-in user it was issued to
  "iat": 1789030000, "exp": 1789030300,     # seconds; exp - iat <= 300
  "anchor": {
    "kind": "insight" | "summary" | "report" | "dashboard" | "chat_answer",
    "resolution": "report" | "module" | "tenant-only",
    "key": "insight:target_tracker:4:2026-09-15",   # [A-Za-z0-9:_\-.]{1,200}
    "title": "...",                                  # <= 200 chars
    "reportRowId": 11, "moduleId": 3, "pbReportId": "guid", "reportSlug": "...",
    "dashboardKey": "...", "url": "/app-relative/only"
  },
  "snapshot": { ...the item as displayed, <= 32 KB... }
}
```

Key: `Messaging:ContextSigningKeys` (Web App) = `MSG_CONTEXT_SIGNING_KEYS`
(Messaging). Base64, 32+ bytes. First key signs; every listed key verifies.

## 2. Messaging → Web App (seam S5): the internal directory API

Called with Messaging's **application** token (managed identity or client
credentials) for the Web App's API scope, carrying app role
`Messaging.Directory`. A person's token is never accepted on these routes.

| Route | Body / query | Returns |
|---|---|---|
| `GET /internal/messaging/v1/users/by-email` | `?email=` | user, or 404 |
| `POST /internal/messaging/v1/users/lookup` | `{customerId, userIds}` | `{users: [user]}` — inactive users included |
| `GET /internal/messaging/v1/customers/{id}/users` | `?q=&limit=` | `{users: [user]}` — active only |
| `POST /internal/messaging/v1/access/anchor` | `{customerId, userIds, anchor}` | `{decisions: [{userId, allowed, reason}]}` |

`user` = `{userId, customerId, name, role, active, customerActive, messagingEnabled, isCustomerAdmin, canViewTeamTasks, canManageChannels}`. No email.

Anchor rules (Web App side, from its SQL entitlements):

| resolution | allowed when |
|---|---|
| `report` + `reportRowId` | the user's role holds an `AccessManagement` row for that report, report active, same customer |
| `report` + `pbReportId` | same, for any active report row in the customer with that Power BI id |
| `module` | the user's role holds any active report in that module |
| `tenant-only` | same customer, user active (Messaging short-circuits this without calling) |
| `chat_answer` | the customer has ScanB AI and the user is active |

Anyone the Web App does not return a decision for is treated as denied.
