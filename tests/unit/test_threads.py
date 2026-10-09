from __future__ import annotations

import pytest

from messaging.errors import Forbidden, Invalid, NotFound, RateLimited
from messaging.ratelimit import Limit
from messaging.views import HIDDEN_TITLE
from tests.conftest import AHMED, FORMER, Harness, LINA, MARCUS, OTHER, PRIYA

pytestmark = pytest.mark.unit


async def send(h, actor, thread_id, text="hello"):
    return await h.threads.post_message(actor, thread_id, message_id=h.ulid(), text=text)


# ───────────────────────────────────────────────────────────── direct ──
async def test_direct_conversation_is_one_per_pair(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    first = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    second = await h.threads.start(ahmed, type="direct", member_ids=[PRIYA])
    assert first["id"] == second["id"] == "dm-101-102"


async def test_cannot_message_another_customer(h):
    priya = await h.actor(PRIYA)
    with pytest.raises(Invalid) as err:
        await h.threads.start(priya, type="direct", member_ids=[OTHER])
    assert err.value.code == "ineligible_people"
    assert err.value.extra["people"] == [{"userId": OTHER, "reason": "not_a_colleague"}]


async def test_cannot_message_inactive_user(h):
    priya = await h.actor(PRIYA)
    with pytest.raises(Invalid) as err:
        await h.threads.start(priya, type="direct", member_ids=[FORMER])
    assert err.value.extra["people"][0]["reason"] == "inactive"


async def test_outsider_cannot_see_or_write_and_cannot_tell_it_exists(h):
    priya, lina, other = await h.actor(PRIYA), await h.actor(LINA), await h.actor(OTHER)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    for outsider in (lina, other):
        with pytest.raises(NotFound):
            await h.threads.get_thread(outsider, thread["id"])
        with pytest.raises(NotFound):
            await h.threads.list_messages(outsider, thread["id"])
        with pytest.raises(NotFound):
            await send(h, outsider, thread["id"])


async def test_malformed_thread_id_is_not_found(h):
    priya = await h.actor(PRIYA)
    with pytest.raises(NotFound):
        await h.threads.get_thread(priya, "../../etc/passwd")


# ──────────────────────────────────────────────────────── send / read ──
async def test_message_makes_conversation_unread_for_recipient_only(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    await send(h, priya, thread["id"], "Can you check ST2?")

    mine = (await h.threads.inbox(priya))["items"][0]
    theirs = (await h.threads.inbox(ahmed))["items"][0]
    assert mine["unread"] is False
    assert theirs["unread"] is True and theirs["lastPreview"] == "Can you check ST2?"
    assert (await h.threads.activity(ahmed))["unreadConversations"] == 1

    await h.threads.mark_read(ahmed, thread["id"], theirs["seq"])
    assert (await h.threads.inbox(ahmed))["items"][0]["unread"] is False
    assert (await h.threads.activity(ahmed))["unreadConversations"] == 0


async def test_retried_send_is_stored_once(h):
    priya = await h.actor(PRIYA)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    mid = h.ulid()
    a = await h.threads.post_message(priya, thread["id"], message_id=mid, text="once")
    b = await h.threads.post_message(priya, thread["id"], message_id=mid, text="once")
    assert a == b
    messages = (await h.threads.list_messages(priya, thread["id"]))["messages"]
    assert [m["id"] for m in messages] == [mid]


async def test_client_message_id_must_be_a_ulid(h):
    priya = await h.actor(PRIYA)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    with pytest.raises(Invalid):
        await h.threads.post_message(priya, thread["id"], message_id="not-a-ulid", text="x")


async def test_text_is_cleaned_of_bidi_overrides_and_controls(h):
    priya = await h.actor(PRIYA)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    msg = await send(h, priya, thread["id"], "pay " + chr(0x202E) + "evil" + chr(0x202C) + " now" + chr(0x07))
    assert msg["text"] == "pay evil now"


async def test_live_events_go_to_members_only(h):
    priya = await h.actor(PRIYA)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    await send(h, priya, thread["id"])
    recipients = {uid for _, uid, e in h.realtime.sent if e["type"] == "message.created"}
    assert recipients == {PRIYA, AHMED}


async def test_pagination_across_segments(h):
    priya = await h.actor(PRIYA)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    for i in range(12):                       # segment size is 5 in tests
        await send(h, priya, thread["id"], f"m{i}")
    page = await h.threads.list_messages(priya, thread["id"], limit=5)
    assert [m["text"] for m in page["messages"]] == ["m7", "m8", "m9", "m10", "m11"]
    assert page["hasMore"] is True
    older = await h.threads.list_messages(priya, thread["id"], before_seq=page["messages"][0]["seq"], limit=10)
    assert [m["text"] for m in older["messages"]] == [f"m{i}" for i in range(7)]
    assert older["hasMore"] is False


async def test_delete_own_message_only(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    msg = await send(h, priya, thread["id"], "oops")
    with pytest.raises(Forbidden):
        await h.threads.delete_message(ahmed, thread["id"], msg["id"])
    await h.threads.delete_message(priya, thread["id"], msg["id"])
    stored = (await h.threads.list_messages(ahmed, thread["id"]))["messages"][-1]
    assert stored["deleted"] is True and stored["text"] == ""


async def test_muted_conversations_do_not_ring_the_bell(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    await h.threads.set_muted(ahmed, thread["id"], True)
    await send(h, priya, thread["id"])
    assert (await h.threads.activity(ahmed))["unreadConversations"] == 0


async def test_send_rate_limit():
    h = Harness(limits={"message.send": Limit(capacity=2, per_second=0.0001)})
    priya = await h.actor(PRIYA)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    await send(h, priya, thread["id"])
    await send(h, priya, thread["id"])
    with pytest.raises(RateLimited):
        await send(h, priya, thread["id"])


# ──────────────────────────────────────────────── groups and members ──
async def test_group_add_member_leaves_visible_note_and_shares_history(h):
    priya, lina = await h.actor(PRIYA), await h.actor(LINA)
    thread = await h.threads.start(priya, type="group", member_ids=[AHMED], title="Range plan")
    await send(h, priya, thread["id"], "before Lina joined")
    await h.threads.add_members(priya, thread["id"], [LINA])

    history = (await h.threads.list_messages(lina, thread["id"]))["messages"]
    assert history[0]["text"] == "before Lina joined"
    assert history[-1]["kind"] == "system" and "added Lina Fernandes" in history[-1]["text"]
    assert any(n["kind"] == "added_to_conversation" for n in (await h.threads.notifications(lina))["items"])


async def test_system_messages_do_not_make_conversations_unread(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    thread = await h.threads.start(priya, type="group", member_ids=[AHMED])
    await h.threads.add_members(priya, thread["id"], [LINA])
    entry = (await h.threads.inbox(ahmed))["items"][0]
    assert entry["unread"] is False and "added" in entry["lastPreview"]


async def test_leaving(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    group = await h.threads.start(priya, type="group", member_ids=[AHMED, LINA])
    await h.threads.leave(ahmed, group["id"])
    with pytest.raises(NotFound):
        await h.threads.get_thread(ahmed, group["id"])
    assert (await h.threads.inbox(ahmed))["items"] == []
    direct = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    with pytest.raises(Invalid):
        await h.threads.leave(ahmed, direct["id"])


async def test_idempotent_group_creation(h):
    priya = await h.actor(PRIYA)
    a = await h.threads.start(priya, type="group", member_ids=[AHMED], idempotency_key="create-abc-123")
    b = await h.threads.start(priya, type="group", member_ids=[AHMED], idempotency_key="create-abc-123")
    assert a["id"] == b["id"]
    assert len((await h.threads.inbox(priya))["items"]) == 1


# ───────────────────────────────────────────────────────── context ──
async def test_context_conversation_requires_recipient_access(h):
    priya = await h.actor(PRIYA)
    token = h.context(priya, report_row_id=12)          # Ahmed has report 11 only
    with pytest.raises(Invalid) as err:
        await h.threads.start(priya, type="context", member_ids=[AHMED], context_token=token)
    assert err.value.extra["people"] == [{"userId": AHMED, "reason": "no_report_access"}]


async def test_context_conversation_shows_snapshot_to_entitled_members(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    thread = await h.threads.start(priya, type="context", member_ids=[AHMED], context_token=h.context(priya),
                                   message_text="Why is ST2 down?", message_id=h.ulid())
    view = await h.threads.get_thread(ahmed, thread["id"])
    assert view["context"]["visible"] is True
    assert view["context"]["snapshot"]["value"] == "USD 55.3K"
    assert view["title"] == "ST2 sales 18% under target"


async def test_sender_without_access_cannot_attach(h):
    lina = await h.actor(LINA)                            # Lina has report 12 only
    with pytest.raises(Forbidden):
        await h.threads.start(lina, type="context", member_ids=[MARCUS], context_token=h.context(lina, report_row_id=11))


async def test_losing_access_hides_item_and_title_but_keeps_conversation(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    thread = await h.threads.start(priya, type="context", member_ids=[AHMED], context_token=h.context(priya),
                                   message_text="Why is ST2 down?", message_id=h.ulid())

    h.directory._grants[AHMED] = set()                    # an admin removes Ahmed's report access

    view = await h.threads.get_thread(ahmed, thread["id"])
    assert view["context"] == {"visible": False, "kind": "insight",
                               "message": "The item this conversation is about is no longer available to you."}
    assert "snapshot" not in view["context"]
    assert view["title"] == HIDDEN_TITLE
    assert (await h.threads.inbox(ahmed))["items"][0]["title"] == HIDDEN_TITLE
    assert (await h.threads.list_messages(ahmed, thread["id"]))["messages"][-1]["text"] == "Why is ST2 down?"
    audit = await h.store.list_audit(1, h.clock.now().date())
    assert any(e.action == "context.hidden" and e.actorId == AHMED for e in audit)


async def test_forged_or_foreign_context_is_rejected(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    token = h.context(priya)
    body, mac = token.split(".")
    with pytest.raises(Invalid) as err:
        await h.threads.start(priya, type="context", member_ids=[MARCUS], context_token=body + "." + mac[::-1])
    assert err.value.extra["reason"] == "signature"
    with pytest.raises(Invalid) as err:                   # a token minted for Priya, replayed by Ahmed
        await h.threads.start(ahmed, type="context", member_ids=[MARCUS], context_token=token)
    assert err.value.extra["reason"] == "audience"


async def test_expired_context_is_rejected(h):
    priya = await h.actor(PRIYA)
    token = h.context(priya)
    h.clock.advance(minutes=10)
    with pytest.raises(Invalid) as err:
        await h.threads.start(priya, type="context", member_ids=[MARCUS], context_token=token)
    assert err.value.extra["reason"] == "expiry"


async def test_adding_member_to_context_conversation_checks_access(h):
    priya = await h.actor(PRIYA)
    thread = await h.threads.start(priya, type="context", member_ids=[AHMED], context_token=h.context(priya))
    with pytest.raises(Invalid):
        await h.threads.add_members(priya, thread["id"], [LINA])   # no access to report 11
    await h.threads.add_members(priya, thread["id"], [MARCUS])


async def test_tenant_only_anchor_allows_any_active_colleague(h):
    priya = await h.actor(PRIYA)
    token = h.context(priya, resolution="tenant-only", report_row_id=None)
    thread = await h.threads.start(priya, type="context", member_ids=[LINA], context_token=token)
    assert (await h.threads.get_thread(await h.actor(LINA), thread["id"]))["context"]["visible"] is True


async def test_directory_outage_hides_items_rather_than_showing_them(h):
    priya, ahmed = await h.actor(PRIYA), await h.actor(AHMED)
    thread = await h.threads.start(priya, type="context", member_ids=[AHMED], context_token=h.context(priya))

    from messaging.directory import DirectoryUnavailable

    async def down(*a, **k):
        raise DirectoryUnavailable("down")
    h.directory.check_anchor = down
    assert (await h.threads.get_thread(ahmed, thread["id"]))["context"]["visible"] is False


async def test_people_search_marks_ineligible(h):
    priya = await h.actor(PRIYA)
    anchor = await h.threads.anchor_for(priya, context_token=h.context(priya), thread_id=None)
    decisions = await h.services.access.anchor_decisions(priya.customer_id, [AHMED, LINA], anchor)
    assert decisions[AHMED].allowed and not decisions[LINA].allowed


async def test_audit_never_contains_message_text(h):
    priya = await h.actor(PRIYA)
    thread = await h.threads.start(priya, type="direct", member_ids=[AHMED])
    await send(h, priya, thread["id"], "confidential margin figure 42%")
    for event in await h.store.list_audit(1, h.clock.now().date()):
        assert "confidential" not in event.model_dump_json()
