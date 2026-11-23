"""
🎬 Funny Video Bot — Telethon (MTProto) + Firebase Firestore, for Render's free web service.

Why Telethon: from Render's shared IPs Telegram's HTTP Bot API (api.telegram.org) keeps answering
"Flood control exceeded". MTProto talks to Telegram's data centres directly and is not affected.

ENV: API_ID, API_HASH (my.telegram.org), BOT_TOKEN (@BotFather), OWNER_ID,
     FIREBASE_CREDENTIALS (service-account JSON text, or base64 of it)
"""
import asyncio
import base64
import csv
import html
import io
import json
import logging
import os
import re
import time

import aiohttp
from aiohttp import web
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.oauth2 import service_account
from telethon import TelegramClient, errors, events, functions, types, utils
from telethon.extensions import html as tl_html
from telethon.sessions import StringSession

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO)
log = logging.getLogger("funnybot")

# ─────────────────────────── CONFIG ───────────────────────────
for _k in ("API_ID", "API_HASH", "BOT_TOKEN", "OWNER_ID"):
    if not os.environ.get(_k):
        raise SystemExit(f"{_k} set karo (Render Environment ya .env file).")
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
PORT = int(os.environ.get("PORT", "10000"))


def load_firebase_info():
    raw = (os.environ.get("FIREBASE_CREDENTIALS") or os.environ.get("FIREBASE_CREDENTIALS_B64") or "").strip()
    if raw:
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "'\"":
            raw = raw[1:-1].strip()
        if not raw.startswith("{"):
            raw = base64.b64decode(raw).decode("utf-8").strip()
        return json.loads(raw, strict=False)
    path = os.environ.get("FIREBASE_CREDENTIALS_FILE")
    if path:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    raise SystemExit("FIREBASE_CREDENTIALS set karo (service-account JSON ya uska base64).")


# In-memory session: Render's free plan has no disk. The bot logs in with the token on every start.
client = TelegramClient(StringSession(), API_ID, API_HASH)
client.parse_mode = "html"

# ─────────────────────────── FIRESTORE + CACHE ───────────────────────────
# Free Firestore quota is small (50k reads / 20k writes a day) -> everything is cached in memory and
# written through. Run a single instance only.
fs: firestore.AsyncClient = None
S = {}            # settings
ADMINS = {}       # uid -> {"name", "perms": set}
CATS = []         # [{"id","name","style","position"}]
VIDS = {}         # category id -> [{"id","file_id","kind","added_at"}]  (lazy)
CHANNELS = {}     # chat_id -> {"title","link","channel_id","access_hash"}
BANNED = set()
KNOWN = {}        # uid -> (name, username, access_hash) already saved by this process
HASH = {}         # uid -> access_hash (Telethon can't reach a user without it after a restart)
PENDING = []      # [{"doc","chat_id","message_id","at"}]
STATE = {}        # uid -> admin "waiting for input" state
_COUNT = {}

DEFAULTS = {
    "welcome": "🎉 <b>Welcome {name}!</b>\n\nNeeche se apni pasand ka funny video chuno 👇😂",
    "brand": "",
    "timer": "5",       # minutes, 0 = off
    "protect": "1",     # 1 = no forward / no save
}


def make_client():
    info = load_firebase_info()
    creds = service_account.Credentials.from_service_account_info(info)
    return firestore.AsyncClient(project=info["project_id"], credentials=creds)


async def load_all():
    d = await fs.collection("settings").document("main").get()
    if d.exists:
        S.update(d.to_dict() or {})
    async for a in fs.collection("admins").stream():
        x = a.to_dict() or {}
        ADMINS[int(a.id)] = {"name": x.get("name") or a.id, "perms": set(x.get("perms", []))}
    async for c in fs.collection("categories").stream():
        x = c.to_dict() or {}
        CATS.append({"id": c.id, "name": x.get("name", "?"), "style": x.get("style", ""), "position": x.get("position", 0)})
    sort_cats()
    async for c in fs.collection("channels").stream():
        x = c.to_dict() or {}
        if x.get("channel_id") and x.get("access_hash"):
            CHANNELS[int(c.id)] = {"title": x.get("title", c.id), "link": x.get("link"),
                                   "channel_id": int(x["channel_id"]), "access_hash": int(x["access_hash"])}
    async for u in fs.collection("users").where(filter=FieldFilter("banned", "==", True)).stream():
        BANNED.add(int(u.id))
    async for p in fs.collection("pending_deletes").stream():
        x = p.to_dict() or {}
        PENDING.append({"doc": p.reference, "chat_id": x["chat_id"], "message_id": x["message_id"], "at": x["at"]})
        if x.get("hash"):
            HASH.setdefault(int(x["chat_id"]), int(x["hash"]))
    log.info("Loaded: %d admins, %d buttons, %d channels, %d banned, %d pending deletes",
             len(ADMINS), len(CATS), len(CHANNELS), len(BANNED), len(PENDING))


def sort_cats():
    CATS.sort(key=lambda c: (c["position"], c["id"]))


def get(key):
    return S.get(key, DEFAULTS[key])


async def put(key, value):
    S[key] = value
    await fs.collection("settings").document("main").set({key: value}, merge=True)


async def count(coll, field=None, value=None, ttl=30):
    key = (coll, field, value)
    c = _COUNT.get(key)
    if c and time.time() - c[0] < ttl:
        return c[1]
    q = fs.collection(coll)
    if field:
        q = q.where(filter=FieldFilter(field, "==", value))
    res = await q.count().get()
    n = int(res[0][0].value)
    _COUNT[key] = (time.time(), n)
    return n


# ---- categories (buttons)
def cat_get(cid):
    return next((c for c in CATS if c["id"] == cid), None)


async def create_cat(name):
    pos = int(time.time() * 1000)
    _, ref = await fs.collection("categories").add({"name": name, "style": "primary", "position": pos})
    CATS.append({"id": ref.id, "name": name, "style": "primary", "position": pos})
    sort_cats()
    return ref.id


async def update_cat(cid, **fields):
    c = cat_get(cid)
    if c:
        c.update(fields)
        sort_cats()
        await fs.collection("categories").document(cid).update(fields)


async def move_up(cid):
    i = next((i for i, c in enumerate(CATS) if c["id"] == cid), None)
    if not i:
        return
    a, b = CATS[i - 1], CATS[i]
    pa, pb = a["position"], b["position"]
    if pa == pb:
        pb = pa + 1
    await update_cat(a["id"], position=pb)
    await update_cat(b["id"], position=pa)


async def delete_cat(cid):
    refs = []
    async for v in fs.collection("videos").where(filter=FieldFilter("category_id", "==", cid)).stream():
        refs.append(v.reference)
    for i in range(0, len(refs), 400):
        batch = fs.batch()
        for ref in refs[i:i + 400]:
            batch.delete(ref)
        await batch.commit()
    await fs.collection("categories").document(cid).delete()
    CATS[:] = [c for c in CATS if c["id"] != cid]
    VIDS.pop(cid, None)
    _COUNT.clear()


# ---- videos
async def vids_of(cid):
    if cid not in VIDS:
        items = []
        async for v in fs.collection("videos").where(filter=FieldFilter("category_id", "==", cid)).stream():
            x = v.to_dict() or {}
            items.append({"id": v.id, "file_id": x["file_id"], "kind": x.get("kind", "video"), "added_at": x.get("added_at", 0)})
        items.sort(key=lambda v: v["added_at"])
        VIDS[cid] = items
    return VIDS[cid]


async def add_video(cid, file_id, kind, uid):
    lst = await vids_of(cid)
    now = int(time.time() * 1000)
    _, ref = await fs.collection("videos").add(
        {"category_id": cid, "file_id": file_id, "kind": kind, "added_by": uid, "added_at": now})
    lst.append({"id": ref.id, "file_id": file_id, "kind": kind, "added_at": now})
    _COUNT.clear()
    return len(lst)


async def delete_video(vid, cid):
    await fs.collection("videos").document(vid).delete()
    if cid in VIDS:
        VIDS[cid] = [v for v in VIDS[cid] if v["id"] != vid]
    _COUNT.clear()


# ---- users
def display_name(sender):
    if sender is None:
        return "dost"
    return " ".join(x for x in (getattr(sender, "first_name", None), getattr(sender, "last_name", None)) if x) or "dost"


async def track(event):
    """Save every private-chat user to Firestore on first contact (any message or tap)."""
    if not event.is_private:
        return
    sender = await event.get_sender()
    if sender is None or getattr(sender, "bot", False):
        return
    uid = sender.id
    name = display_name(sender)
    uname = getattr(sender, "username", None) or ""
    h = getattr(sender, "access_hash", None)
    if h:
        HASH[uid] = h
    sig = (name, uname, h)
    if KNOWN.get(uid) == sig:
        return
    ref = fs.collection("users").document(str(uid))
    data = {"name": name, "username": uname, "access_hash": h}
    if uid not in KNOWN:
        snap = await ref.get()
        if not snap.exists:
            data["joined_at"] = int(time.time())
            _COUNT.clear()
    await ref.set(data, merge=True)
    KNOWN[uid] = sig


async def set_ban(tid, flag):
    await fs.collection("users").document(str(tid)).set({"banned": flag}, merge=True)
    (BANNED.add if flag else BANNED.discard)(tid)
    _COUNT.clear()


def peer(uid):
    """InputPeerUser if we know the access hash (works after restarts), else the bare id."""
    h = HASH.get(uid)
    return types.InputPeerUser(uid, h) if h else uid


# ---- admins
async def save_admin(tid):
    a = ADMINS[tid]
    await fs.collection("admins").document(str(tid)).set(
        {"name": a["name"], "perms": sorted(a["perms"]), "added_at": int(time.time())}, merge=True)


# ---- pending deletes (survive restarts / Render sleep)
async def schedule_delete(chat_id, mids, minutes):
    at = int(time.time()) + minutes * 60
    batch = fs.batch()
    for mid in mids:
        ref = fs.collection("pending_deletes").document()
        batch.set(ref, {"chat_id": chat_id, "message_id": mid, "at": at, "hash": HASH.get(chat_id)})
        PENDING.append({"doc": ref, "chat_id": chat_id, "message_id": mid, "at": at})
    await batch.commit()


async def sweeper():
    while True:
        await asyncio.sleep(10)
        try:
            now = int(time.time())
            due = [p for p in PENDING if p["at"] <= now]
            if not due:
                continue
            by_chat = {}
            for p in due:
                PENDING.remove(p)
                by_chat.setdefault(p["chat_id"], []).append(p["message_id"])
            for chat_id, ids in by_chat.items():
                try:
                    await client.delete_messages(peer(chat_id), ids)
                except Exception as e:
                    log.info("auto-delete skipped for %s: %s", chat_id, e)
            for i in range(0, len(due), 400):
                batch = fs.batch()
                for p in due[i:i + 400]:
                    batch.delete(p["doc"])
                await batch.commit()
        except Exception:
            log.exception("sweeper error")


# ─────────────────────────── PERMISSIONS ───────────────────────────
PERMS = {
    "upload": "📤 Upload videos",
    "buttons": "🔘 Manage buttons",
    "videos": "🎞 Manage videos",
    "branding": "🏷 Branding",
    "timer": "⏱ Auto-delete timer",
    "welcome": "👋 Welcome msg",
    "stats": "📊 Stats",
    "broadcast": "📣 Broadcast",
    "ban": "🚫 Ban / Unban",
}
SEC_PERM = {"upload": "upload", "btns": "buttons", "vids": "videos", "brand": "branding",
            "timer": "timer", "welcome": "welcome", "stats": "stats", "bc": "broadcast", "ban": "ban"}
OWNER_ONLY = {"fj", "adm", "protect"}


def is_owner(uid):
    return uid == OWNER_ID


def is_staff(uid):
    return is_owner(uid) or uid in ADMINS


def has_perm(uid, perm):
    return is_owner(uid) or (uid in ADMINS and perm in ADMINS[uid]["perms"])


# ─────────────────────────── BUTTONS (raw Telegram TL types) ───────────────────────────
# Colours (blue / green / red) use the Bot API 9.4 `style` field. They are sent only if the installed
# Telethon knows that field; otherwise buttons are plain (everything else keeps working).
HAS_STYLE = hasattr(types, "KeyboardButtonStyle")
_STYLE_FLAG = {"primary": "bg_primary", "success": "bg_success", "danger": "bg_danger"}


def _style(color):
    if not (HAS_STYLE and color in _STYLE_FLAG):
        return None
    try:
        return types.KeyboardButtonStyle(**{_STYLE_FLAG[color]: True})
    except Exception:
        return None


def _mk(cls, color, *args, **kw):
    st = _style(color)
    if st is not None:
        try:
            return cls(*args, style=st, **kw)
        except TypeError:
            pass
    return cls(*args, **kw)


def B(text, data=None, style=None, url=None):
    """Inline button (callback or url)."""
    if url:
        return _mk(types.KeyboardButtonUrl, style, text, url)
    return _mk(types.KeyboardButtonCallback, style, text, data.encode())


def KB(text, style=None):
    """Bottom (reply) keyboard button."""
    return _mk(types.KeyboardButton, style, text)


def inline(rows):
    rows = [r for r in rows if r]
    if not rows:
        return None
    return types.ReplyInlineMarkup(rows=[types.KeyboardButtonRow(buttons=list(r)) for r in rows])


def reply_markup(rows):
    krows = [types.KeyboardButtonRow(buttons=list(r)) for r in rows]
    for kw in ({"resize": True, "persistent": True, "placeholder": "Neeche se video chuno 👇"}, {"resize": True}):
        try:
            return types.ReplyKeyboardMarkup(rows=krows, **kw)
        except TypeError:
            continue


def back(to="a:home"):
    return [B("⬅️ Back", to)]


def esc(s):
    return html.escape(str(s), quote=False)


def chunk(lst, n=2):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


async def show(event, text, rows):
    try:
        await event.edit(text, buttons=inline(rows))
    except errors.MessageNotModifiedError:
        pass


ADMIN_BTN = "⚙️ Admin Panel"


def menu_markup(uid):
    """Persistent bottom keyboard: one button per video category (+ Admin Panel for staff)."""
    rows_ = chunk([KB(c["name"], c["style"] or None) for c in CATS], 2)
    if is_staff(uid):
        rows_.append([KB(ADMIN_BTN, "primary")])
    if not rows_:
        return types.ReplyKeyboardHide()
    return reply_markup(rows_)


async def panel_text(uid):
    tag = "👑 <b>OWNER PANEL</b>" if is_owner(uid) else "🛡 <b>ADMIN PANEL</b>"
    n_u = await count("users")
    n_v = await count("videos")
    return f"{tag}\n━━━━━━━━━━━━━━\n👥 Users: <b>{n_u}</b>   🎬 Videos: <b>{n_v}</b>\n\nKya karna hai? 👇"


def main_kb(uid):
    items = [
        ("📤 Upload Video", "a:upload", "success", "upload"),
        ("🔘 Buttons", "a:btns", "primary", "buttons"),
        ("🎞 Videos", "a:vids", "primary", "videos"),
        ("🏷 Branding", "a:brand", "primary", "branding"),
        ("⏱ Auto Delete", "a:timer", "primary", "timer"),
        ("👋 Welcome", "a:welcome", "primary", "welcome"),
        ("📊 Stats", "a:stats", None, "stats"),
        ("📣 Broadcast", "a:bc", "success", "broadcast"),
        ("🚫 Ban/Unban", "a:ban", "danger", "ban"),
    ]
    btns = [B(t, d, s) for t, d, s, p in items if has_perm(uid, p)]
    if is_owner(uid):
        btns += [B("📢 Force Join", "a:fj", "primary"), B("👥 Admins", "a:adm", "primary"),
                 B("🔒 Protect", "a:protect", "danger")]
    kb = chunk(btns)
    kb.append([B("🏠 User View", "uhome")])
    return kb


async def cat_kb(prefix, with_count=False):
    btns = []
    for c in CATS:
        label = c["name"]
        if with_count:
            label += f" ({len(await vids_of(c['id']))})"
        btns.append(B(label, f"{prefix}{c['id']}", c["style"] or None))
    return chunk(btns)


# ─────────────────────────── FORCE JOIN / GATE ───────────────────────────
async def is_member(ch, user_peer):
    try:
        res = await client(functions.channels.GetParticipantRequest(
            channel=types.InputChannel(ch["channel_id"], ch["access_hash"]), participant=user_peer))
        return not isinstance(res.participant, (types.ChannelParticipantLeft, types.ChannelParticipantBanned))
    except errors.UserNotParticipantError:
        return False
    except Exception as e:  # bot removed / channel deleted -> don't lock users out
        log.warning("membership check failed in %s: %s", ch.get("title"), e)
        return True


async def missing_channels(event):
    if not CHANNELS:
        return []
    user_peer = await event.get_input_sender()
    return [ch for ch in list(CHANNELS.values()) if not await is_member(ch, user_peer)]


async def gate(event) -> bool:
    uid = event.sender_id
    if is_staff(uid):
        return True
    if uid in BANNED:
        await client.send_message(uid, "🚫 Tumhe is bot se ban kar diya gaya hai.")
        return False
    missing = await missing_channels(event)
    if not missing:
        return True
    kb = [[B(f"📢 Join {c['title']}", url=c["link"], style="primary")] for c in missing if c["link"]]
    kb.append([B("✅ Maine Join Kar Liya", "chk", "success")])
    await client.send_message(uid, "🔐 <b>Pehle ye channels join karo</b>, phir videos unlock honge 👇", buttons=inline(kb))
    return False


# ─────────────────────────── USER SIDE ───────────────────────────
async def send_home(uid, name):
    text = get("welcome").replace("{name}", esc(name))
    if not CATS:
        text += "\n\n<i>Abhi koi video nahi hai, jaldi aayenge! 🎬</i>"
    await client.send_message(uid, text, buttons=menu_markup(uid))


def _rid():
    return int.from_bytes(os.urandom(8), "big", signed=True)


def _msg_id(res):
    if isinstance(res, types.UpdateShortSentMessage):
        return res.id
    ups = getattr(res, "updates", None) or []
    for u in ups:
        if isinstance(u, types.UpdateMessageID):
            return u.id
    for u in ups:
        m = getattr(u, "message", None)
        if m is not None and getattr(m, "id", None):
            return m.id
    return None


async def send_media(uid, v, caption, protect):
    """Send a stored video with protect_content (no forward/save). Returns the new message id."""
    doc = utils.resolve_bot_file_id(v["file_id"])
    if doc is None:
        raise ValueError("saved file_id is not valid")
    text, ents = tl_html.parse(caption) if caption else ("", [])
    res = await client(functions.messages.SendMediaRequest(
        peer=await client.get_input_entity(peer(uid)),
        media=utils.get_input_media(doc),
        message=text,
        random_id=_rid(),
        entities=ents or None,
        noforwards=True if protect else None,
    ))
    return _msg_id(res)


async def deliver(event, cid):
    uid = event.sender_id
    vids = (await vids_of(cid))[:30]
    if not vids:
        await client.send_message(uid, "Is button me abhi koi video nahi hai 😅", buttons=menu_markup(uid))
        return
    if not await gate(event):
        return
    protect = get("protect") == "1"
    minutes = int(get("timer"))
    caption = get("brand")
    sent = []
    for v in vids:
        try:
            mid = await send_media(uid, v, caption, protect)
            if mid:
                sent.append(mid)
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            log.warning("send failed: %s", e)
        await asyncio.sleep(0.3)
    if minutes > 0:
        txt = f"⏳ <i>Ye video <b>{minutes} minute</b> me automatically delete ho jayega.</i>"
    else:
        txt = "😂 Aur chahiye? Neeche se button chuno 👇"
    n = await client.send_message(uid, txt, buttons=menu_markup(uid))
    if minutes > 0 and sent:
        sent.append(n.id)
        await schedule_delete(uid, sent, minutes)


async def menu_text(event, text):
    """A normal user tapped a bottom-keyboard button (it arrives as plain text)."""
    c = next((c for c in CATS if c["name"] == text), None)
    if c:
        await deliver(event, c["id"])
    else:
        await event.respond("🔄 Menu update ho gaya hai, neeche se naya button chuno 👇",
                            buttons=menu_markup(event.sender_id))


# ─────────────────────────── ADMIN PANEL ───────────────────────────
async def open_panel(event):
    uid = event.sender_id
    STATE.pop(uid, None)
    await event.respond(await panel_text(uid), buttons=inline(main_kb(uid)))


def adm_view(uid):
    a = ADMINS.get(uid)
    on = a["perms"] if a else set()
    kb = []
    for k, label in PERMS.items():
        kb.append([B(("✅ " if k in on else "❌ ") + label, f"a:adm:tog:{uid}:{k}", "success" if k in on else "danger")])
    kb.append([B("🗑 Remove Admin", f"a:adm:del:{uid}", "danger")])
    kb.append(back("a:adm"))
    text = f"🛡 <b>Admin:</b> {esc(a['name'] if a else uid)} (<code>{uid}</code>)\n\nPermissions toggle karo 👇"
    return text, kb


async def admin_cb(event, data):
    uid = event.sender_id
    p = data.split(":")
    sec = p[1]
    sub = p[2] if len(p) > 2 else None
    if not is_staff(uid):
        await event.answer("⛔ Access nahi hai", alert=True)
        return
    need = SEC_PERM.get(sec)
    if (sec in OWNER_ONLY and not is_owner(uid)) or (need and not has_perm(uid, need)) \
            or (sec == "stats" and sub == "export" and not is_owner(uid)):
        await event.answer("⛔ Is feature ki permission nahi hai", alert=True)
        return
    await event.answer()
    STATE.pop(uid, None)
    q = event

    # ---- home
    if sec == "home":
        await show(q, await panel_text(uid), main_kb(uid))

    # ---- upload
    elif sec == "upload":
        if sub is None:
            kb = await cat_kb("a:upload:cat:")
            if not kb:
                kb = [[B("➕ Pehle Button Banao", "a:btns:new", "success")]]
            kb.append(back())
            await show(q, "📤 <b>Upload Video</b>\n\nKis button me video daalni hai? 👇", kb)
        elif sub == "cat":
            c = cat_get(p[3])
            if not c:
                await show(q, "Button nahi mila.", [back()])
                return
            STATE[uid] = ("upload", c["id"])
            await show(q, f"📤 Button: <b>{esc(c['name'])}</b>\n\nAb video(s) bhejo (ek ke baad ek). Khatam hone par <b>Done</b> dabao.",
                       [[B("✅ Done", "a:home", "success")]])

    # ---- buttons
    elif sec == "btns":
        if sub is None:
            kb = await cat_kb("a:btns:view:", True)
            kb.append([B("➕ New Button", "a:btns:new", "success")])
            kb.append(back())
            await show(q, "🔘 <b>Buttons</b>\n\nEdit karne ke liye button chuno 👇", kb)
        elif sub == "new":
            STATE[uid] = ("newbtn",)
            await show(q, "➕ Naye button ka <b>naam</b> bhejo (emoji bhi laga sakte ho):", [back("a:btns")])
        elif sub in ("view", "col", "up", "ren", "del", "delok"):
            cid = p[3]
            c = cat_get(cid)
            if not c:
                await show(q, "Button nahi mila.", [back("a:btns")])
                return
            if sub == "ren":
                STATE[uid] = ("rename", cid)
                await show(q, f"✏️ <b>{esc(c['name'])}</b> ka naya naam bhejo:", [back(f"a:btns:view:{cid}")])
                return
            if sub == "del":
                await show(q, f"⚠️ <b>{esc(c['name'])}</b> aur uske saare videos delete ho jayenge. Pakka?",
                           [[B("🗑 Haan, Delete", f"a:btns:delok:{cid}", "danger"), B("❌ Cancel", f"a:btns:view:{cid}", "success")]])
                return
            if sub == "delok":
                await delete_cat(cid)
                await show(q, "🗑 Button delete ho gaya.", [back("a:btns")])
                return
            if sub == "col":
                order = ["primary", "success", "danger", ""]
                cur = c["style"] if c["style"] in order else ""
                await update_cat(cid, style=order[(order.index(cur) + 1) % len(order)])
            if sub == "up":
                await move_up(cid)
            c = cat_get(cid)
            n = len(await vids_of(cid))
            colors = {"primary": "🔵 Blue", "success": "🟢 Green", "danger": "🔴 Red", "": "⚪ Default"}
            kb = [
                [B("✏️ Rename", f"a:btns:ren:{cid}", "primary"), B("🎨 Color", f"a:btns:col:{cid}", "primary")],
                [B("⬆️ Move Up", f"a:btns:up:{cid}"), B("🗑 Delete", f"a:btns:del:{cid}", "danger")],
                back("a:btns"),
            ]
            await show(q, f"🔘 <b>{esc(c['name'])}</b>\n🎬 Videos: {n}\n🎨 Color: {colors.get(c['style'] or '')}", kb)

    # ---- videos
    elif sec == "vids":
        if sub is None:
            kb = await cat_kb("a:vids:cat:", True)
            kb.append(back())
            await show(q, "🎞 <b>Videos</b>\n\nKis button ke videos dekhne hain? 👇", kb)
        elif sub in ("cat", "del"):
            if sub == "del":
                cid = p[4]
                await delete_video(p[3], cid)
            else:
                cid = p[3]
            c = cat_get(cid)
            vs = (await vids_of(cid))[:30]
            kb = chunk([B(f"🗑 Video #{i}", f"a:vids:del:{v['id']}:{cid}", "danger") for i, v in enumerate(vs, 1)], 3)
            kb.append(back("a:vids"))
            await show(q, f"🎞 <b>{esc(c['name'] if c else '?')}</b> — {len(vs)} videos\n\nDelete karne ke liye dabao 👇", kb)

    # ---- branding
    elif sec == "brand":
        if sub == "set":
            STATE[uid] = ("brand",)
            await show(q, "🏷 Naya <b>branding text</b> bhejo (bold/italic/links sab chalega).\nYe har video ke caption me niche lagega.", [back("a:brand")])
        elif sub == "clr":
            await put("brand", "")
            await show(q, "🗑 Branding hata di gayi.", [back("a:brand")])
        else:
            cur = get("brand")
            kb = [[B("✏️ Set / Edit", "a:brand:set", "success"), B("🗑 Remove", "a:brand:clr", "danger")], back()]
            await show(q, "🏷 <b>Branding</b>\n\n<b>Current:</b>\n" + (cur or "<i>— koi nahi —</i>"), kb)

    # ---- timer
    elif sec == "timer":
        if sub == "set":
            await put("timer", p[3])
        elif sub == "custom":
            STATE[uid] = ("timer",)
            await show(q, "⏱ Minutes bhejo (0 = auto-delete band, max 1440):", [back("a:timer")])
            return
        cur = int(get("timer"))
        presets = [1, 2, 5, 10, 30]
        btns = [B(("✅ " if cur == m else "") + f"{m} min", f"a:timer:set:{m}", "success" if cur == m else "primary") for m in presets]
        kb = chunk(btns, 3)
        kb.append([B("✍️ Custom", "a:timer:custom", "primary"), B("🚫 Off", "a:timer:set:0", "danger")])
        kb.append(back())
        await show(q, f"⏱ <b>Auto Delete</b>\n\nAbhi: <b>{'OFF' if cur == 0 else str(cur) + ' minute'}</b>", kb)

    # ---- protect (owner)
    elif sec == "protect":
        if sub == "tog":
            await put("protect", "0" if get("protect") == "1" else "1")
        on = get("protect") == "1"
        kb = [[B("🔴 Turn OFF" if on else "🟢 Turn ON", "a:protect:tog", "danger" if on else "success")], back()]
        await show(q, f"🔒 <b>Forward / Save Protection</b>\n\nStatus: <b>{'ON ✅' if on else 'OFF ❌'}</b>\n\nON hone par users video forward, save ya copy nahi kar sakte.", kb)

    # ---- force join (owner)
    elif sec == "fj":
        if sub == "add":
            STATE[uid] = ("fjadd",)
            await show(q, "📢 Channel ka <b>@username</b>, link ya ID bhejo — ya channel ki koi post yahan forward karo.\n\n⚠️ Bot ko pehle us channel me <b>admin</b> bana do.", [back("a:fj")])
            return
        if sub == "del":
            cid = int(p[3])
            CHANNELS.pop(cid, None)
            await fs.collection("channels").document(str(cid)).delete()
        kb = [[B(f"🗑 {c['title']}", f"a:fj:del:{cid}", "danger")] for cid, c in CHANNELS.items()]
        kb.append([B("➕ Add Channel", "a:fj:add", "success")])
        kb.append(back())
        await show(q, f"📢 <b>Force Join</b> — {len(CHANNELS)} channel(s)\n\nRemove karne ke liye channel pe dabao.", kb)

    # ---- admins (owner)
    elif sec == "adm":
        if sub == "add":
            STATE[uid] = ("admadd",)
            await show(q, "👥 Naye admin ki <b>Telegram user ID</b> bhejo.\n(ID ke liye wo @userinfobot use kar sakta hai; usne bot ko ek baar /start kiya hona chahiye)", [back("a:adm")])
        elif sub == "view":
            text, kb = adm_view(int(p[3]))
            await show(q, text, kb)
        elif sub == "tog":
            tid, perm = int(p[3]), p[4]
            if tid in ADMINS and perm in PERMS:
                ADMINS[tid]["perms"] ^= {perm}
                await save_admin(tid)
            text, kb = adm_view(tid)
            await show(q, text, kb)
        elif sub == "del":
            tid = int(p[3])
            ADMINS.pop(tid, None)
            await fs.collection("admins").document(str(tid)).delete()
            await show(q, "🗑 Admin hata diya gaya.", [back("a:adm")])
        else:
            kb = [[B(f"🛡 {a['name']}", f"a:adm:view:{tid}", "primary")] for tid, a in ADMINS.items()]
            kb.append([B("➕ Add Admin", "a:adm:add", "success")])
            kb.append(back())
            await show(q, f"👥 <b>Admins</b> — {len(ADMINS)}\n\n👑 Owner ko koi hata nahi sakta.", kb)

    # ---- stats
    elif sec == "stats":
        if sub == "export":
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["user_id", "name", "username", "joined_at_utc", "banned"])
            n = 0
            async for d in fs.collection("users").stream():
                x = d.to_dict() or {}
                joined = time.strftime("%Y-%m-%d %H:%M", time.gmtime(x["joined_at"])) if x.get("joined_at") else ""
                w.writerow([d.id, x.get("name", ""), x.get("username") or "", joined, int(bool(x.get("banned")))])
                n += 1
            f = io.BytesIO(buf.getvalue().encode("utf-8-sig"))
            f.name = "users.csv"
            await client.send_file(uid, f, caption=f"👥 {n} users", force_document=True)
            return
        n_users = await count("users", ttl=0)
        n_vid = await count("videos", ttl=0)
        text = (f"📊 <b>Statistics</b>\n━━━━━━━━━━━━━━\n👥 Users: <b>{n_users}</b>\n🚫 Banned: <b>{len(BANNED)}</b>\n"
                f"🔘 Buttons: <b>{len(CATS)}</b>\n🎬 Videos: <b>{n_vid}</b>\n🛡 Admins: <b>{len(ADMINS)}</b>\n📢 Force-join: <b>{len(CHANNELS)}</b>")
        kb = [[B("📥 Export Users (CSV)", "a:stats:export", "success")]] if is_owner(uid) else []
        kb.append(back())
        await show(q, text, kb)

    # ---- broadcast
    elif sec == "bc":
        STATE[uid] = ("bc",)
        await show(q, "📣 Jo message sab users ko bhejna hai wo bhejo (text / photo / video, kuch bhi).", [back()])

    # ---- ban
    elif sec == "ban":
        if sub in ("b", "u"):
            STATE[uid] = ("ban" if sub == "b" else "unban",)
            await show(q, "User ki <b>ID</b> bhejo:", [back("a:ban")])
        else:
            await show(q, "🚫 <b>Ban / Unban</b>", [[B("🚫 Ban", "a:ban:b", "danger"), B("✅ Unban", "a:ban:u", "success")], back()])

    # ---- welcome
    elif sec == "welcome":
        if sub == "set":
            STATE[uid] = ("welcome",)
            await show(q, "👋 Naya welcome message bhejo. User ka naam lagane ke liye <code>{name}</code> likho.", [back("a:welcome")])
        elif sub == "rst":
            await put("welcome", DEFAULTS["welcome"])
            await show(q, "♻️ Welcome message reset ho gaya.", [back("a:welcome")])
        else:
            kb = [[B("✏️ Edit", "a:welcome:set", "success"), B("♻️ Reset", "a:welcome:rst", "danger")], back()]
            await show(q, "👋 <b>Welcome Message</b>\n\n" + get("welcome"), kb)


# ─────────────────────────── ADMIN INPUT (messages) ───────────────────────────
def media_of(m):
    """(kind, bot-api-style file id) for a video / gif / video-document message, else (None, None)."""
    kind = None
    if m.gif:
        kind = "animation"
    elif m.video:
        kind = "video"
    elif m.document and (m.document.mime_type or "").startswith("video"):
        kind = "document"
    if not kind:
        return None, None
    return kind, utils.pack_bot_file_id(m.media)


async def do_broadcast(admin_uid, msg):
    ok = fail = 0
    try:
        async for u in fs.collection("users").stream():
            x = u.to_dict() or {}
            if x.get("banned"):
                continue
            h = x.get("access_hash")
            if not h:
                fail += 1
                continue
            for _ in range(2):
                try:
                    await client.send_message(types.InputPeerUser(int(u.id), int(h)), msg)
                    ok += 1
                    break
                except errors.FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 1)
                except Exception:
                    fail += 1
                    break
            await asyncio.sleep(0.05)
    except Exception:
        log.exception("broadcast error")
    await client.send_message(peer(admin_uid), f"📣 <b>Broadcast complete</b>\n✅ Sent: {ok}\n❌ Failed: {fail}")


async def handle_state(event, st):
    uid = event.sender_id
    msg = event.message
    text = (event.raw_text or "").strip()
    kind = st[0]

    async def reply(t, rows=None):
        await event.respond(t, buttons=inline(rows) if rows else None)

    if kind == "upload":
        if not has_perm(uid, "upload"):
            return
        mk, fid = media_of(msg)
        if not fid:
            await reply("❌ Sirf video bhejo (ya Done dabao).")
            return
        n = await add_video(st[1], fid, mk, uid)
        await reply(f"✅ Video save ho gayi! (is button me total {n})\nAur bhejo ya Done dabao.",
                    [[B("✅ Done", "a:home", "success")]])

    elif kind == "newbtn":
        if not has_perm(uid, "buttons") or not text:
            return
        cid = await create_cat(text[:40])
        STATE.pop(uid, None)
        await reply("✅ Button ban gaya! Ab isme video upload karo 👇",
                    [[B("📤 Upload Here", f"a:upload:cat:{cid}", "success")], back()])

    elif kind == "rename":
        if not has_perm(uid, "buttons") or not text:
            return
        await update_cat(st[1], name=text[:40])
        STATE.pop(uid, None)
        await reply("✅ Naam badal gaya!", [back("a:btns")])

    elif kind == "brand":
        if not has_perm(uid, "branding") or not text:
            return
        await put("brand", msg.text[:900])      # msg.text keeps bold/italic/links as HTML
        STATE.pop(uid, None)
        await reply("✅ Branding save ho gayi!", [back("a:brand")])

    elif kind == "timer":
        if not has_perm(uid, "timer") or not text.isdigit() or int(text) > 1440:
            await reply("❌ 0 se 1440 ke beech number bhejo.")
            return
        await put("timer", str(int(text)))
        STATE.pop(uid, None)
        await reply("✅ Timer set ho gaya!", [back("a:timer")])

    elif kind == "welcome":
        if not has_perm(uid, "welcome") or not text:
            return
        await put("welcome", msg.text)
        STATE.pop(uid, None)
        await reply("✅ Welcome message save ho gaya!", [back("a:welcome")])

    elif kind == "bc":
        if not has_perm(uid, "broadcast"):
            return
        STATE.pop(uid, None)
        await reply("📣 Broadcast shuru ho gaya, khatam hone par report milegi.")
        asyncio.create_task(do_broadcast(uid, msg))

    elif kind in ("ban", "unban"):
        if not has_perm(uid, "ban") or not text.isdigit():
            await reply("❌ Sirf numeric user ID bhejo.")
            return
        tid = int(text)
        if is_owner(tid):
            await reply("👑 Owner ko ban nahi kar sakte.")
            return
        await set_ban(tid, kind == "ban")
        STATE.pop(uid, None)
        await reply("🚫 Banned." if kind == "ban" else "✅ Unbanned.", [back("a:ban")])

    elif kind == "fjadd":
        if not is_owner(uid):
            return
        ent, ref = None, None
        fwd = msg.forward
        if fwd is not None:
            try:
                ent = await fwd.get_chat()
            except Exception:
                ent = None
        if ent is None and text:
            m = re.search(r"(?:t\.me|telegram\.me)/([A-Za-z0-9_]{4,})/?$", text)
            if m:
                ref = m.group(1)
            elif re.fullmatch(r"-?\d+", text):
                ref = int(text)
            elif text.startswith("@"):
                ref = text[1:]
        if ent is None:
            if ref is None:
                await reply("❌ Samajh nahi aaya. Channel ki post forward karo ya @username / ID bhejo.")
                return
            try:
                ent = await client.get_entity(ref)
            except Exception:
                await reply("❌ Channel nahi mila. Bot ko channel me admin banao, phir uski post forward karo.")
                return
        if not isinstance(ent, types.Channel):
            await reply("❌ Sirf channel ya supergroup add ho sakta hai.")
            return
        try:
            res = await client(functions.channels.GetParticipantRequest(channel=ent, participant=types.InputPeerSelf()))
            is_admin = isinstance(res.participant, (types.ChannelParticipantAdmin, types.ChannelParticipantCreator))
        except Exception:
            is_admin = False
        if not is_admin:
            await reply(f"❌ Bot <b>{esc(ent.title)}</b> me admin nahi hai. Pehle admin banao, phir dubara bhejo.")
            return
        if ent.username:
            link = f"https://t.me/{ent.username}"
        else:
            try:
                inv = await client(functions.messages.ExportChatInviteRequest(peer=ent))
                link = inv.link
            except Exception:
                await reply(f"❌ <b>{esc(ent.title)}</b> private hai: bot ko <b>Invite Users</b> admin right do, phir dubara bhejo.")
                return
        chat_id = utils.get_peer_id(ent)
        info = {"title": ent.title, "link": link, "channel_id": ent.id, "access_hash": ent.access_hash}
        CHANNELS[chat_id] = info
        await fs.collection("channels").document(str(chat_id)).set(info)
        STATE.pop(uid, None)
        await reply(f"✅ <b>{esc(ent.title)}</b> force-join me add ho gaya!", [back("a:fj")])

    elif kind == "admadd":
        if not is_owner(uid) or not text.isdigit():
            await reply("❌ Sirf numeric user ID bhejo.")
            return
        tid = int(text)
        if is_owner(tid):
            await reply("👑 Owner already sabse upar hai.")
            return
        if tid not in ADMINS:
            name = str(tid)
            snap = await fs.collection("users").document(str(tid)).get()
            if snap.exists:
                name = (snap.to_dict() or {}).get("name") or name
            ADMINS[tid] = {"name": name, "perms": {"upload"}}
            await save_admin(tid)
        STATE.pop(uid, None)
        t, kb = adm_view(tid)
        await reply("✅ Admin add ho gaya! Ab permissions set karo.\n\n" + t, kb)


# ─────────────────────────── EVENT HANDLERS ───────────────────────────
async def handle_message(event):
    uid = event.sender_id
    text = (event.raw_text or "").strip()
    try:
        await track(event)
    except Exception:
        log.exception("track failed")

    if text.startswith("/start"):
        STATE.pop(uid, None)
        if await gate(event):
            await send_home(uid, display_name(await event.get_sender()))
        return
    if text.startswith("/admin") or (text == ADMIN_BTN and is_staff(uid)):
        if is_staff(uid):
            await open_panel(event)
        return
    if text.startswith("/cancel"):
        STATE.pop(uid, None)
        await event.respond("❎ Cancel ho gaya.", buttons=menu_markup(uid))
        return
    st = STATE.get(uid)
    if st and is_staff(uid):
        await handle_state(event, st)
        return
    if text:
        await menu_text(event, text)


@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def on_message(event):
    try:
        await handle_message(event)
    except Exception:
        log.exception("message handler error")


@client.on(events.CallbackQuery())
async def on_callback(event):
    try:
        data = event.data.decode("utf-8", "ignore")
        try:
            await track(event)
        except Exception:
            log.exception("track failed")
        uid = event.sender_id
        if data.startswith("a:"):
            await admin_cb(event, data)
        elif data.startswith("v:"):
            cid = data.split(":", 1)[1]
            if not await vids_of(cid):
                await event.answer("Is button me abhi koi video nahi hai 😅", alert=True)
                return
            await event.answer("🎬 Video aa raha hai...")
            await deliver(event, cid)
        elif data == "chk":
            if await missing_channels(event):
                await event.answer("❌ Abhi tak sab channels join nahi kiye!", alert=True)
                return
            await event.answer("✅ Verified!")
            try:
                await event.delete()
            except Exception:
                pass
            await send_home(uid, display_name(await event.get_sender()))
        elif data == "uhome":
            await event.answer()
            await send_home(uid, display_name(await event.get_sender()))
        else:
            await event.answer()
    except errors.MessageNotModifiedError:
        pass
    except Exception:
        log.exception("callback handler error")


# ─────────────────────────── WEB SERVER + MAIN ───────────────────────────
async def start_web():
    """Render web services must listen on $PORT; this also gives uptime pingers something to hit."""
    async def health(_request):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Health server listening on :%s", PORT)


async def keep_alive():
    """Ping our own public URL so Render's free plan doesn't spin the service down (15 min idle)."""
    base = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("KEEPALIVE_URL")
    if not base:
        log.info("No RENDER_EXTERNAL_URL / KEEPALIVE_URL set - self-ping disabled.")
        return
    url = base.rstrip("/") + "/health"
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        while True:
            await asyncio.sleep(540)
            try:
                async with session.get(url) as resp:
                    log.info("keep-alive ping -> %s", resp.status)
            except Exception as e:
                log.warning("keep-alive ping failed: %s", e)


async def start_client():
    while True:
        try:
            await client.start(bot_token=BOT_TOKEN)
            return
        except errors.FloodWaitError as e:
            log.warning("Telegram login flood wait: sleeping %ss", e.seconds)
            await asyncio.sleep(e.seconds + 2)


async def main():
    global fs
    await start_web()                       # open the port first so Render sees the service as live
    asyncio.create_task(keep_alive())
    fs = make_client()
    await load_all()
    await start_client()
    me = await client.get_me()
    try:
        await client(functions.bots.SetBotCommandsRequest(
            scope=types.BotCommandScopeDefault(), lang_code="",
            commands=[types.BotCommand(command="start", description="Bot start karo"),
                      types.BotCommand(command="admin", description="Admin panel")]))
    except Exception as e:
        log.info("set commands skipped: %s", e)
    asyncio.create_task(sweeper())
    log.info("Button colours: %s", "supported by this Telethon" if HAS_STYLE else "NOT supported by installed Telethon (plain buttons)")
    log.info("Bot @%s is running (Firestore connected)", me.username)
    await client.run_until_disconnected()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
