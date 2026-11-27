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
from telethon import Button, TelegramClient, errors, events, functions, types, utils
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
def clean_token(raw: str) -> str:
    """Tolerate quotes / spaces / newlines / a leading 'bot' that sneak in when pasting into Render."""
    t = raw.strip().strip("'\"").strip()
    if t.lower().startswith("bot") and ":" in t:
        t = t[3:]
    return re.sub(r"\s+", "", t)


BOT_TOKEN = clean_token(os.environ["BOT_TOKEN"])
TOKEN_OK = bool(re.fullmatch(r"\d{6,12}:[A-Za-z0-9_-]{30,}", BOT_TOKEN))
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
CATS = []         # [{"id","name","style","position","icon","fb"}]
VIDS = {}         # category id -> [{"id","file_id","kind","added_at"}]  (lazy)
CHANNELS = {}     # chat_id -> {"title","link","channel_id","access_hash"}
BANNED = set()
KNOWN = {}        # uid -> (name, username, access_hash) already saved by this process
HASH = {}         # uid -> access_hash (Telethon can't reach a user without it after a restart)
PENDING = []      # [{"doc","chat_id","message_id","at"}]
STATE = {}        # uid -> admin "waiting for input" state
NAV = {}          # uid -> id of the button whose sub-menu the user is looking at (None = main menu)
REQS = set()      # (channel_id, user_id) pairs that asked to join a private channel (join-request links)
LEN_BUFFER = 30   # seconds added to a video's length before auto-delete
_COUNT = {}

DEFAULTS = {
    "welcome": "🎉 <b>Welcome {name}!</b>\n\nNeeche se apni pasand ka funny video chuno 👇😂",
    "brand": "",
    "timer": "5",       # minutes, 0 = off
    "protect": "1",     # 1 = no forward / no save
    "timer_mode": "0",  # 1 = delete each video after its own length
}


def make_client():
    info = load_firebase_info()
    creds = service_account.Credentials.from_service_account_info(info)
    return firestore.AsyncClient(project=info["project_id"], credentials=creds)


async def load_all():
    d = await fs.collection("settings").document("main").get()
    if d.exists:
        S.update(d.to_dict() or {})
    for it in S.get("icons") or []:          # premium-emoji map saved from the bot ("emoji.map")
        e, _, i = str(it).rpartition("|")
        if e and i.isdigit():
            PREMIUM_ICONS[e] = i
    async for a in fs.collection("admins").stream():
        x = a.to_dict() or {}
        ADMINS[int(a.id)] = {"name": x.get("name") or a.id, "perms": set(x.get("perms", []))}
    async for c in fs.collection("categories").stream():
        x = c.to_dict() or {}
        CATS.append(_cat_dict(c.id, x))
    sort_cats()
    async for c in fs.collection("channels").stream():
        x = c.to_dict() or {}
        if x.get("channel_id") and x.get("access_hash"):
            CHANNELS[int(c.id)] = {"title": x.get("title", c.id), "link": x.get("link"),
                                   "channel_id": int(x["channel_id"]), "access_hash": int(x["access_hash"])}
    async for r in fs.collection("join_requests").stream():
        x = r.to_dict() or {}
        if x.get("channel_id") and x.get("user_id"):
            REQS.add((int(x["channel_id"]), int(x["user_id"])))
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


def _cat_dict(cid, x):
    return {"id": cid, "name": x.get("name", "?"), "style": x.get("style", ""), "position": x.get("position", 0),
            "icon": x.get("icon") or None, "fb": x.get("fb") or None, "parent": x.get("parent") or None,
            "cover": x.get("cover") or None, "link_text": x.get("link_text") or "",
            "link_url": x.get("link_url") or "", "cover_cap": x.get("cover_cap") or ""}


async def create_cat(name, icon=None, fb=None, parent=None):
    pos = int(time.time() * 1000)
    data = {"name": name, "style": "primary", "position": pos, "icon": icon, "fb": fb, "parent": parent}
    _, ref = await fs.collection("categories").add(data)
    CATS.append(_cat_dict(ref.id, data))
    sort_cats()
    return ref.id


async def update_cat(cid, **fields):
    c = cat_get(cid)
    if c:
        c.update(fields)
        sort_cats()
        await fs.collection("categories").document(cid).update(fields)


def siblings(c):
    return [x for x in CATS if (x.get("parent") or None) == (c.get("parent") or None)]


async def move_dir(cid, d):
    """d = -1 up, +1 down (inside the same menu level)."""
    c = cat_get(cid)
    if not c:
        return
    sibs = siblings(c)
    i = next((i for i, x in enumerate(sibs) if x["id"] == cid), None)
    j = None if i is None else i + d
    if j is None or j < 0 or j >= len(sibs):
        return
    upper, lower = sibs[min(i, j)], sibs[max(i, j)]
    pu, pl = upper["position"], lower["position"]
    if pu == pl:
        pl = pu + 1
    await update_cat(upper["id"], position=pl)
    await update_cat(lower["id"], position=pu)


def descendants(cid):
    ids = [cid]
    grew = True
    while grew:
        grew = False
        for c in CATS:
            if c.get("parent") in ids and c["id"] not in ids:
                ids.append(c["id"])
                grew = True
    return ids


def cpath(c):
    names, seen = [], set()
    while c and c["id"] not in seen:
        seen.add(c["id"])
        names.append(c["name"])
        c = cat_get(c.get("parent")) if c.get("parent") else None
    return " › ".join(reversed(names))


async def delete_cat(cid):
    ids = descendants(cid)          # the button and every sub-button inside it
    refs = []
    for one in ids:
        async for v in fs.collection("videos").where(filter=FieldFilter("category_id", "==", one)).stream():
            refs.append(v.reference)
    for i in range(0, len(refs), 400):
        batch = fs.batch()
        for ref in refs[i:i + 400]:
            batch.delete(ref)
        await batch.commit()
    for one in ids:
        await fs.collection("categories").document(one).delete()
        VIDS.pop(one, None)
    CATS[:] = [c for c in CATS if c["id"] not in ids]
    _COUNT.clear()


# ---- videos
async def vids_of(cid):
    if cid not in VIDS:
        items = []
        async for v in fs.collection("videos").where(filter=FieldFilter("category_id", "==", cid)).stream():
            x = v.to_dict() or {}
            items.append({"id": v.id, "file_id": x["file_id"], "kind": x.get("kind", "video"),
                          "added_at": x.get("added_at", 0), "duration": int(x.get("duration") or 0)})
        items.sort(key=lambda v: v["added_at"])
        VIDS[cid] = items
    return VIDS[cid]


async def add_video(cid, file_id, kind, uid, duration=0):
    lst = await vids_of(cid)
    now = int(time.time() * 1000)
    _, ref = await fs.collection("videos").add(
        {"category_id": cid, "file_id": file_id, "kind": kind, "added_by": uid, "added_at": now, "duration": duration})
    lst.append({"id": ref.id, "file_id": file_id, "kind": kind, "added_at": now, "duration": duration})
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
async def schedule_delete(chat_id, mids, minutes=None, seconds=None):
    at = int(time.time()) + (int(seconds) if seconds is not None else int(minutes) * 60)
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
OWNER_ONLY = {"fj", "adm", "protect", "emoji"}


def is_owner(uid):
    return uid == OWNER_ID


def is_staff(uid):
    return is_owner(uid) or uid in ADMINS


def has_perm(uid, perm):
    return is_owner(uid) or (uid in ADMINS and perm in ADMINS[uid]["perms"])


# ─────────────────────────── BUTTONS (raw Telegram TL types) ───────────────────────────
# Coloured buttons + premium (custom) emoji icons need MTProto layer 222+, which Telethon 1.40.0 does not know.
# So we serialise those buttons ourselves, exactly per Telegram's schema (verified on core.telegram.org):
#   keyboardButtonStyle#4fdd3430    flags:# bg_primary:0 bg_danger:1 bg_success:2 icon:flags.3?long
#   keyboardButtonCallback#e62bc960 flags:# requires_password:0 style:flags.10?KeyboardButtonStyle text data
#   keyboardButtonUrl#d80c25ec      flags:# style:flags.10?KeyboardButtonStyle text url
#   keyboardButton#7d170cff         flags:# style:flags.10?KeyboardButtonStyle text
# At startup the bot sends the owner a tiny test message; if Telegram rejects styled buttons, styles switch
# themselves off and plain buttons are used (nothing breaks). Force off any time with env BUTTON_STYLES=off.
#
# Premium emoji on buttons: map a normal emoji -> custom emoji id in PREMIUM_ICONS (below) or env
# PREMIUM_ICONS='{"✅":"5368324170671202286"}'. A button whose text starts with that emoji gets the premium
# emoji as icon. Telegram rule: works only if the bot OWNER has Telegram Premium (or bot has Fragment usernames).
# Owner can send any premium emoji to the bot (no active state) and the bot replies with its custom emoji id.
import inspect
import struct

import telethon.tl.types as _tl_types
from telethon.tl.tlobject import TLObject as _TLObject

PREMIUM_ICONS = {
    # "✅": "0000000000000000000",
    # "❌": "0000000000000000000",
}
try:
    PREMIUM_ICONS.update(json.loads(os.environ.get("PREMIUM_ICONS", "") or "{}"))
except Exception:
    log.warning("PREMIUM_ICONS env is not valid JSON – ignored")

STYLES_ON = os.environ.get("BUTTON_STYLES", "on").strip().lower() != "off"
HAS_STYLE = STYLES_ON  # kept for the startup log line
_STYLE_BIT = {"primary": 1, "danger": 2, "success": 4}


def _tl(name):
    """Find a raw TL class by name in whichever module this Telethon version keeps it."""
    return getattr(types, name, None) or getattr(_tl_types, name, None)


def _params(cls):
    try:
        return set(inspect.signature(cls.__init__).parameters) - {"self"}
    except Exception:
        return set()


ENV_ICONS = dict(PREMIUM_ICONS)


def _match_icon(text):
    """Label starts with an emoji that has a saved premium twin? -> (rest_of_label, icon_id, emoji)."""
    t = text.replace("\ufe0f", "")
    for emo, cid in PREMIUM_ICONS.items():
        e = emo.replace("\ufe0f", "")
        if e and t.startswith(e):
            return t[len(e):].lstrip(), cid, emo
    return text, None, None


async def save_icons():
    await put("icons", [f"{e}|{i}" for e, i in PREMIUM_ICONS.items() if ENV_ICONS.get(e) != i])


def _style_bytes(color, icon=None):
    flags = _STYLE_BIT.get(color, 0) | (8 if icon else 0)
    out = struct.pack("<II", 0x4FDD3430, flags)
    if icon:
        out += struct.pack("<q", int(icon))
    return out


_STYLED = {}


def _styled():
    """Lazily build button subclasses that serialise with the `style` field."""
    if _STYLED:
        return _STYLED
    ser = _TLObject.serialize_bytes
    Cb, Url, Kb = _tl("KeyboardButtonCallback"), _tl("KeyboardButtonUrl"), _tl("KeyboardButton")
    try:
        class SCb(Cb):
            def __init__(self, text, data, sb):
                super().__init__(text=text, data=data)
                self._sb = sb

            def _bytes(self):
                return b"".join((struct.pack("<II", 0xE62BC960, 1 << 10), _style_bytes(*self._sb), ser(self.text), ser(self.data)))

        class SUrl(Url):
            def __init__(self, text, url, sb):
                super().__init__(text=text, url=url)
                self._sb = sb

            def _bytes(self):
                return b"".join((struct.pack("<II", 0xD80C25EC, 1 << 10), _style_bytes(*self._sb), ser(self.text), ser(self.url)))

        class SKb(Kb):
            def __init__(self, text, sb):
                super().__init__(text=text)
                self._sb = sb

            def _bytes(self):
                return b"".join((struct.pack("<II", 0x7D170CFF, 1 << 10), _style_bytes(*self._sb), ser(self.text)))

        _STYLED.update(cb=SCb, url=SUrl, kb=SKb)
    except Exception:
        log.exception("styled button classes could not be built – plain buttons will be used")
    return _STYLED


def _prep(text, color, icon=None, fb=None):
    """-> (shown_text, style_bytes_or_None, plain_text).
    icon given (button made with a premium emoji): text has no emoji, fb is the normal-emoji fallback.
    icon None: label may start with an emoji that has a saved premium twin (auto icon).
    When styles are off / unsupported the plain text (with normal emoji) is used."""
    icons_on = S.get("icon_off") != "1"
    if icon:
        shown, plain = text, (f"{fb} {text}" if fb else text)
    else:
        shown, icon, _ = _match_icon(text) if icons_on else (text, None, None)
        plain = text
    if not icons_on:
        icon = None
    if STYLES_ON and (color in _STYLE_BIT or icon) and _styled():
        return (shown if icon else plain), (color if color in _STYLE_BIT else None, icon), plain
    return plain, None, plain


def _mk(kind, native_name, sb, fallback, *args):
    """Try: 1) styled raw button, 2) plain native constructor, 3) Telethon's own helper."""
    attempts = []
    if sb is not None and _styled().get(kind):
        attempts.append(lambda: _styled()[kind](*args, sb))
    nat = _tl(native_name)
    if nat is not None:
        attempts.append(lambda: nat(*args))
    attempts.append(fallback)
    err = None
    for attempt in attempts:
        try:
            return attempt()
        except Exception as e:
            err = e
    raise err


def B(text, data=None, style=None, url=None, icon=None, fb=None):
    """Inline button (callback or url)."""
    text, sb, plain = _prep(text, style, icon, fb)
    if url:
        return _mk("url", "KeyboardButtonUrl", sb, lambda: Button.url(plain, url).button, text, url)
    raw = data.encode()
    return _mk("cb", "KeyboardButtonCallback", sb, lambda: Button.inline(plain, raw).button, text, raw)


def KB(text, style=None, icon=None, fb=None):
    """Bottom (reply) keyboard button."""
    text, sb, plain = _prep(text, style, icon, fb)
    return _mk("kb", "KeyboardButton", sb, lambda: Button.text(plain).button, text)


def cname(c):
    """Category name for admin texts (normal-emoji fallback shown in front)."""
    return f"{c['fb']} {c['name']}" if c.get("fb") else c["name"]


def _custom_emoji_ents(event):
    ce = _tl("MessageEntityCustomEmoji")
    return [e for e in (event.message.entities or []) if ce and isinstance(e, ce)]


def _u16(text, a, b=None):
    """Slice a string by Telegram entity offsets (UTF-16 code units)."""
    raw = text.encode("utf-16-le")
    return raw[a * 2:(b * 2 if b is not None else None)].decode("utf-16-le", "ignore")


def parse_cat_name(event):
    """Button name from an admin message. A premium emoji at the very start becomes the button icon.
    -> (name, icon_id_or_None, fallback_emoji_or_None)"""
    raw = event.raw_text or ""
    lead = len(raw) - len(raw.lstrip())
    name, icon, fb = raw.strip(), None, None
    for e in _custom_emoji_ents(event):
        if e.offset == lead:
            fb = _u16(raw, e.offset, e.offset + e.length)
            icon = int(e.document_id)
            name = _u16(raw, e.offset + e.length).strip()
            break
    if icon is None:  # plain emoji that has a saved premium twin -> use it too
        rest, mic, emo = _match_icon(name)
        if mic and rest:
            name, icon, fb = rest, int(mic), emo
    if icon and not name:  # only an emoji, no text: keep it as a normal emoji name
        name, icon, fb = fb, None, None
    return name[:40], icon, fb


async def style_selftest():
    """Send the owner a styled test button. If Telegram refuses it, switch styles off globally."""
    global STYLES_ON, HAS_STYLE
    if not STYLES_ON:
        return
    try:
        m = await client.send_message(OWNER_ID, "<code>$ style --selftest</code>",
                                      buttons=inline([[B("✅ ok", "noop", "success"), B("⛔ ok", "noop", "danger"),
                                                       B("🔷 ok", "noop", "primary")]]))
        try:
            await m.delete()
        except Exception:
            pass
        log.info("Button styles: self-test PASSED (colours%s active)", " + premium icons" if PREMIUM_ICONS else "")
    except (errors.UserIsBlockedError, errors.PeerIdInvalidError, errors.InputUserDeactivatedError, ValueError):
        log.info("Button styles: self-test skipped (owner has not started the bot) – styles stay ON")
    except Exception as e:
        STYLES_ON = HAS_STYLE = False
        log.warning("Button styles: Telegram rejected the test (%s: %s) – plain buttons from now on", type(e).__name__, e)


def _unify(rows):
    """Every row gets ONE colour: the colour of its first coloured button."""
    for r in rows:
        sty = [b for b in r if hasattr(b, "_sb")]
        if len(sty) > 1:
            col = next((b._sb[0] for b in sty if b._sb[0]), None)
            if col:
                for b in sty:
                    b._sb = (col, b._sb[1])


def inline(rows):
    rows = [r for r in rows if r]
    if not rows:
        return None
    _unify(rows)
    return _tl("ReplyInlineMarkup")(rows=[_tl("KeyboardButtonRow")(buttons=list(r)) for r in rows])


def reply_markup(rows):
    _unify(rows)
    krows = [_tl("KeyboardButtonRow")(buttons=list(r)) for r in rows]
    for kw in ({"resize": True, "persistent": True, "placeholder": "Neeche se video chuno 👇"}, {"resize": True}):
        try:
            return _tl("ReplyKeyboardMarkup")(rows=krows, **kw)
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


BACK_BTN = "⬅️ Back"


def menu_markup(uid, parent="__nav__"):
    """Persistent bottom keyboard: the buttons of the level the user is on (+ Back inside a sub-menu)."""
    if parent == "__nav__":
        parent = NAV.get(uid)
        if parent and not cat_get(parent):
            parent = NAV[uid] = None
    try:
        rows_ = chunk([KB(c["name"], c["style"] or None, c.get("icon"), c.get("fb"))
                       for c in CATS if (c.get("parent") or None) == parent], 2)
        if parent:
            rows_.append([KB(BACK_BTN, "danger")])
        elif is_staff(uid):
            rows_.append([KB(ADMIN_BTN, "primary")])
        if not rows_:
            return types.ReplyKeyboardHide()
        return reply_markup(rows_)
    except Exception:
        log.exception("bottom keyboard could not be built - sending without it")
        return None


async def panel_text(uid):
    tag = "👑 <b>OWNER PANEL</b>" if is_owner(uid) else "🛡 <b>ADMIN PANEL</b>"
    n_u = await count("users")
    n_v = await count("videos")
    return f"{tag}\n━━━━━━━━━━━━━━\n👥 Users: <b>{n_u}</b>   🎬 Videos: <b>{n_v}</b>\n\nKya karna hai? 👇"


def main_kb(uid):
    items = [
        ("📤 Upload", "a:upload", "success", "upload"),
        ("🔘 Buttons", "a:btns", "primary", "buttons"),
        ("🎞 Videos", "a:vids", "primary", "videos"),
        ("🏷 Branding", "a:brand", "primary", "branding"),
        ("⏱ Auto Delete", "a:timer", "primary", "timer"),
        ("👋 Welcome", "a:welcome", "primary", "welcome"),
        ("📊 Stats", "a:stats", None, "stats"),
        ("📣 Broadcast", "a:bc", "success", "broadcast"),
        ("🚫 Ban/Unban", "a:ban", "danger", "ban"),
    ]
    pairs = [(t, d) for t, d, _s, p in items if has_perm(uid, p)]
    if is_owner(uid):
        pairs += [("📢 Force Join", "a:fj"), ("👥 Admins", "a:adm"), ("🔒 Protect", "a:protect"),
                  ("✨ Premium Emoji", "a:emoji")]
    colours = ["primary", "success"]          # row 1 blue, row 2 green, row 3 blue ... (same colour inside a row)
    kb = [[B(t, d, colours[i % 2]) for t, d in row] for i, row in enumerate(chunk(pairs, 2))]
    kb.append([B("🏠 User View", "uhome", "danger")])
    return kb


async def cat_kb(prefix, with_count=False, parent="*"):
    """Admin list of buttons. parent="*" = all (shown as 'A › B'), None = top level, <id> = inside that button."""
    btns = []
    for c in CATS:
        if parent != "*" and (c.get("parent") or None) != parent:
            continue
        label = cpath(c) if parent == "*" else c["name"]
        if with_count:
            label += f" ({len(await vids_of(c['id']))})"
        btns.append(B(label, f"{prefix}{c['id']}", c["style"] or None, icon=c.get("icon"), fb=c.get("fb")))
    return chunk(btns)


# ─────────────────────────── PROTECT EVERY MESSAGE ───────────────────────────
# One hook on every request the bot makes: messages to normal users get noforwards=True (no forward / save /
# copy, screenshots blocked on Android). Admin / Owner chats are skipped so they can save files.
from telethon.client.users import UserMethods as _UserMethods

_orig_call = _UserMethods.__call__


def _protect_request(r):
    try:
        if get("protect") != "1" or type(r).__name__ not in ("SendMessageRequest", "SendMediaRequest", "SendMultiMediaRequest"):
            return
        pr = getattr(r, "peer", None)
        if not isinstance(pr, types.InputPeerUser) or is_staff(pr.user_id):
            return
        if getattr(r, "noforwards", None) is None:
            r.noforwards = True
    except Exception:
        log.exception("protect hook failed")


async def _call_with_protect(self, request, *a, **kw):
    for r in (request if isinstance(request, (list, tuple)) else [request]):
        _protect_request(r)
    return await _orig_call(self, request, *a, **kw)


_UserMethods.__call__ = _call_with_protect


# ─────────────────────────── FORCE JOIN / GATE ───────────────────────────
async def is_member(ch, user_peer):
    uid_ = getattr(user_peer, "user_id", None)
    if uid_ and (ch["channel_id"], int(uid_)) in REQS:      # asked to join a private channel (approval link)
        return True
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
    NAV[uid] = None
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


def input_media_for(file_id):
    if file_id.startswith("rawphoto:"):
        _, pid, ah, ref = file_id.split(":", 3)
        return types.InputMediaPhoto(id=types.InputPhoto(
            id=int(pid), access_hash=int(ah), file_reference=base64.urlsafe_b64decode(ref.encode())))
    doc = utils.resolve_bot_file_id(file_id)
    if doc is None:
        raise ValueError("saved file_id is not valid")
    return utils.get_input_media(doc)


async def send_media(uid, v, caption, protect, markup=None, spoiler=False):
    """Send a stored video/photo with protect_content (no forward/save). Returns the new message id."""
    text, ents = tl_html.parse(caption) if caption else ("", [])
    media = input_media_for(v["file_id"])
    if spoiler and hasattr(media, "spoiler"):
        media.spoiler = True
    res = await client(functions.messages.SendMediaRequest(
        peer=await client.get_input_entity(peer(uid)),
        media=media,
        message=text,
        random_id=_rid(),
        entities=ents or None,
        noforwards=True if protect else None,
        reply_markup=markup,
    ))
    return _msg_id(res)


def del_seconds(v):
    """Seconds after which this item is auto-deleted (None = never)."""
    if get("timer_mode") == "1" and v.get("duration"):
        return int(v["duration"]) + LEN_BUFFER
    m = int(get("timer"))
    return m * 60 if m > 0 else None


async def send_cover(uid, c):
    """Cover photo (spoiler) with a link button under it."""
    protect = get("protect") == "1"
    markup = None
    if c.get("link_url"):
        markup = inline([[B(c.get("link_text") or "▶️ Click Here To Watch", url=c["link_url"], style="success")]])
    mid = None
    for mk in ([markup, None] if markup is not None else [None]):
        try:
            mid = await send_media(uid, {"file_id": c["cover"]}, c.get("cover_cap") or "", protect, mk, spoiler=True)
            break
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            log.warning("cover send failed (%s)%s", e, " - retrying without link button" if mk is not None else "")
    if mid:
        m = int(get("timer"))
        if m > 0:
            await schedule_delete(uid, [mid], seconds=m * 60)


async def send_step(event, cid, idx):
    """Send ONE video/photo of a button, with a 'Next' button under it (not the last one)."""
    uid = event.sender_id
    vids = (await vids_of(cid))[:30]
    if idx >= len(vids):
        await client.send_message(uid, "🎉 Sab videos dekh liye! Neeche se dusra button chuno 👇", buttons=menu_markup(uid))
        return
    protect = get("protect") == "1"
    v = vids[idx]
    secs = del_seconds(v)
    caption = get("brand")
    nxt = None
    if idx < len(vids) - 1:
        # when the video will be auto-deleted, 'Next' goes in its own message so it survives the delete
        cb = ("ns:" if secs else "nx:") + f"{cid}:{idx + 1}"
        nxt = inline([[B(f"➡️ Next Video ({idx + 2}/{len(vids)})", cb, "primary")]])
    mid = None
    for attempt in (1, 2):
        try:
            mid = await send_media(uid, v, caption, protect, None if secs else nxt)
            break
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            log.warning("send failed: %s", e)
            break
    if not mid:
        await client.send_message(uid, "❌ Ye video abhi nahi bhej paaye, thodi der baad try karo.", buttons=menu_markup(uid))
        return
    to_delete = [mid]
    if secs and nxt:
        await client.send_message(uid, "👇 Agla video dekhne ke liye", buttons=nxt)
    if idx == 0 and secs:
        if get("timer_mode") == "1" and v.get("duration"):
            txt = "⏳ <i>Ye video apni length ke hisaab se automatically delete ho jayega.</i>"
        else:
            txt = f"⏳ <i>Ye video <b>{max(1, secs // 60)} minute</b> me automatically delete ho jayega.</i>"
        n = await client.send_message(uid, txt, buttons=menu_markup(uid))
        to_delete.append(n.id)
    if secs:
        await schedule_delete(uid, to_delete, seconds=secs)


async def open_cat(event, c):
    """A button was tapped: sub-menu (if it has sub-buttons) + cover photo + videos."""
    uid = event.sender_id
    if not await gate(event):
        return
    kids = [x for x in CATS if x.get("parent") == c["id"]]
    vids = await vids_of(c["id"])
    if kids:
        NAV[uid] = c["id"]
        await client.send_message(uid, f"📂 <b>{esc(cname(c))}</b>\n\nNeeche se chuno 👇", buttons=menu_markup(uid))
    if c.get("cover"):
        await send_cover(uid, c)
    if vids:
        await send_step(event, c["id"], 0)
    if not kids and not c.get("cover") and not vids:
        await client.send_message(uid, "Is button me abhi koi video nahi hai 😅", buttons=menu_markup(uid))


async def deliver(event, cid):
    c = cat_get(cid)
    if c:
        await open_cat(event, c)


async def menu_text(event, text):
    """A normal user tapped a bottom-keyboard button (it arrives as plain text)."""
    uid = event.sender_id
    cur = NAV.get(uid)
    if text == BACK_BTN:
        c = cat_get(cur) if cur else None
        NAV[uid] = (c.get("parent") if c else None) or None
        now = cat_get(NAV[uid]) if NAV[uid] else None
        await event.respond(f"📂 <b>{esc(cname(now))}</b>" if now else "🏠 <b>Main menu</b>", buttons=menu_markup(uid))
        return

    def _key(t):
        return re.sub(r"^[^\w]+", "", t or "").strip().lower()

    def find(pool):
        return next((c for c in pool if c["name"] == text), None) \
            or next((c for c in pool if _key(c["name"]) and _key(c["name"]) == _key(text)), None)

    c = find([c for c in CATS if (c.get("parent") or None) == cur]) or find(CATS)
    if c:
        await open_cat(event, c)
    else:
        await event.respond("🔄 Menu update ho gaya hai, neeche se naya button chuno 👇", buttons=menu_markup(uid))


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
            await show(q, "📤 <b>Upload</b>\n\nKis button me video / photo daalni hai? 👇", kb)
        elif sub == "cat":
            c = cat_get(p[3])
            if not c:
                await show(q, "Button nahi mila.", [back()])
                return
            STATE[uid] = ("upload", c["id"])
            await show(q, f"📤 Button: <b>{esc(cname(c))}</b>\n\nAb video / photo bhejo (ek ke baad ek). Khatam hone par <b>Done</b> dabao.",
                       [[B("✅ Done", "a:home", "success")]])

    # ---- buttons
    elif sec == "btns":
        if sub is None:
            kb = await cat_kb("a:btns:view:", True, parent=None)
            kb.append([B("➕ New Button", "a:btns:new", "success")])
            kb.append(back())
            await show(q, "🔘 <b>Buttons</b>\n\nEdit karne ke liye button chuno 👇", kb)
        elif sub == "new":
            par = p[3] if len(p) > 3 else None
            STATE[uid] = ("newbtn", par)
            pc = cat_get(par) if par else None
            where = f" (<b>{esc(cpath(pc))}</b> ke andar)" if pc else ""
            await show(q, f"➕ Naye button{where} ka <b>naam</b> bhejo.\n✨ Premium emoji ke saath bhi bhej sakte ho (jaise: 🔥 Funny).",
                       [back(f"a:btns:view:{par}" if par else "a:btns")])
        elif sub in ("view", "col", "up", "dn", "ren", "del", "delok", "cov"):
            cid = p[3]
            c = cat_get(cid)
            if not c:
                await show(q, "Button nahi mila.", [back("a:btns")])
                return
            par = c.get("parent")
            up_to = f"a:btns:view:{par}" if par else "a:btns"
            if sub == "ren":
                STATE[uid] = ("rename", cid)
                await show(q, f"✏️ <b>{esc(cname(c))}</b> ka naya naam bhejo (premium emoji ke saath bhi chalega):", [back(f"a:btns:view:{cid}")])
                return
            if sub == "del":
                n_sub = len(descendants(cid)) - 1
                extra = f" (aur uske {n_sub} sub-button)" if n_sub else ""
                await show(q, f"⚠️ <b>{esc(cname(c))}</b>{extra} aur saare videos delete ho jayenge. Pakka?",
                           [[B("🗑 Haan, Delete", f"a:btns:delok:{cid}", "danger"), B("❌ Cancel", f"a:btns:view:{cid}", "success")]])
                return
            if sub == "delok":
                await delete_cat(cid)
                await show(q, "🗑 Button delete ho gaya.", [back(up_to)])
                return
            if sub == "cov":
                act = p[4] if len(p) > 4 else None
                prompts = {
                    "photo": ("covphoto", "🖼 <b>Cover photo</b> bhejo (photo ki tarah, file ki tarah nahi). Ye spoiler ke saath dikhegi."),
                    "text": ("covtext", "🔘 Photo ke niche wale button ka <b>text</b> bhejo (jaise: Click Here To Watch)."),
                    "link": ("covlink", "🔗 Button ka <b>link</b> bhejo (https://... ya t.me/...)."),
                    "cap": ("covcap", "📝 Photo ke niche ka <b>caption</b> bhejo (optional)."),
                }
                if act in prompts:
                    STATE[uid] = (prompts[act][0], cid)
                    await show(q, prompts[act][1], [back(f"a:btns:cov:{cid}")])
                    return
                if act == "clr":
                    await update_cat(cid, cover=None, link_text="", link_url="", cover_cap="")
                    c = cat_get(cid)
                kb = [[B("🖼 Cover Photo", f"a:btns:cov:{cid}:photo", "success"), B("🔘 Button Text", f"a:btns:cov:{cid}:text", "success")],
                      [B("🔗 Link", f"a:btns:cov:{cid}:link", "success"), B("📝 Caption", f"a:btns:cov:{cid}:cap", "success")],
                      [B("🗑 Cover Hatao", f"a:btns:cov:{cid}:clr", "danger")],
                      back(f"a:btns:view:{cid}")]
                await show(q, f"🖼 <b>Cover + Link</b> — {esc(cname(c))}\n\n"
                              f"🖼 Photo: {'✅' if c.get('cover') else '❌'}\n"
                              f"🔘 Button text: {esc(c.get('link_text') or '—')}\n"
                              f"🔗 Link: {esc(c.get('link_url') or '—')}\n"
                              f"📝 Caption: {'✅' if c.get('cover_cap') else '—'}\n\n"
                              "User is button pe click karega to ye photo <b>spoiler</b> ke saath aayegi, uske niche link wala button hoga. "
                              "Is button me video bhi ho to wo cover ke baad aayenge.", kb)
                return
            if sub == "col":
                order = ["primary", "success", "danger", ""]
                cur = c["style"] if c["style"] in order else ""
                await update_cat(cid, style=order[(order.index(cur) + 1) % len(order)])
            if sub == "up":
                await move_dir(cid, -1)
            if sub == "dn":
                await move_dir(cid, 1)
            c = cat_get(cid)
            n = len(await vids_of(cid))
            colors = {"primary": "🔵 Blue", "success": "🟢 Green", "danger": "🔴 Red", "": "⚪ Default"}
            kb = [
                [B("✏️ Rename", f"a:btns:ren:{cid}", "primary"), B("🎨 Color", f"a:btns:col:{cid}", "primary")],
                [B("⬆️ Move Up", f"a:btns:up:{cid}", "success"), B("⬇️ Move Down", f"a:btns:dn:{cid}", "success")],
                [B("➕ Sub Button", f"a:btns:new:{cid}", "primary"), B("🖼 Cover + Link", f"a:btns:cov:{cid}", "primary")],
                [B("🗑 Delete", f"a:btns:del:{cid}", "danger")],
            ]
            kids = await cat_kb("a:btns:view:", True, parent=cid)
            kb += kids
            kb.append(back(up_to))
            await show(q, f"🔘 <b>{esc(cpath(c))}</b>\n🎬 Videos: {n}\n🔘 Sub-buttons: {len(kids and [x for r in kids for x in r])}\n"
                          f"🖼 Cover: {'haan' if c.get('cover') else 'nahi'}\n🎨 Color: {colors.get(c['style'] or '')}\n"
                          f"✨ Premium emoji: {'haan' if c.get('icon') else 'nahi'}", kb)

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
            kb = chunk([B(f"🗑 #{i}", f"a:vids:del:{v['id']}:{cid}", "danger") for i, v in enumerate(vs, 1)], 3)
            kb.append(back("a:vids"))
            await show(q, f"🎞 <b>{esc(cname(c) if c else '?')}</b> — {len(vs)} videos\n\nDelete karne ke liye dabao 👇", kb)

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
        elif sub == "mode":
            await put("timer_mode", "0" if get("timer_mode") == "1" else "1")
        elif sub == "custom":
            STATE[uid] = ("timer",)
            await show(q, "⏱ Minutes bhejo (0 = auto-delete band, max 1440):", [back("a:timer")])
            return
        cur = int(get("timer"))
        presets = [1, 2, 5, 10, 30]
        btns = [B(("✅ " if cur == m else "") + f"{m} min", f"a:timer:set:{m}", "success" if cur == m else "primary") for m in presets]
        kb = chunk(btns, 3)
        kb.append([B("✍️ Custom", "a:timer:custom", "primary"), B("🚫 Off", "a:timer:set:0", "danger")])
        lm = get("timer_mode") == "1"
        kb.append([B("🎬 Video length mode: ON" if lm else "🎬 Video length mode: OFF", "a:timer:mode", "success" if lm else "danger")])
        kb.append(back())
        await show(q, f"⏱ <b>Auto Delete</b>\n\nFixed timer: <b>{'OFF' if cur == 0 else str(cur) + ' minute'}</b>\n"
                      f"🎬 Video length mode: <b>{'ON' if lm else 'OFF'}</b>\n\n"
                      "Length mode ON ho to har video apni length (+30 sec) ke baad delete hoti hai. "
                      "Photo ya jinki length pata nahi (purani videos), unpe fixed timer lagta hai.", kb)

    # ---- protect (owner)
    elif sec == "protect":
        if sub == "tog":
            await put("protect", "0" if get("protect") == "1" else "1")
        on = get("protect") == "1"
        kb = [[B("🔴 Turn OFF" if on else "🟢 Turn ON", "a:protect:tog", "danger" if on else "success")], back()]
        await show(q, f"🔒 <b>Forward / Save Protection</b>\n\nStatus: <b>{'ON ✅' if on else 'OFF ❌'}</b>\n\n"
                      "ON hone par bot ke <b>saare messages</b> (video, photo, text) users forward, save ya copy nahi kar sakte. "
                      "Android pe Telegram screenshot aur screen recording bhi block kar deta hai. "
                      "iPhone / desktop pe ye guarantee nahi hai, aur kisi doosre phone se record karne se koi nahi rok sakta.\n\n"
                      "Admin / Owner ko jo messages jaate hain (panel, CSV) unpe protection nahi lagta, taaki tum file save kar sako.", kb)

    # ---- force join (owner)
    elif sec == "fj":
        if sub == "add":
            STATE[uid] = ("fjadd",)
            await show(q, "📢 <b>Public channel:</b> @username bhejo, ya channel ki koi post yahan forward karo.\n\n"
                          "🔒 <b>Private channel (2 step):</b>\n"
                          "1️⃣ Pehle apna invite link bhejo (<code>t.me/+xxxx</code>) — jo users ko dikhana hai. "
                          "Link me <b>Request admin approval</b> ON ho to user join-request bhejte hi verified ho jata hai.\n"
                          "2️⃣ Fir us channel ki koi <b>post forward</b> karo (ya channel ki ID <code>-100...</code> bhejo) taaki bot channel pehchan sake.\n\n"
                          "⚠️ Bot ko pehle us channel me <b>admin</b> (Invite Users right ke saath) bana do.", [back("a:fj")])
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

    # ---- premium emoji map (owner)
    elif sec == "emoji":
        if sub == "add":
            STATE[uid] = ("emoji",)
            await show(q, "✨ <b>Premium emoji</b> bhejo (ek ya zyada, ek hi message me).\n"
                          "Bot unhe yaad rakhega. Uske baad jis button ke naam ki shuruaat us emoji se hogi, "
                          "wahan premium emoji lagega.",
                       [back("a:emoji")])
            return
        if sub == "tog":
            await put("icon_off", "0" if S.get("icon_off") == "1" else "1")
        if sub == "clr":
            PREMIUM_ICONS.clear()
            PREMIUM_ICONS.update(ENV_ICONS)
            await save_icons()
        items = list(PREMIUM_ICONS.items())
        lines = "\n".join(f"{e}  →  {i}" for e, i in items[:40]) or "(empty)"
        icons_on = S.get("icon_off") != "1"
        kb = [[B("➕ Emoji Add", "a:emoji:add", "success"), B("🗑 Sab Hatao", "a:emoji:clr", "danger")],
              [B("✨ Premium icon: ON" if icons_on else "✨ Premium icon: OFF", "a:emoji:tog", "success" if icons_on else "danger")],
              back()]
        await show(q, f"✨ <b>Premium Emoji</b> — {len(items)} saved\n\n{esc(lines)}\n\n"
                      "Button banate waqt seedha premium emoji + naam bhejna bhi chalta hai (jaise: 🔥 Funny).\n"
                      "Agar button me premium emoji nahi dikh raha, to <b>Premium icon</b> OFF karo — tab normal emoji dikhega.", kb)

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
def duration_of(m):
    """Length in seconds of a video / gif message (0 if unknown)."""
    try:
        for a in (m.document.attributes if m.document else []):
            if isinstance(a, types.DocumentAttributeVideo):
                return int(a.duration or 0)
    except Exception:
        pass
    return 0


def pack_file_id(m):
    """Bot-API style file id. If Telethon cannot pack a photo, keep the raw photo id instead."""
    try:
        fid = utils.pack_bot_file_id(m.media)
        if fid:
            return fid
    except Exception as e:
        log.warning("pack_bot_file_id failed (%s) - using raw photo id", e)
    ph = m.photo
    if ph is not None:
        ref = base64.urlsafe_b64encode(bytes(ph.file_reference or b"")).decode()
        return f"rawphoto:{ph.id}:{ph.access_hash}:{ref}"
    return None


def media_of(m):
    """(kind, file id) for a video / gif / photo / video-or-image document message, else (None, None)."""
    kind = None
    if m.gif:
        kind = "animation"
    elif m.video:
        kind = "video"
    elif m.photo:
        kind = "photo"
    elif m.document and (m.document.mime_type or "").startswith(("video", "image")):
        kind = "document"
    if not kind:
        return None, None
    return kind, pack_file_id(m)


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
            await reply("❌ Sirf video ya photo bhejo (ya Done dabao).")
            return
        try:
            n = await add_video(st[1], fid, mk, uid, duration_of(msg))
        except Exception as e:
            log.exception("upload save failed")
            await reply(f"❌ Save nahi hua: {esc(type(e).__name__)}. Dobara bhejo.")
            return
        await reply(f"✅ {'Photo' if mk == 'photo' else 'Video'} save ho gayi! (is button me total {n})\nAur bhejo ya Done dabao.",
                    [[B("✅ Done", "a:home", "success")]])

    elif kind == "newbtn":
        if not has_perm(uid, "buttons") or not text:
            return
        name, icon, fb = parse_cat_name(event)
        par = st[1] if len(st) > 1 and cat_get(st[1]) else None
        cid = await create_cat(name, icon, fb, par)
        STATE.pop(uid, None)
        await reply("✅ Button ban gaya!" + (" (premium emoji ✨ ke saath)" if icon else "") + "\nAb isme video / photo upload karo 👇",
                    [[B("📤 Upload Here", f"a:upload:cat:{cid}", "success"), B("🔘 Open", f"a:btns:view:{cid}", "primary")],
                     back(f"a:btns:view:{par}" if par else "a:btns")])

    elif kind in ("covphoto", "covtext", "covlink", "covcap"):
        if not has_perm(uid, "buttons") or not cat_get(st[1]):
            return
        cid = st[1]
        back_to = [[B("🖼 Cover Settings", f"a:btns:cov:{cid}", "success")]]
        if kind == "covphoto":
            mk, fid = media_of(msg)
            if mk != "photo":
                await reply("❌ Sirf photo bhejo (photo ki tarah, file ki tarah nahi).")
                return
            await update_cat(cid, cover=fid)
        elif kind == "covtext":
            if not text:
                return
            await update_cat(cid, link_text=text[:60])
        elif kind == "covlink":
            url = text.split()[0] if text else ""
            if url.startswith("t.me/") or url.startswith("telegram.me/"):
                url = "https://" + url
            if not re.match(r"^(https?://|tg://)\S+$", url):
                await reply("❌ Sahi link bhejo (https://... ya t.me/...).")
                return
            await update_cat(cid, link_url=url)
        else:
            await update_cat(cid, cover_cap=(msg.text or "")[:900])
        STATE.pop(uid, None)
        await reply("✅ Save ho gaya!", back_to)

    elif kind == "emoji":
        if not is_owner(uid):
            return
        ents = _custom_emoji_ents(event)
        raw = event.raw_text or ""
        added = 0
        for e in ents:
            ch = _u16(raw, e.offset, e.offset + e.length).strip()
            if ch:
                PREMIUM_ICONS[ch] = str(e.document_id)
                added += 1
        if not added:
            await reply("❌ Koi premium emoji nahi mila. Premium emoji wala message bhejo ya /cancel karo.")
            return
        await save_icons()
        STATE.pop(uid, None)
        lines = "\n".join(f"{e}  →  {i}" for e, i in list(PREMIUM_ICONS.items())[:40])
        await reply(f"✅ {added} premium emoji save ho gaye!\n\n{esc(lines)}",
                    [[B("➕ aur emoji", "a:emoji:add", "success")], back("a:emoji")])

    elif kind == "rename":
        if not has_perm(uid, "buttons") or not text:
            return
        name, icon, fb = parse_cat_name(event)
        await update_cat(st[1], name=name, icon=icon, fb=fb)
        STATE.pop(uid, None)
        await reply("✅ Naam badal gaya!" + (" (premium emoji ✨ ke saath)" if icon else ""), [back("a:btns")])

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
        invite_link = st[1] if len(st) > 1 else None   # link sent in step 1 (private channel)
        fwd = msg.forward
        if fwd is not None:
            try:
                ent = await fwd.get_chat()
            except Exception:
                log.exception("forward get_chat failed")
                ent = None
        if ent is None and text:
            # Telegram does NOT let bots open invite links, so we only store the link; the channel itself is
            # identified from a forwarded post / ID in the next message.
            m_inv = re.search(r"(?:https?://)?(?:t\.me|telegram\.me)/(\+[A-Za-z0-9_-]+|joinchat/[A-Za-z0-9_-]+)", text)
            if m_inv:
                STATE[uid] = ("fjadd", "https://t.me/" + m_inv.group(1))
                await reply("✅ Link mil gaya.\n\nAb us <b>private channel ki koi post forward</b> karo "
                            "(ya channel ki ID <code>-100...</code> bhejo) taaki bot channel pehchan sake.\n\n"
                            "⚠️ Bot ko us channel me admin (Invite Users right ke saath) bana hona chahiye. "
                            "Agar channel me forward band hai to ID bhejo.", [back("a:fj")])
                return
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
                log.exception("fjadd get_entity failed")
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
        if invite_link:
            link = invite_link
        elif ent.username:
            link = f"https://t.me/{ent.username}"
        else:
            try:  # private channel: make a link that needs admin approval
                inv = await client(functions.messages.ExportChatInviteRequest(peer=ent, request_needed=True, title="Bot Join"))
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
    if is_owner(uid):
        ce = _tl("MessageEntityCustomEmoji")
        ents = [e for e in (event.message.entities or []) if ce and isinstance(e, ce)]
        if ents:
            raw = (event.raw_text or "").encode("utf-16-le")  # entity offsets are in UTF-16 units
            lines = [f"{raw[e.offset * 2:(e.offset + e.length) * 2].decode('utf-16-le', 'ignore')} = <code>{e.document_id}</code>" for e in ents]
            await event.respond("🆔 <b>Premium emoji IDs</b>\n" + "\n".join(lines) +
                                "\n\nPREMIUM_ICONS me aise daalo: <code>{\"✅\": \"ID\"}</code>", parse_mode="html")
            return
    if text:
        await menu_text(event, text)


@client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
async def on_message(event):
    try:
        await handle_message(event)
    except Exception:
        log.exception("message handler error")


@client.on(events.Raw(types.UpdateBotChatInviteRequester))
async def on_join_request(update):
    """Someone pressed 'Request to join' on one of our force-join links -> count them as joined."""
    try:
        chan = getattr(update.peer, "channel_id", None)
        if not chan or not any(ch["channel_id"] == int(chan) for ch in CHANNELS.values()):
            return
        key = (int(chan), int(update.user_id))
        if key in REQS:
            return
        REQS.add(key)
        await fs.collection("join_requests").document(f"{key[0]}_{key[1]}").set(
            {"channel_id": key[0], "user_id": key[1], "at": int(time.time())})
    except Exception:
        log.exception("join request handler failed")


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
            if not cat_get(cid):
                await event.answer("Button nahi mila", alert=True)
                return
            await event.answer("🎬 Aa raha hai...")
            await deliver(event, cid)
        elif data.startswith("nx:"):
            _, cid, i = data.split(":")
            if not await gate(event):
                await event.answer()
                return
            await event.answer()
            try:  # take the Next button off the video it was under, so it can't be tapped twice
                await client(functions.messages.EditMessageRequest(
                    peer=await event.get_input_chat(), id=event.message_id,
                    reply_markup=_tl("ReplyInlineMarkup")(rows=[])))
            except Exception:
                pass
            await send_step(event, cid, int(i))
        elif data.startswith("ns:"):
            _, cid, i = data.split(":")
            if not await gate(event):
                await event.answer()
                return
            await event.answer()
            try:  # the little "Next" message is used up -> remove it
                await event.delete()
            except Exception:
                pass
            await send_step(event, cid, int(i))
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
    shown = f"{BOT_TOKEN.split(':')[0]}:{'*' * 6}" if ":" in BOT_TOKEN else "(no ':' found)"
    log.info("BOT_TOKEN as seen by the bot: %s  (length %d, format %s)", shown, len(BOT_TOKEN), "OK" if TOKEN_OK else "WRONG")
    while True:
        try:
            await client.start(bot_token=BOT_TOKEN)
            return
        except errors.FloodWaitError as e:
            log.warning("Telegram login flood wait: sleeping %ss", e.seconds)
            await asyncio.sleep(e.seconds + 2)
        except errors.AccessTokenInvalidError:
            # Don't crash: Render would restart us in a loop. Stay alive and tell the owner what to fix.
            log.error("TELEGRAM REJECTED BOT_TOKEN (%s). Fix: @BotFather -> /mybots -> your bot -> API Token "
                      "(or Revoke current token), copy the FULL token and paste it in Render -> Environment -> "
                      "BOT_TOKEN, then redeploy. Retrying in 60s.", shown)
            await asyncio.sleep(60)
        except errors.RPCError as e:
            log.warning("login failed (%s), retrying in 15s", type(e).__name__)
            await asyncio.sleep(15)
        except ValueError as e:      # Telethon: "Request was unsuccessful N time(s)" (Telegram-side hiccup)
            log.warning("login failed (%s), retrying in 15s", e)
            await asyncio.sleep(15)


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
    await style_selftest()
    try:
        import telethon
        log.info("Telethon %s | KeyboardButton%s | KeyboardButtonCallback%s | KeyboardButtonStyle%s",
                 telethon.__version__,
                 sorted(_params(_tl("KeyboardButton"))) if _tl("KeyboardButton") else "MISSING",
                 sorted(_params(_tl("KeyboardButtonCallback"))) if _tl("KeyboardButtonCallback") else "MISSING",
                 sorted(_params(_tl("KeyboardButtonStyle"))) if HAS_STYLE else "MISSING")
    except Exception:
        log.exception("telethon debug info failed")
    log.info("Bot @%s is running (Firestore connected)", me.username)
    await client.run_until_disconnected()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
