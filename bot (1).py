"""
🎬 Funny Video Bot — Firebase (Firestore) database + Render free web service
Telegram Bot API 9.4 coloured inline buttons.
Owner + limited admins, force-join, protected & auto-deleting videos, branding.
"""
import os
import sys
import io
import csv
import json
import time
import html
import base64
import signal
import hashlib
import asyncio
import logging

import httpx
from aiohttp import web
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.oauth2 import service_account

from telegram import (
    Update, InlineKeyboardButton as IKB, InlineKeyboardMarkup as IKM, BotCommand,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove,
)
from telegram.constants import ParseMode, ChatMemberStatus as CMS
from telegram.error import BadRequest, TelegramError, RetryAfter, NetworkError
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler, TypeHandler,
    ContextTypes, filters,
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("funnybot")

# ─────────────────────────── CONFIG ───────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = os.environ.get("OWNER_ID")
if not BOT_TOKEN or not OWNER_ID:
    raise SystemExit("BOT_TOKEN aur OWNER_ID set karo (Render Environment ya .env file).")
OWNER_ID = int(OWNER_ID)
PORT = int(os.environ.get("PORT", "10000"))
# Render sets RENDER_EXTERNAL_URL automatically. Locally leave empty -> polling mode.
BASE_URL = (os.environ.get("WEBHOOK_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")
SECRET = os.environ.get("WEBHOOK_SECRET") or hashlib.sha256(BOT_TOKEN.encode()).hexdigest()[:48]
H = ParseMode.HTML


def load_firebase_info():
    raw = os.environ.get("FIREBASE_CREDENTIALS_B64") or os.environ.get("FIREBASE_CREDENTIALS")
    if raw:
        raw = raw.strip()
        if not raw.startswith("{"):
            raw = base64.b64decode(raw).decode("utf-8")
        return json.loads(raw)
    path = os.environ.get("FIREBASE_CREDENTIALS_FILE")
    if path:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    raise SystemExit("Firebase credentials missing: FIREBASE_CREDENTIALS_B64 set karo.")


# ─────────────────────────── FIRESTORE + CACHE ───────────────────────────
# Firestore free quota is limited (50k reads / 20k writes per day), so everything
# is cached in memory and written through. The bot runs as a single instance.
fs: firestore.AsyncClient = None
S = {}            # settings
ADMINS = {}       # uid -> {"name": str, "perms": set}
CATS = []         # [{"id","name","style","position"}]
VIDS = {}         # category id -> [{"id","file_id","kind","added_at"}]  (lazy)
CHANNELS = {}     # chat_id -> {"title","link"}
BANNED = set()
KNOWN = set()     # users already registered in this process
PENDING = []      # [{"doc","chat_id","message_id","at"}]
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
        CHANNELS[int(c.id)] = {"title": x.get("title", c.id), "link": x.get("link")}
    async for u in fs.collection("users").where(filter=FieldFilter("banned", "==", True)).stream():
        BANNED.add(int(u.id))
    async for p in fs.collection("pending_deletes").stream():
        x = p.to_dict() or {}
        PENDING.append({"doc": p.reference, "chat_id": x["chat_id"], "message_id": x["message_id"], "at": x["at"]})
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
    ids = []
    async for v in fs.collection("videos").where(filter=FieldFilter("category_id", "==", cid)).stream():
        ids.append(v.reference)
    for i in range(0, len(ids), 400):
        batch = fs.batch()
        for ref in ids[i:i + 400]:
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
async def register(u):
    if u.id in KNOWN:
        return
    ref = fs.collection("users").document(str(u.id))
    snap = await ref.get()
    data = {"name": u.full_name, "username": u.username}
    if not snap.exists:
        data["joined_at"] = int(time.time())
        _COUNT.clear()
    await ref.set(data, merge=True)
    KNOWN.add(u.id)


async def set_ban(tid, flag):
    await fs.collection("users").document(str(tid)).set({"banned": flag}, merge=True)
    (BANNED.add if flag else BANNED.discard)(tid)
    _COUNT.clear()


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
        batch.set(ref, {"chat_id": chat_id, "message_id": mid, "at": at})
        PENDING.append({"doc": ref, "chat_id": chat_id, "message_id": mid, "at": at})
    await batch.commit()


async def sweeper(bot):
    while True:
        await asyncio.sleep(10)
        try:
            now = int(time.time())
            due = [p for p in PENDING if p["at"] <= now]
            if not due:
                continue
            for p in due:
                PENDING.remove(p)
                try:
                    await bot.delete_message(p["chat_id"], p["message_id"])
                except TelegramError:
                    pass
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


# ─────────────────────────── UI HELPERS ───────────────────────────
def B(text, data=None, style=None, url=None):
    """Inline button. `style` = primary (blue) / success (green) / danger (red)  -> Bot API 9.4."""
    kw = {"api_kwargs": {"style": style}} if style else {}
    if url:
        return IKB(text, url=url, **kw)
    return IKB(text, callback_data=data, **kw)


def back(to="a:home"):
    return [B("⬅️ Back", to)]


ADMIN_BTN = "⚙️ Admin Panel"


def KB(text, style=None):
    """Reply-keyboard button (bottom keyboard). Coloured via Bot API 9.4 `style`."""
    return KeyboardButton(text, api_kwargs={"style": style}) if style else KeyboardButton(text)


def menu_kb(uid):
    """Persistent bottom keyboard: one button per video category (+ Admin Panel for staff)."""
    rows_ = chunk([KB(c["name"], c["style"] or None) for c in CATS], 2)
    if is_staff(uid):
        rows_.append([KB(ADMIN_BTN, "primary")])
    if not rows_:
        return ReplyKeyboardRemove()
    return ReplyKeyboardMarkup(rows_, resize_keyboard=True, is_persistent=True,
                               input_field_placeholder="Neeche se video chuno 👇")


def esc(s):
    return html.escape(str(s))


def chunk(lst, n=2):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


async def show(q, text, kb):
    try:
        await q.edit_message_text(text, reply_markup=IKM(kb), parse_mode=H)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise


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
async def missing_channels(bot, uid):
    missing = []
    for chat_id, ch in list(CHANNELS.items()):
        try:
            m = await bot.get_chat_member(chat_id, uid)
            ok = m.status in (CMS.MEMBER, CMS.ADMINISTRATOR, CMS.OWNER) or \
                (m.status == CMS.RESTRICTED and getattr(m, "is_member", False))
            if not ok:
                missing.append(ch)
        except TelegramError as e:
            log.warning("force-join check failed for %s: %s", chat_id, e)
    return missing


async def gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    u = update.effective_user
    if is_staff(u.id):
        return True
    if u.id in BANNED:
        await context.bot.send_message(u.id, "🚫 Tumhe is bot se ban kar diya gaya hai.")
        return False
    missing = await missing_channels(context.bot, u.id)
    if not missing:
        return True
    kb = [[B(f"📢 Join {c['title']}", url=c["link"], style="primary")] for c in missing if c["link"]]
    kb.append([B("✅ Maine Join Kar Liya", "chk", "success")])
    await context.bot.send_message(
        u.id, "🔐 <b>Pehle ye channels join karo</b>, phir videos unlock honge 👇",
        reply_markup=IKM(kb), parse_mode=H)
    return False


# ─────────────────────────── USER SIDE ───────────────────────────
async def send_home(bot, uid, name):
    text = get("welcome").replace("{name}", esc(name))
    if not CATS:
        text += "\n\n<i>Abhi koi video nahi hai, jaldi aayenge! 🎬</i>"
    await bot.send_message(uid, text, reply_markup=menu_kb(uid), parse_mode=H)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await register(u)
    if not await gate(update, context):
        return
    await send_home(context.bot, u.id, u.full_name)


async def check_join_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    u = q.from_user
    if await missing_channels(context.bot, u.id):
        await q.answer("❌ Abhi tak sab channels join nahi kiye!", show_alert=True)
        return
    await q.answer("✅ Verified!")
    try:
        await q.message.delete()
    except TelegramError:
        pass
    await send_home(context.bot, u.id, u.full_name)


async def user_home_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await send_home(context.bot, q.from_user.id, q.from_user.full_name)


async def send_media(bot, chat_id, v, caption, protect):
    kw = dict(caption=caption or None, parse_mode=H, protect_content=protect)
    if v["kind"] == "animation":
        return await bot.send_animation(chat_id, v["file_id"], **kw)
    if v["kind"] == "document":
        return await bot.send_document(chat_id, v["file_id"], **kw)
    return await bot.send_video(chat_id, v["file_id"], supports_streaming=True, **kw)


async def user_video_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Old inline buttons (from earlier messages) still work."""
    q = update.callback_query
    cid = q.data.split(":")[1]
    if not await vids_of(cid):
        await q.answer("Is button me abhi koi video nahi hai 😅", show_alert=True)
        return
    await q.answer("🎬 Video aa raha hai...")
    await deliver(update, context, cid)


async def menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """A normal user tapped a bottom-keyboard button (it arrives as plain text)."""
    m = update.message
    text = (m.text or "").strip()
    if not text:
        return
    c = next((c for c in CATS if c["name"] == text), None)
    if c:
        await deliver(update, context, c["id"])
    else:
        # stale keyboard (admin changed buttons) -> push the fresh one
        await m.reply_text("🔄 Menu update ho gaya hai, neeche se naya button chuno 👇",
                           reply_markup=menu_kb(m.from_user.id))


async def deliver(update: Update, context: ContextTypes.DEFAULT_TYPE, cid):
    u = update.effective_user
    vids = (await vids_of(cid))[:30]
    if not vids:
        await context.bot.send_message(u.id, "Is button me abhi koi video nahi hai 😅", reply_markup=menu_kb(u.id))
        return
    if not await gate(update, context):
        return
    protect = get("protect") == "1"
    minutes = int(get("timer"))
    caption = get("brand")
    sent = []
    for v in vids:
        try:
            m = await send_media(context.bot, u.id, v, caption, protect)
            sent.append(m.message_id)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except TelegramError as e:
            log.warning("send failed: %s", e)
        await asyncio.sleep(0.3)
    if minutes > 0:
        txt = f"⏳ <i>Ye video <b>{minutes} minute</b> me automatically delete ho jayega.</i>"
    else:
        txt = "😂 Aur chahiye? Neeche se button chuno 👇"
    n = await context.bot.send_message(u.id, txt, reply_markup=menu_kb(u.id), parse_mode=H)
    if minutes > 0 and sent:
        sent.append(n.message_id)
        await schedule_delete(u.id, sent, minutes)


# ─────────────────────────── ADMIN PANEL ───────────────────────────
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_staff(uid):
        return
    context.user_data.pop("state", None)
    await update.message.reply_text(await panel_text(uid), reply_markup=IKM(main_kb(uid)), parse_mode=H)


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


async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    p = q.data.split(":")
    sec = p[1]
    sub = p[2] if len(p) > 2 else None
    if not is_staff(uid):
        await q.answer("⛔ Access nahi hai", show_alert=True)
        return
    need = SEC_PERM.get(sec)
    if (sec in OWNER_ONLY and not is_owner(uid)) or (need and not has_perm(uid, need)):
        await q.answer("⛔ Is feature ki permission nahi hai", show_alert=True)
        return
    await q.answer()
    ud = context.user_data
    ud.pop("state", None)

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
            ud["state"] = ("upload", c["id"])
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
            ud["state"] = ("newbtn",)
            await show(q, "➕ Naye button ka <b>naam</b> bhejo (emoji bhi laga sakte ho):", [back("a:btns")])
        elif sub in ("view", "col", "up", "ren", "del", "delok"):
            cid = p[3]
            c = cat_get(cid)
            if not c:
                await show(q, "Button nahi mila.", [back("a:btns")])
                return
            if sub == "ren":
                ud["state"] = ("rename", cid)
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
            ud["state"] = ("brand",)
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
            ud["state"] = ("timer",)
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
            ud["state"] = ("fjadd",)
            await show(q, "📢 Channel/group ka <b>@username</b>, link ya ID bhejo (ya uska koi message forward karo).\n\n⚠️ Bot ko pehle us channel me <b>admin</b> bana do.", [back("a:fj")])
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
            ud["state"] = ("admadd",)
            await show(q, "👥 Naye admin ki <b>Telegram user ID</b> bhejo.\n(ID ke liye wo @userinfobot use kar sakta hai)", [back("a:adm")])
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
            if not is_owner(uid):
                await q.answer("⛔ Sirf owner export kar sakta hai", show_alert=True)
                return
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["user_id", "name", "username", "joined_at_utc", "banned"])
            n = 0
            async for d in fs.collection("users").stream():
                x = d.to_dict() or {}
                joined = time.strftime("%Y-%m-%d %H:%M", time.gmtime(x["joined_at"])) if x.get("joined_at") else ""
                w.writerow([d.id, x.get("name", ""), x.get("username") or "", joined, int(bool(x.get("banned")))])
                n += 1
            await context.bot.send_document(uid, document=buf.getvalue().encode("utf-8-sig"),
                                            filename="users.csv", caption=f"👥 {n} users")
            return
        n_users = await count("users", ttl=0)
        n_ban = len(BANNED)
        n_vid = await count("videos", ttl=0)
        text = (f"📊 <b>Statistics</b>\n━━━━━━━━━━━━━━\n👥 Users: <b>{n_users}</b>\n🚫 Banned: <b>{n_ban}</b>\n"
                f"🔘 Buttons: <b>{len(CATS)}</b>\n🎬 Videos: <b>{n_vid}</b>\n🛡 Admins: <b>{len(ADMINS)}</b>\n📢 Force-join: <b>{len(CHANNELS)}</b>")
        kb = [[B("📥 Export Users (CSV)", "a:stats:export", "success")]] if is_owner(uid) else []
        kb.append(back())
        await show(q, text, kb)

    # ---- broadcast
    elif sec == "bc":
        ud["state"] = ("bc",)
        await show(q, "📣 Jo message sab users ko bhejna hai wo bhejo (text / photo / video, kuch bhi).", [back()])

    # ---- ban
    elif sec == "ban":
        if sub in ("b", "u"):
            ud["state"] = ("ban" if sub == "b" else "unban",)
            await show(q, "User ki <b>ID</b> bhejo:", [back("a:ban")])
        else:
            await show(q, "🚫 <b>Ban / Unban</b>", [[B("🚫 Ban", "a:ban:b", "danger"), B("✅ Unban", "a:ban:u", "success")], back()])

    # ---- welcome
    elif sec == "welcome":
        if sub == "set":
            ud["state"] = ("welcome",)
            await show(q, "👋 Naya welcome message bhejo. User ka naam lagane ke liye <code>{name}</code> likho.", [back("a:welcome")])
        elif sub == "rst":
            await put("welcome", DEFAULTS["welcome"])
            await show(q, "♻️ Welcome message reset ho gaya.", [back("a:welcome")])
        else:
            kb = [[B("✏️ Edit", "a:welcome:set", "success"), B("♻️ Reset", "a:welcome:rst", "danger")], back()]
            await show(q, "👋 <b>Welcome Message</b>\n\n" + get("welcome"), kb)


# ─────────────────────────── ADMIN INPUT (messages) ───────────────────────────
def media_of(m):
    if m.video:
        return "video", m.video.file_id
    if m.animation:
        return "animation", m.animation.file_id
    if m.document and (m.document.mime_type or "").startswith("video"):
        return "document", m.document.file_id
    return None, None


async def do_broadcast(bot, admin_chat, from_chat, msg_id):
    ok = fail = 0
    try:
        async for u in fs.collection("users").stream():
            if (u.to_dict() or {}).get("banned"):
                continue
            for _ in range(2):
                try:
                    await bot.copy_message(int(u.id), from_chat, msg_id)
                    ok += 1
                    break
                except RetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                except TelegramError:
                    fail += 1
                    break
            await asyncio.sleep(0.05)
    except Exception:
        log.exception("broadcast error")
    await bot.send_message(admin_chat, f"📣 <b>Broadcast complete</b>\n✅ Sent: {ok}\n❌ Failed: {fail}", parse_mode=H)


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.message
    uid = m.from_user.id
    if m.text == ADMIN_BTN and is_staff(uid):
        context.user_data.pop("state", None)
        await m.reply_text(await panel_text(uid), reply_markup=IKM(main_kb(uid)), parse_mode=H)
        return
    st = context.user_data.get("state")
    if not st or not is_staff(uid):
        await menu_text(update, context)
        return
    kind = st[0]

    if kind == "upload":
        if not has_perm(uid, "upload"):
            return
        mk, fid = media_of(m)
        if not fid:
            await m.reply_text("❌ Sirf video bhejo (ya Done dabao).")
            return
        n = await add_video(st[1], fid, mk, uid)
        await m.reply_text(f"✅ Video save ho gayi! (is button me total {n})\nAur bhejo ya Done dabao.",
                           reply_markup=IKM([[B("✅ Done", "a:home", "success")]]))

    elif kind == "newbtn":
        if not has_perm(uid, "buttons") or not m.text:
            return
        cid = await create_cat(m.text.strip()[:40])
        context.user_data.pop("state")
        await m.reply_text("✅ Button ban gaya! Ab isme video upload karo 👇",
                           reply_markup=IKM([[B("📤 Upload Here", f"a:upload:cat:{cid}", "success")], back()]))

    elif kind == "rename":
        if not has_perm(uid, "buttons") or not m.text:
            return
        await update_cat(st[1], name=m.text.strip()[:40])
        context.user_data.pop("state")
        await m.reply_text("✅ Naam badal gaya!", reply_markup=IKM([back("a:btns")]))

    elif kind == "brand":
        if not has_perm(uid, "branding") or not m.text:
            return
        await put("brand", m.text_html[:900])
        context.user_data.pop("state")
        await m.reply_text("✅ Branding save ho gayi!", reply_markup=IKM([back("a:brand")]))

    elif kind == "timer":
        if not has_perm(uid, "timer") or not m.text or not m.text.strip().isdigit() or int(m.text) > 1440:
            await m.reply_text("❌ 0 se 1440 ke beech number bhejo.")
            return
        await put("timer", str(int(m.text)))
        context.user_data.pop("state")
        await m.reply_text("✅ Timer set ho gaya!", reply_markup=IKM([back("a:timer")]))

    elif kind == "welcome":
        if not has_perm(uid, "welcome") or not m.text:
            return
        await put("welcome", m.text_html)
        context.user_data.pop("state")
        await m.reply_text("✅ Welcome message save ho gaya!", reply_markup=IKM([back("a:welcome")]))

    elif kind == "bc":
        if not has_perm(uid, "broadcast"):
            return
        context.user_data.pop("state")
        await m.reply_text("📣 Broadcast shuru ho gaya, khatam hone par report milegi.")
        context.application.create_task(do_broadcast(context.bot, m.chat_id, m.chat_id, m.message_id))

    elif kind in ("ban", "unban"):
        if not has_perm(uid, "ban") or not m.text or not m.text.strip().isdigit():
            await m.reply_text("❌ Sirf numeric user ID bhejo.")
            return
        tid = int(m.text.strip())
        if is_owner(tid):
            await m.reply_text("👑 Owner ko ban nahi kar sakte.")
            return
        await set_ban(tid, kind == "ban")
        context.user_data.pop("state")
        await m.reply_text("🚫 Banned." if kind == "ban" else "✅ Unbanned.", reply_markup=IKM([back("a:ban")]))

    elif kind == "fjadd":
        if not is_owner(uid):
            return
        ref = None
        fo = getattr(m, "forward_origin", None)
        if fo is not None and getattr(fo, "chat", None):
            ref = fo.chat.id
        elif m.text:
            t = m.text.strip()
            if "t.me/" in t:
                t = "@" + t.split("t.me/")[-1].strip("/").split("?")[0]
            ref = int(t) if t.lstrip("-").isdigit() else (t if t.startswith("@") else "@" + t)
        if ref is None:
            await m.reply_text("❌ Channel ka @username / ID bhejo.")
            return
        try:
            chat = await context.bot.get_chat(ref)
            me = await context.bot.get_chat_member(chat.id, context.bot.id)
            if me.status not in (CMS.ADMINISTRATOR, CMS.OWNER):
                await m.reply_text("❌ Bot us channel me admin nahi hai. Pehle admin banao, phir dubara bhejo.")
                return
            link = f"https://t.me/{chat.username}" if chat.username else chat.invite_link
            if not link:
                try:
                    link = await context.bot.export_chat_invite_link(chat.id)
                except TelegramError:
                    link = None
            title = chat.title or str(chat.id)
            CHANNELS[chat.id] = {"title": title, "link": link}
            await fs.collection("channels").document(str(chat.id)).set({"title": title, "link": link})
        except TelegramError as e:
            await m.reply_text(f"❌ Channel nahi mila: {esc(e)}", parse_mode=H)
            return
        context.user_data.pop("state")
        await m.reply_text(f"✅ <b>{esc(title)}</b> force-join me add ho gaya!", reply_markup=IKM([back("a:fj")]), parse_mode=H)

    elif kind == "admadd":
        if not is_owner(uid) or not m.text or not m.text.strip().isdigit():
            await m.reply_text("❌ Sirf numeric user ID bhejo.")
            return
        tid = int(m.text.strip())
        if is_owner(tid):
            await m.reply_text("👑 Owner already sabse upar hai.")
            return
        if tid not in ADMINS:
            name = str(tid)
            try:
                c = await context.bot.get_chat(tid)
                name = c.full_name or c.title or name
            except TelegramError:
                pass
            ADMINS[tid] = {"name": name, "perms": {"upload"}}
            await save_admin(tid)
        context.user_data.pop("state")
        text, kb = adm_view(tid)
        await m.reply_text("✅ Admin add ho gaya! Ab permissions set karo.\n\n" + text, reply_markup=IKM(kb), parse_mode=H)


async def track(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save every private-chat user to Firestore on first contact (any message or tap)."""
    u = update.effective_user
    chat = update.effective_chat
    if u and not u.is_bot and chat and chat.type == "private":
        try:
            await register(u)
        except Exception:
            log.exception("register failed")


async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    log.error("Handler error", exc_info=context.error)


# ─────────────────────────── WEB SERVER + MAIN ───────────────────────────
APP: Application = None


async def health(request):
    return web.Response(text="ok")


async def handle_webhook(request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != SECRET:
        return web.Response(status=403)
    if APP is None:
        return web.Response(status=503)
    update = Update.de_json(await request.json(), APP.bot)
    await APP.update_queue.put(update)
    return web.Response(text="ok")


async def keepalive(url):
    """Render free web services sleep after ~15 min without inbound traffic."""
    async with httpx.AsyncClient(timeout=20) as c:
        while True:
            await asyncio.sleep(600)
            try:
                await c.get(url + "/health")
            except Exception as e:
                log.warning("keepalive failed: %s", e)


async def retry(fn, what, tries=10):
    """Telegram can be slow/unreachable right after a Render cold start: retry instead of crashing."""
    for i in range(1, tries + 1):
        try:
            return await fn()
        except NetworkError as e:
            if i == tries:
                raise
            wait = min(5 * i, 30)
            log.warning("%s failed (%s), retry %d/%d in %ds", what, type(e).__name__, i, tries, wait)
            await asyncio.sleep(wait)


def build_app():
    # Render free instances have very little CPU, so the default 5s timeouts are too tight.
    app = (Application.builder().token(BOT_TOKEN).concurrent_updates(True)
           .connect_timeout(30).read_timeout(40).write_timeout(40).pool_timeout(30)
           .media_write_timeout(90)
           .build())
    app.add_handler(TypeHandler(Update, track), group=-1)
    app.add_handler(CommandHandler("start", start, filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("admin", admin_cmd, filters.ChatType.PRIVATE))
    app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^a:"))
    app.add_handler(CallbackQueryHandler(user_video_cb, pattern=r"^v:\w+$"))
    app.add_handler(CallbackQueryHandler(check_join_cb, pattern=r"^chk$"))
    app.add_handler(CallbackQueryHandler(user_home_cb, pattern=r"^uhome$"))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_message))
    app.add_error_handler(on_error)
    return app


async def amain():
    global fs, APP
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, stop.set)
        except NotImplementedError:
            pass

    # Bind the port first so Render's port scan succeeds quickly.
    web_app = web.Application()
    web_app.router.add_get("/", health)
    web_app.router.add_get("/health", health)
    web_app.router.add_post("/tg", handle_webhook)
    runner = web.AppRunner(web_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    fs = make_client()
    await load_all()

    APP = build_app()
    await retry(APP.initialize, "initialize (get_me)")
    await APP.start()
    await retry(lambda: APP.bot.set_my_commands(
        [BotCommand("start", "Bot start karo"), BotCommand("admin", "Admin panel")]), "set_my_commands")

    tasks = [asyncio.create_task(sweeper(APP.bot))]
    if BASE_URL:
        await retry(lambda: APP.bot.set_webhook(
            f"{BASE_URL}/tg", secret_token=SECRET, allowed_updates=Update.ALL_TYPES), "set_webhook")
        tasks.append(asyncio.create_task(keepalive(BASE_URL)))
        log.info("Webhook mode: %s/tg", BASE_URL)
    else:
        await APP.bot.delete_webhook()
        await APP.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        log.info("Polling mode (no RENDER_EXTERNAL_URL / WEBHOOK_URL set)")

    await stop.wait()
    log.info("Shutting down...")
    for t in tasks:
        t.cancel()
    if APP.updater.running:
        await APP.updater.stop()
    await APP.stop()
    await APP.shutdown()
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(amain())
