# main.py
"""
Telegram shop bot — final version with Crypto Pay integration and product delivery types.
Requirements:
  pip install aiogram aiosqlite aiohttp
Run:
  python main.py

Notes:
- Single admin (ADMIN_ID).
- No webhooks required. Uses Crypto Pay API polling/checking for invoice status at user request.
- Crypto Pay API token must be set in CRYPTO_API_TOKEN.
- All user-visible texts stored in DB (texts table) and editable via admin panel.
"""

import asyncio
import logging
import aiosqlite
import json
import time
import csv
import os
from typing import Optional, List, Dict, Any

import aiohttp

from aiogram import Bot, Dispatcher
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    Message,
    FSInputFile,
    InputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import BaseFilter, Command

# -----------------------------
# CONFIG (tokens provided by you)
# -----------------------------
BOT_TOKEN = "8246983928:AAH9BRXupUHBQf0c0oSn45Owlr5GV3VWW8E"
ADMIN_ID = 1627227943
CRYPTO_API_TOKEN = "459309:AAseBlc4ZXsFTmrhsHkhuPavf1vvm4EfcUX"  # Crypto Pay app token (from Crypto Bot)
DB_PATH = "shop.db"
START_ORDER_NUMBER = 1000  # first order will get number 1001
TICKET_BASE_NUMBER = 100
CURRENCY = "RUB"  # fiat currency to show; Crypto Pay supports fiat param

# Crypto Pay base URL (mainnet)
CRYPTO_BASE_URL = "https://pay.crypt.bot/api"

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -----------------------------
# FSM states
# -----------------------------
class CheckoutStates(StatesGroup):
    waiting_contact = State()
    waiting_comment = State()

class TicketStates(StatesGroup):
    waiting_text = State()

class ProductCreateStates(StatesGroup):
    title = State()
    description = State()
    price = State()
    choose_type = State()
    extra = State()  # for text or file
    upload_images = State()
    preview = State()

class ProductEditStates(StatesGroup):
    choose_field = State()
    new_value = State()
    upload_images = State()
    preview = State()

class TextEditStates(StatesGroup):
    key = State()
    value = State()
    preview = State()

class BroadcastStates(StatesGroup):
    message = State()
    preview = State()

class SendUserStates(StatesGroup):
    username = State()
    message = State()
    preview = State()

class TicketReplyStates(StatesGroup):
    reply = State()

class PromoCreateStates(StatesGroup):
    code = State()
    percent = State()

# -----------------------------
# Admin filter
# -----------------------------
class IsAdminFilter(BaseFilter):
    async def __call__(self, obj, state: FSMContext = None) -> bool:
        user_id = None
        if isinstance(obj, Message):
            user_id = obj.from_user.id
        elif isinstance(obj, CallbackQuery):
            user_id = obj.from_user.id
        return user_id == ADMIN_ID

is_admin = IsAdminFilter()

# -----------------------------
# DB wrapper
# -----------------------------
class DB:
    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON;")
        await self._conn.commit()

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def execute(self, sql: str, params: tuple = ()):
        cur = await self._conn.execute(sql, params)
        await self._conn.commit()
        return cur

    async def fetchone(self, sql: str, params: tuple = ()):
        cur = await self._conn.execute(sql, params)
        row = await cur.fetchone()
        await cur.close()
        return row

    async def fetchall(self, sql: str, params: tuple = ()):
        cur = await self._conn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows

# -----------------------------
# Initialize DB schema
# -----------------------------
async def init_db(db: DB):
    # Basic tables
    await db.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        updated_at INTEGER
    );""")
    await db.execute("""
    CREATE TABLE IF NOT EXISTS texts (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );""")
    await db.execute("""
    CREATE TABLE IF NOT EXISTS admins (
        tg_id INTEGER PRIMARY KEY
    );""")
    await db.execute("INSERT OR IGNORE INTO admins (tg_id) VALUES (?)", (ADMIN_ID,))

    # Categories and products
    await db.execute("""
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        parent_id INTEGER DEFAULT NULL,
        FOREIGN KEY(parent_id) REFERENCES categories(id) ON DELETE SET NULL
    );""")

    # products now include extra_text and file_id columns for delivery types
    # use CREATE TABLE with columns; if table exists but missing columns, attempt ALTER
    await db.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category_id INTEGER,
        title TEXT NOT NULL,
        description TEXT,
        price INTEGER NOT NULL,
        product_type TEXT DEFAULT 'digital',  -- 'manual', 'text', 'file'
        file_id TEXT DEFAULT NULL,            -- used for file-type products
        extra_text TEXT DEFAULT NULL,         -- used for text-type products
        infinite_stock INTEGER DEFAULT 1,
        created_at INTEGER,
        FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE SET NULL
    );""")

    await db.execute("""
    CREATE TABLE IF NOT EXISTS product_images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        file_id TEXT,
        position INTEGER DEFAULT 0,
        FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
    );""")

    # cart / orders
    await db.execute("""
    CREATE TABLE IF NOT EXISTS cart (
        user_id INTEGER,
        product_id INTEGER,
        qty INTEGER,
        PRIMARY KEY(user_id, product_id)
    );""")
    await db.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_number INTEGER UNIQUE,
        user_id INTEGER,
        items_json TEXT,
        total INTEGER,
        status TEXT DEFAULT 'created',
        contact TEXT,
        comment TEXT,
        created_at INTEGER,
        payment_invoice_id TEXT DEFAULT NULL
    );""")

    # tickets / logs / promo
    await db.execute("""
    CREATE TABLE IF NOT EXISTS tickets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        subject TEXT,
        status TEXT DEFAULT 'open',
        created_at INTEGER
    );""")
    await db.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER,
        level TEXT,
        source TEXT,
        message TEXT
    );""")
    await db.execute("""
    CREATE TABLE IF NOT EXISTS promo_codes (
        code TEXT PRIMARY KEY,
        discount_percent INTEGER DEFAULT 0,
        active INTEGER DEFAULT 1
    );""")

    # Defaults for texts
    defaults = {
        "welcome_message": "Привет! Добро пожаловать в магазин. Выберите действие ниже.",
        "menu_catalog": "Каталог",
        "menu_cart": "Корзина",
        "menu_orders": "Мои заказы",
        "menu_support": "Техподдержка",
        "menu_admin": "Админ",
        "button_checkout": "Оформить заказ",
        "button_confirm_payment": "Я оплатил(а)",
        "button_check_payment": "Проверить оплату",
        "payment_success_text": "Оплата подтверждена! Ваш заказ №{order_number}. Администратор свяжется с вами.",
        "payment_pending_text": "Платёж ожидается. Нажмите «Проверить оплату» после оплаты.",
        "empty_cart_text": "Ваша корзина пуста.",
        "added_to_cart_text": "Товар добавлен в корзину.",
        "admin_only_text": "Это меню доступно только администратору."
    }
    for k, v in defaults.items():
        await db.execute("INSERT OR IGNORE INTO texts (key, value) VALUES (?, ?)", (k, v))

    # Ensure columns exist (in case of upgrade from older schema)
    # Try to add columns if missing; ignore errors
    try:
        await db.execute("ALTER TABLE products ADD COLUMN file_id TEXT DEFAULT NULL")
    except Exception:
        pass
    try:
        await db.execute("ALTER TABLE products ADD COLUMN extra_text TEXT DEFAULT NULL")
    except Exception:
        pass

# -----------------------------
# Helpers
# -----------------------------
async def get_text(db: DB, key: str) -> str:
    row = await db.fetchone("SELECT value FROM texts WHERE key = ?", (key,))
    return row["value"] if row else f"[missing text: {key}]"

async def set_text(db: DB, key: str, value: str):
    await db.execute("INSERT INTO texts (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

async def add_log(db: DB, level: str, source: str, message: str):
    await db.execute("INSERT INTO logs (ts, level, source, message) VALUES (?, ?, ?, ?)", (int(time.time()), level, source, message))

async def io_price_to_display(price_int: int) -> str:
    rub = price_int // 100
    kop = price_int % 100
    return f"{rub}₽ {kop:02d}к"

async def next_order_number(db: DB) -> int:
    row = await db.fetchone("SELECT MAX(order_number) as maxn FROM orders")
    maxn = row["maxn"] if row and row["maxn"] else START_ORDER_NUMBER
    return int(maxn) + 1

def ticket_display_number(tid: int) -> str:
    return f"TIKET#{TICKET_BASE_NUMBER + tid}"

STATUS_RU = {
    "created": "Создан",
    "paid": "Оплачен",
    "processing": "В обработке",
    "shipped": "Отправлен",
    "cancelled": "Отменён"
}

def build_kb(rows: List[List[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=rows)

# -----------------------------
# Crypto Pay client using aiohttp
# -----------------------------
class CryptoPayHTTP:
    def __init__(self, token: str, session: aiohttp.ClientSession):
        self.token = token
        self.session = session
        self.base = CRYPTO_BASE_URL

    async def create_invoice(self, amount: float, description: str = "", payload: Optional[dict] = None, fiat: Optional[str] = "RUB") -> Optional[Dict[str, Any]]:
        """
        Create invoice via Crypto Pay API.
        amount: float (fiat amount)
        fiat: fiat currency code, e.g. 'RUB'
        Returns dict with invoice_id and bot_invoice_url on success, else None.
        """
        url = f"{self.base}/createInvoice"
        headers = {"Crypto-Pay-API-Token": self.token}
        data = {
            "amount": str(amount),
            "currency_type": "fiat",
            "fiat": fiat,
            "description": description,
            "payload": json.dumps(payload) if payload else "",
            "allow_comments": True
        }
        try:
            async with self.session.post(url, json=data, headers=headers, timeout=15) as resp:
                text = await resp.text()
                j = None
                try:
                    j = await resp.json()
                except Exception:
                    logging.error("CryptoPay create_invoice: invalid json: %s", text)
                    return None
                if j.get("ok"):
                    res = j.get("result", {})
                    # prefer bot_invoice_url or web_app_invoice_url or pay_url
                    pay_url = res.get("bot_invoice_url") or res.get("web_app_invoice_url") or res.get("pay_url") or res.get("bot_invoice_url")
                    return {"invoice_id": str(res.get("invoice_id") or res.get("id") or ""), "pay_url": pay_url}
                else:
                    logging.error("CryptoPay create_invoice failed: %s", j.get("error"))
                    return None
        except Exception:
            logging.exception("CryptoPay create_invoice exception")
            return None

    async def get_invoices_by_ids(self, invoice_ids: List[str]) -> Optional[Dict[str, Any]]:
        """
        Call getInvoices with invoice_ids param (comma-separated).
        Returns the raw response dict if ok, else None.
        """
        url = f"{self.base}/getInvoices"
        headers = {"Crypto-Pay-API-Token": self.token}
        params = {"invoice_ids": ",".join(invoice_ids)}
        try:
            async with self.session.get(url, params=params, headers=headers, timeout=15) as resp:
                j = await resp.json()
                if j.get("ok"):
                    return j.get("result", {})
                else:
                    logging.error("CryptoPay getInvoices error: %s", j.get("error"))
                    return None
        except Exception:
            logging.exception("CryptoPay getInvoices exception")
            return None

# -----------------------------
# Safe edit/delete utils
# -----------------------------
async def safe_edit_message(callback: CallbackQuery, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None, parse_mode=ParseMode.HTML):
    try:
        await callback.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception:
        try:
            await callback.message.answer(text, reply_markup=reply_markup)
        except Exception:
            pass
    finally:
        try:
            await callback.answer()
        except Exception:
            pass

async def safe_delete_message(message: Message):
    try:
        await message.delete()
    except Exception:
        pass

# -----------------------------
# Main bot
# -----------------------------
async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    db = DB(DB_PATH)
    await db.connect()
    await init_db(db)

    session = aiohttp.ClientSession()
    crypto = CryptoPayHTTP(CRYPTO_API_TOKEN, session)

    # Upsert user helper
    async def upsert_user(user):
        await db.execute(
            "INSERT INTO users (id, username, first_name, last_name, updated_at) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name, updated_at=excluded.updated_at",
            (user.id, getattr(user, "username", None), getattr(user, "first_name", None), getattr(user, "last_name", None), int(time.time()))
        )

    # Main menu builder
    async def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
        t_catalog = await get_text(db, "menu_catalog")
        t_cart = await get_text(db, "menu_cart")
        t_orders = await get_text(db, "menu_orders")
        t_support = await get_text(db, "menu_support")
        t_admin = await get_text(db, "menu_admin")
        rows = [
            [InlineKeyboardButton(text=t_catalog, callback_data="menu:catalog"),
             InlineKeyboardButton(text=t_cart, callback_data="menu:cart")],
            [InlineKeyboardButton(text=t_orders, callback_data="menu:orders"),
             InlineKeyboardButton(text=t_support, callback_data="menu:support")]
        ]
        if user_id == ADMIN_ID:
            rows.append([InlineKeyboardButton(text=t_admin, callback_data="menu:admin")])
        return build_kb(rows)

    # /start
    @dp.message(Command("start"))
    async def cmd_start(message: Message, state: FSMContext):
        await upsert_user(message.from_user)
        txt = await get_text(db, "welcome_message")
        kb = await main_menu_kb(message.from_user.id)
        await message.answer(txt, reply_markup=kb)
        await add_log(db, "INFO", "user_start", f"user {message.from_user.id} started")

    # Menu router
    @dp.callback_query(lambda c: c.data and c.data.startswith("menu:"))
    async def menu_router(callback: CallbackQuery, state: FSMContext):
        cmd = callback.data.split(":", 1)[1]
        if cmd == "main":
            kb = await main_menu_kb(callback.from_user.id)
            await safe_edit_message(callback, await get_text(db, "welcome_message"), reply_markup=kb)
            return
        if cmd == "catalog":
            await show_catalog_menu(callback, state)
            return
        if cmd == "cart":
            await show_cart(callback, state)
            return
        if cmd == "orders":
            await user_orders_list(callback, state)
            return
        if cmd == "support":
            kb = build_kb([[InlineKeyboardButton(text="Создать тикет", callback_data="ticket:create")],
                            [InlineKeyboardButton(text="Мои тикеты", callback_data="ticket:mytickets")],
                            [InlineKeyboardButton(text="В главное меню", callback_data="menu:main")]])
            await safe_edit_message(callback, "Техподдержка:", reply_markup=kb)
            return
        if cmd == "admin":
            if callback.from_user.id != ADMIN_ID:
                await callback.answer(await get_text(db, "admin_only_text"), show_alert=True)
                return
            await admin_panel(callback, state)
            return
        await callback.answer()

    # -------------------------
    # Catalog & product view
    # -------------------------
    async def show_catalog_menu(callback: CallbackQuery, state: FSMContext, category_id: Optional[int] = None):
        if category_id is None:
            rows = await db.fetchall("SELECT id, title FROM categories WHERE parent_id IS NULL ORDER BY id")
            if not rows:
                products = await db.fetchall("SELECT id, title, price FROM products ORDER BY id DESC LIMIT 50")
                if not products:
                    kb = build_kb([[InlineKeyboardButton(text="В главное меню", callback_data="menu:main")]])
                    await safe_edit_message(callback, "Каталог пуст. (Админ может добавить товары/категории)", reply_markup=kb)
                    return
                kb_rows = [[InlineKeyboardButton(text=f"{p['title']} — {await io_price_to_display(p['price'])}", callback_data=f"product:{p['id']}")] for p in products]
                kb_rows.append([InlineKeyboardButton(text="В главное меню", callback_data="menu:main")])
                await safe_edit_message(callback, "Товары:", reply_markup=build_kb(kb_rows))
                return
            kb_rows = [[InlineKeyboardButton(text=r["title"], callback_data=f"catalog:{r['id']}")] for r in rows]
            kb_rows.append([InlineKeyboardButton(text="В главное меню", callback_data="menu:main")])
            await safe_edit_message(callback, "Категории:", reply_markup=build_kb(kb_rows))
            return
        else:
            rows_p = await db.fetchall("SELECT id, title, price FROM products WHERE category_id = ? ORDER BY id DESC", (category_id,))
            if not rows_p:
                kb = build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:catalog")]])
                await safe_edit_message(callback, "В этой категории пока нет товаров.", reply_markup=kb)
                return
            kb_rows = [[InlineKeyboardButton(text=f"{p['title']} — {await io_price_to_display(p['price'])}", callback_data=f"product:{p['id']}")] for p in rows_p]
            kb_rows.append([InlineKeyboardButton(text="Назад к категориям", callback_data="menu:catalog")])
            await safe_edit_message(callback, "Товары в категории:", reply_markup=build_kb(kb_rows))
            return

    @dp.callback_query(lambda c: c.data and c.data.startswith("catalog:"))
    async def catalog_click(callback: CallbackQuery):
        _, catid = callback.data.split(":", 1)
        await show_catalog_menu(callback, None, int(catid))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("product:"))
    async def product_view(callback: CallbackQuery):
        _, pid = callback.data.split(":", 1)
        row = await db.fetchone("SELECT * FROM products WHERE id = ?", (int(pid),))
        if not row:
            await callback.answer("Товар не найден", show_alert=True)
            return
        title = row["title"]
        desc = row["description"] or ""
        price = await io_price_to_display(row["price"])
        kb_rows = [
            [InlineKeyboardButton(text="Купить сейчас", callback_data=f"buy:{pid}:1")],
            [InlineKeyboardButton(text="Добавить в корзину", callback_data=f"cart_add:{pid}:1")],
            [InlineKeyboardButton(text="В каталог", callback_data="menu:catalog")]
        ]
        imgs = await db.fetchall("SELECT file_id FROM product_images WHERE product_id = ? ORDER BY position", (int(pid),))
        if imgs:
            try:
                await callback.message.answer_photo(photo=imgs[0]["file_id"], caption=f"<b>{title}</b>\n{desc}\n\nЦена: {price}\nТип: {row['product_type']}", reply_markup=build_kb(kb_rows))
                for img in imgs[1:]:
                    await callback.message.answer_photo(photo=img["file_id"])
            except Exception:
                await safe_edit_message(callback, f"<b>{title}</b>\n{desc}\n\nЦена: {price}\nТип: {row['product_type']}", reply_markup=build_kb(kb_rows))
        else:
            await safe_edit_message(callback, f"<b>{title}</b>\n{desc}\n\nЦена: {price}\nТип: {row['product_type']}", reply_markup=build_kb(kb_rows))
        return

    # -------------------------
    # Cart and checkout (unchanged)
    # -------------------------
    @dp.callback_query(lambda c: c.data and c.data.startswith("cart_add:"))
    async def cart_add(callback: CallbackQuery):
        _, pid, qty = callback.data.split(":", 2)
        uid = callback.from_user.id
        pid_i = int(pid)
        qty_i = int(qty)
        row = await db.fetchone("SELECT qty FROM cart WHERE user_id = ? AND product_id = ?", (uid, pid_i))
        if row:
            newq = row["qty"] + qty_i
            await db.execute("UPDATE cart SET qty = ? WHERE user_id = ? AND product_id = ?", (newq, uid, pid_i))
        else:
            await db.execute("INSERT INTO cart (user_id, product_id, qty) VALUES (?, ?, ?)", (uid, pid_i, qty_i))
        await callback.answer(await get_text(db, "added_to_cart_text"), show_alert=True)
        await add_log(db, "INFO", "cart", f"user {uid} add product {pid_i} qty {qty_i}")
        return

    async def show_cart(callback: CallbackQuery, state: FSMContext):
        uid = callback.from_user.id
        rows = await db.fetchall("""
            SELECT p.id, p.title, p.price, c.qty FROM cart c
            JOIN products p ON p.id = c.product_id
            WHERE c.user_id = ?
        """, (uid,))
        if not rows:
            kb_back = build_kb([[InlineKeyboardButton(text="В главное меню", callback_data="menu:main")]])
            await safe_edit_message(callback, await get_text(db, "empty_cart_text"), reply_markup=kb_back)
            return
        lines = []
        total = 0
        kb_rows = []
        for r in rows:
            price = r["price"]
            qty = r["qty"]
            total += price * qty
            lines.append(f"{r['title']} — {await io_price_to_display(price)} × {qty}")
            kb_rows.append([InlineKeyboardButton(text=f"Удалить {r['title']}", callback_data=f"cart_remove:{r['id']}")])
        kb_rows.append([InlineKeyboardButton(text=await get_text(db, "button_checkout"), callback_data="cart:checkout")])
        kb_rows.append([InlineKeyboardButton(text="В каталог", callback_data="menu:catalog")])
        await safe_edit_message(callback, "\n".join(lines) + f"\n\nИтого: {await io_price_to_display(total)}", reply_markup=build_kb(kb_rows))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("cart_remove:"))
    async def cart_remove(callback: CallbackQuery):
        _, pid = callback.data.split(":", 1)
        uid = callback.from_user.id
        await db.execute("DELETE FROM cart WHERE user_id = ? AND product_id = ?", (uid, int(pid)))
        await callback.answer("Удалено из корзины")
        await add_log(db, "INFO", "cart", f"user {uid} removed product {pid}")
        return

    @dp.callback_query(lambda c: c.data and c.data == "cart:checkout")
    async def cart_checkout(callback: CallbackQuery, state: FSMContext):
        uid = callback.from_user.id
        rows = await db.fetchall("""
            SELECT p.id, p.title, p.price, c.qty FROM cart c
            JOIN products p ON p.id = c.product_id
            WHERE c.user_id = ?
        """, (uid,))
        if not rows:
            await callback.answer(await get_text(db, "empty_cart_text"), show_alert=True)
            return
        await state.set_state(CheckoutStates.waiting_contact)
        kb = build_kb([[InlineKeyboardButton(text="Отмена", callback_data="menu:main")]])
        await safe_edit_message(callback, "Пожалуйста, пришлите ваш контакт (номер телефона) или напишите 'нет' если не нужно.", reply_markup=kb)
        return

    @dp.message(CheckoutStates.waiting_contact)
    async def got_contact(message: Message, state: FSMContext):
        await upsert_user(message.from_user)
        contact = message.text.strip()
        await safe_delete_message(message)
        await state.update_data(contact=contact)
        await state.set_state(CheckoutStates.waiting_comment)
        await message.answer("Комментарий к заказу (например, адрес, пожелания). Если нет — напишите 'нет'.")
        return

    @dp.message(CheckoutStates.waiting_comment)
    async def got_comment(message: Message, state: FSMContext):
        data = await state.get_data()
        contact = data.get("contact")
        comment = message.text.strip()
        uid = message.from_user.id
        await safe_delete_message(message)
        rows = await db.fetchall("""
            SELECT p.id, p.title, p.price, c.qty, p.product_type, p.file_id, p.extra_text FROM cart c
            JOIN products p ON p.id = c.product_id
            WHERE c.user_id = ?
        """, (uid,))
        items = []
        total = 0
        for r in rows:
            items.append({"product_id": r["id"], "title": r["title"], "qty": r["qty"], "price": r["price"], "product_type": r["product_type"], "file_id": r["file_id"], "extra_text": r["extra_text"]})
            total += r["price"] * r["qty"]
        order_number = await next_order_number(db)
        created_at = int(time.time())
        cur = await db.execute("INSERT INTO orders (order_number, user_id, items_json, total, contact, comment, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (order_number, uid, json.dumps(items, ensure_ascii=False), total, contact, comment, created_at))
        order_id = cur.lastrowid
        await db.execute("DELETE FROM cart WHERE user_id = ?", (uid,))
        await add_log(db, "INFO", "order", f"user {uid} created order {order_id} number {order_number}")
        # Send user confirmation + try to create invoice
        try:
            await bot.send_message(uid, f"Заказ создан #{order_number}. {await get_text(db, 'payment_pending_text')}")
        except Exception:
            pass
        # Create invoice via Crypto Pay
        invoice = await crypto.create_invoice(amount=total/100.0, description=f"Order #{order_number}", payload={"order_id": order_id, "user_id": uid}, fiat="RUB")
        if invoice and invoice.get("pay_url"):
            await db.execute("UPDATE orders SET payment_invoice_id = ? WHERE id = ?", (invoice.get("invoice_id"), order_id))
            kb_rows = [
                [InlineKeyboardButton(text="Оплатить", url=invoice.get("pay_url"))],
                [InlineKeyboardButton(text=await get_text(db, "button_confirm_payment"), callback_data=f"order:confirmpay:{order_id}")],
                [InlineKeyboardButton(text=await get_text(db, "button_check_payment"), callback_data=f"order:checkpay:{order_id}")]
            ]
            await bot.send_message(uid, "Ссылка на оплату:", reply_markup=build_kb(kb_rows))
        else:
            # no invoice due to error: admin will confirm manually
            await bot.send_message(uid, "Платёжный провайдер недоступен в автоматическом режиме. Админ подтвердит оплату вручную.", reply_markup=build_kb([[InlineKeyboardButton(text=await get_text(db, "button_confirm_payment"), callback_data=f"order:confirmpay:{order_id}")]]))
        # notify admin
        await bot.send_message(ADMIN_ID, f"Новый заказ #{order_number} (id={order_id}) от {uid}.")
        await state.clear()
        return

    # -------------------------
    # Order payment checking & delivery
    # -------------------------
    @dp.callback_query(lambda c: c.data and c.data.startswith("order:checkpay:"))
    async def order_check_payment(callback: CallbackQuery):
        _, _, oid = callback.data.split(":", 2)
        o = await db.fetchone("SELECT payment_invoice_id, order_number, total, user_id FROM orders WHERE id = ?", (int(oid),))
        if not o:
            await callback.answer("Заказ не найден", show_alert=True)
            return
        invoice_id = o["payment_invoice_id"]
        if not invoice_id:
            await callback.answer("У заказа нет invoice_id для проверки (возможно платёж создавался вручную).", show_alert=True)
            return
        # call getInvoices
        res = await crypto.get_invoices_by_ids([invoice_id])
        if not res:
            await callback.answer("Не удалось получить данные по инвойсу.", show_alert=True)
            return
        # result format: list of invoices
        invoices = res if isinstance(res, list) else res.get("invoices") if isinstance(res, dict) else res
        # handle depending on structure
        invoice = None
        if isinstance(invoices, list) and invoices:
            invoice = invoices[0]
        elif isinstance(invoices, dict) and invoices.get("0"):
            invoice = invoices.get("0")
        if not invoice:
            await callback.answer("Инвойс не найден.", show_alert=True)
            return
        status = invoice.get("status")
        if status == "paid":
            # mark order paid and deliver goods
            await db.execute("UPDATE orders SET status = 'paid' WHERE id = ?", (int(oid),))
            await callback.message.answer(await get_text(db, "payment_success_text").format(order_number=o["order_number"]))
            await add_log(db, "INFO", "payment", f"order {oid} auto-mark paid")
            # deliver products
            await deliver_order_products(int(oid))
            await bot.send_message(ADMIN_ID, f"Заказ #{o['order_number']} (id={oid}) оплачен (авто).")
        else:
            await callback.answer(f"Статус инвойса: {status}", show_alert=True)
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("order:confirmpay:"))
    async def order_confirm_payment(callback: CallbackQuery):
        _, _, oid = callback.data.split(":", 2)
        order = await db.fetchone("SELECT order_number, user_id, status FROM orders WHERE id = ?", (int(oid),))
        if not order:
            await callback.answer("Заказ не найден", show_alert=True)
            return
        if callback.from_user.id != ADMIN_ID:
            # notify admin, user clicked "I've paid"
            await callback.answer("Я уведомил администратора. Оплата будет подтверждена вручную.", show_alert=True)
            await bot.send_message(ADMIN_ID, f"Пользователь {callback.from_user.id} нажал 'Я оплатил' для заказа #{order['order_number']} (id={oid}). Проверьте, пожалуйста.")
            return
        # admin confirming
        await db.execute("UPDATE orders SET status = 'paid' WHERE id = ?", (int(oid),))
        await callback.message.answer(f"Заказ #{order['order_number']} помечен как оплаченный (админ).")
        await bot.send_message(ADMIN_ID, f"Вы пометили заказ #{order['order_number']} (id={oid}) как оплаченный.")
        await add_log(db, "INFO", "payment", f"order {oid} admin-mark paid")
        # deliver products automatically on admin confirmation
        await deliver_order_products(int(oid))
        return

    async def deliver_order_products(order_id: int):
        """
        For a paid order, deliver items according to product_type:
        - manual: notify admin to deliver (and inform user)
        - text: send extra_text to user
        - file: send file_id (photo/document) to user
        """
        o = await db.fetchone("SELECT user_id, items_json, order_number FROM orders WHERE id = ?", (order_id,))
        if not o:
            return
        user_id = o["user_id"]
        items = json.loads(o["items_json"])
        for it in items:
            p = await db.fetchone("SELECT product_type, extra_text, file_id, title FROM products WHERE id = ?", (it["product_id"],))
            if not p:
                continue
            ptype = p["product_type"]
            if ptype == "manual":
                # notify admin
                await bot.send_message(ADMIN_ID, f"Заказ #{o['order_number']}: пожалуйста вручную выдайте товар '{p['title']}' пользователю {user_id}. Комментарий: {it.get('qty',1)} шт.")
                await bot.send_message(user_id, f"Товар '{p['title']}' будет выдан вручную администратором. Ожидайте сообщение от админа.")
            elif ptype == "text":
                text = p["extra_text"] or "Контент отсутствует."
                await bot.send_message(user_id, f"Ваш товар — текстовый контент для '{p['title']}':\n\n{text}")
            elif ptype == "file":
                file_id = p["file_id"]
                if file_id:
                    # try send as document; could be photo id too
                    try:
                        await bot.send_document(user_id, InputFile(file_id))
                    except Exception:
                        # fallback to photo
                        try:
                            await bot.send_photo(user_id, file_id)
                        except Exception:
                            await bot.send_message(user_id, f"Файл для товара '{p['title']}' не удалось отправить автоматически. Администратор свяжется с вами.")
                else:
                    await bot.send_message(user_id, f"Файл для товара '{p['title']}' не задан. Админ свяжется с вами.")
            else:
                # default digital: try to send images
                imgs = await db.fetchall("SELECT file_id FROM product_images WHERE product_id = ? ORDER BY position", (it["product_id"],))
                if imgs:
                    try:
                        await bot.send_photo(user_id, imgs[0]["file_id"], caption=f"Ваш товар: {p['title']}")
                        for im in imgs[1:]:
                            await bot.send_photo(user_id, im["file_id"])
                    except Exception:
                        await bot.send_message(user_id, f"Ваш товар '{p['title']}' (изображения) — не удалось отправить автоматически. Админ свяжется с вами.")
                else:
                    await bot.send_message(user_id, f"Спасибо! Ваш заказ #{o['order_number']} оплачен. Администратор свяжется с вами для выдачи товара.")
        await add_log(db, "INFO", "delivery", f"delivered order {order_id}")

    # -------------------------
    # User orders list & detail
    # -------------------------
    async def user_orders_list(callback: CallbackQuery, state: FSMContext):
        uid = callback.from_user.id
        rows = await db.fetchall("SELECT id, order_number, status, total, created_at FROM orders WHERE user_id = ? ORDER BY created_at DESC", (uid,))
        if not rows:
            kb = build_kb([[InlineKeyboardButton(text="В главное меню", callback_data="menu:main")]])
            await safe_edit_message(callback, "У вас ещё нет заказов.", reply_markup=kb)
            return
        kb_rows = [[InlineKeyboardButton(text=f"#{r['order_number']} — {STATUS_RU.get(r['status'], r['status'])}", callback_data=f"user:order_view:{r['id']}")] for r in rows]
        kb_rows.append([InlineKeyboardButton(text="В главное меню", callback_data="menu:main")])
        await safe_edit_message(callback, "Ваши заказы:", reply_markup=build_kb(kb_rows))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("user:order_view:"))
    async def user_order_view(callback: CallbackQuery):
        _, _, oid = callback.data.split(":", 2)
        o = await db.fetchone("SELECT * FROM orders WHERE id = ?", (int(oid),))
        if not o:
            await callback.answer("Заказ не найден", show_alert=True)
            return
        items = json.loads(o["items_json"])
        items_txt = "\n".join([f"{it['title']} × {it['qty']} — {await io_price_to_display(it['price'])}" for it in items])
        status_ru = STATUS_RU.get(o["status"], o["status"])
        created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(o["created_at"])) if o["created_at"] else "-"
        types = set()
        for it in items:
            p = await db.fetchone("SELECT product_type FROM products WHERE id = ?", (it["product_id"],))
            if p:
                types.add(p["product_type"])
        type_txt = ", ".join(types) if types else "—"
        kb_rows = [
            [InlineKeyboardButton(text="Назад к списку заказов", callback_data="menu:orders")],
            [InlineKeyboardButton(text="В главное меню", callback_data="menu:main")]
        ]
        detail = (f"Заказ #{o['order_number']}\n"
                  f"Статус: {status_ru}\n"
                  f"Время: {created}\n"
                  f"Тип: {type_txt}\n"
                  f"Сумма: {await io_price_to_display(o['total'])}\n"
                  f"Комментарий: {o['comment'] or '—'}\n\n"
                  f"Товары:\n{items_txt}")
        await safe_edit_message(callback, detail, reply_markup=build_kb(kb_rows))
        return

    # ---------------------------
    # Tickets: create + my tickets + admin reply
    # ---------------------------
    @dp.callback_query(lambda c: c.data and c.data == "ticket:create")
    async def ticket_create_start(callback: CallbackQuery, state: FSMContext):
        await state.set_state(TicketStates.waiting_text)
        await safe_edit_message(callback, "Опишите проблему / вопрос. После отправки — тикет будет создан и админу придёт уведомление.", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="menu:main")]]))
        return

    @dp.message(TicketStates.waiting_text)
    async def ticket_create_message(message: Message, state: FSMContext):
        await upsert_user(message.from_user)
        txt = message.text.strip()
        created_at = int(time.time())
        cur = await db.execute("INSERT INTO tickets (user_id, subject, status, created_at) VALUES (?, ?, 'open', ?)", (message.from_user.id, txt, created_at))
        tid = cur.lastrowid
        display = ticket_display_number(tid)
        await message.answer(f"Тикет создан: {display}. Администратор свяжется с вами.")
        await bot.send_message(ADMIN_ID, f"Новый тикет {display} от {message.from_user.id}: {txt}")
        await add_log(db, "INFO", "ticket", f"user {message.from_user.id} created ticket {display}")
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data == "ticket:mytickets")
    async def user_tickets_list(callback: CallbackQuery):
        uid = callback.from_user.id
        rows = await db.fetchall("SELECT id, subject, status, created_at FROM tickets WHERE user_id = ? ORDER BY created_at DESC", (uid,))
        if not rows:
            await safe_edit_message(callback, "У вас нет тикетов.", reply_markup=build_kb([[InlineKeyboardButton(text="В главное меню", callback_data="menu:main")]]))
            return
        kb_rows = [[InlineKeyboardButton(text=f"{ticket_display_number(r['id'])} — {r['status']}", callback_data=f"user:ticket_view:{r['id']}")] for r in rows]
        kb_rows.append([InlineKeyboardButton(text="В главное меню", callback_data="menu:main")])
        await safe_edit_message(callback, "Ваши тикеты:", reply_markup=build_kb(kb_rows))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("user:ticket_view:"))
    async def user_ticket_view(callback: CallbackQuery):
        _, _, tid = callback.data.split(":", 2)
        t = await db.fetchone("SELECT * FROM tickets WHERE id = ?", (int(tid),))
        if not t:
            await callback.answer("Тикет не найден", show_alert=True)
            return
        display = ticket_display_number(t["id"])
        created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t["created_at"])) if t["created_at"] else "-"
        kb_rows = [
            [InlineKeyboardButton(text="Назад", callback_data="ticket:mytickets")],
            [InlineKeyboardButton(text="В главное меню", callback_data="menu:main")]
        ]
        await safe_edit_message(callback, f"{display}\nСтатус: {t['status']}\nВремя: {created}\n\n{t['subject']}", reply_markup=build_kb(kb_rows))
        return

    # ---------------------------
    # Admin panel (CRUD and other functions)
    # ---------------------------
    async def admin_panel(callback: CallbackQuery, state: FSMContext):
        rows = [
            [InlineKeyboardButton(text="Управление товарами", callback_data="admin:products")],
            [InlineKeyboardButton(text="Редактировать тексты", callback_data="admin:texts")],
            [InlineKeyboardButton(text="Заказы", callback_data="admin:orders")],
            [InlineKeyboardButton(text="Тикеты", callback_data="admin:tickets")],
            [InlineKeyboardButton(text="Просмотр логов", callback_data="admin:logs")],
            [InlineKeyboardButton(text="Рассылка (broadcast)", callback_data="admin:broadcast")],
            [InlineKeyboardButton(text="Отправить по @username", callback_data="admin:send_user")],
            [InlineKeyboardButton(text="Экспорт заказов CSV", callback_data="admin:export_orders")],
            [InlineKeyboardButton(text="Создать промокод", callback_data="admin:create_promo")],
            [InlineKeyboardButton(text="В главное меню", callback_data="menu:main")],
        ]
        await safe_edit_message(callback, "Панель администратора:", reply_markup=build_kb(rows))
        return

    # --- Admin product flows (create/edit/list/delete) with types ---
    @dp.callback_query(lambda c: c.data and c.data == "admin:products")
    async def admin_products_menu(callback: CallbackQuery):
        rows = [
            [InlineKeyboardButton(text="Добавить товар", callback_data="admin:product_add")],
            [InlineKeyboardButton(text="Список товаров", callback_data="admin:product_list")],
            [InlineKeyboardButton(text="Назад", callback_data="menu:admin")]
        ]
        await safe_edit_message(callback, "Управление товарами:", reply_markup=build_kb(rows))
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:product_add")
    async def admin_product_add_start(callback: CallbackQuery, state: FSMContext):
        await state.clear()
        await state.set_state(ProductCreateStates.title)
        await safe_edit_message(callback, "Добавление товара — введите название (введите текст в чат):", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="admin:products")]]))
        return

    @dp.message(ProductCreateStates.title)
    async def product_create_title(message: Message, state: FSMContext):
        await state.update_data(title=message.text.strip())
        await safe_delete_message(message)
        await state.set_state(ProductCreateStates.description)
        await message.answer("Введите описание товара (или 'нет'):")
        return

    @dp.message(ProductCreateStates.description)
    async def product_create_desc(message: Message, state: FSMContext):
        await state.update_data(description=message.text.strip())
        await safe_delete_message(message)
        await state.set_state(ProductCreateStates.price)
        await message.answer("Введите цену в рублях (пример: 199.50):")
        return

    @dp.message(ProductCreateStates.price)
    async def product_create_price(message: Message, state: FSMContext):
        txt = message.text.strip().replace(",", ".")
        try:
            rub = float(txt)
            price_int = int(round(rub * 100))
        except Exception:
            await message.answer("Некорректная цена. Попробуйте ещё.")
            return
        await state.update_data(price=price_int)
        await safe_delete_message(message)
        # Choose type
        kb = build_kb([
            [InlineKeyboardButton(text="Ручная выдача (manual)", callback_data="producttype:manual")],
            [InlineKeyboardButton(text="Текст (text)", callback_data="producttype:text")],
            [InlineKeyboardButton(text="Файл (file)", callback_data="producttype:file")],
            [InlineKeyboardButton(text="Пропустить (digital)", callback_data="producttype:digital")]
        ])
        await state.set_state(ProductCreateStates.choose_type)
        await message.answer("Выберите тип выдачи товара:", reply_markup=kb)
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("producttype:"))
    async def product_create_choose_type(callback: CallbackQuery, state: FSMContext):
        _, ptype = callback.data.split(":", 1)
        await state.update_data(product_type=ptype)
        if ptype == "text":
            await state.set_state(ProductCreateStates.extra)
            await safe_edit_message(callback, "Введите текст, который будет отправляться покупателю после оплаты (введите текст в чат):", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="admin:products")]]))
            return
        elif ptype == "file":
            await state.set_state(ProductCreateStates.extra)
            await safe_edit_message(callback, "Загрузите файл (документ/фото) для выдачи покупателю после оплаты:", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="admin:products")]]))
            return
        else:
            # manual or digital -> proceed to upload images
            await state.set_state(ProductCreateStates.upload_images)
            await safe_edit_message(callback, "Теперь можете присылать фото товара (несколько). После завершения пришлите /done. Если фото не нужны — пришлите /done.", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="admin:products")]]))
            return

    @dp.message(ProductCreateStates.extra)
    async def product_create_extra(message: Message, state: FSMContext):
        data = await state.get_data()
        ptype = data.get("product_type")
        if ptype == "text":
            await state.update_data(extra_text=message.text)
            await safe_delete_message(message)
            await state.set_state(ProductCreateStates.upload_images)
            await message.answer("Текст сохранён. Можно прислать фото товара или /done.")
            return
        elif ptype == "file":
            # Accept file: document, photo, audio, etc.
            fid = None
            if message.document:
                fid = message.document.file_id
            elif message.photo:
                fid = message.photo[-1].file_id
            elif message.video:
                fid = message.video.file_id
            else:
                await message.answer("Нераспознанный файл. Отправьте документ или фото.")
                return
            await state.update_data(file_id=fid)
            await safe_delete_message(message)
            await state.set_state(ProductCreateStates.upload_images)
            await message.answer("Файл сохранён. Можно прислать фото товара (дополнительно) или /done.")
            return
        else:
            await message.answer("Неверный этап. Отмена.")
            await state.clear()
            return

    @dp.message(ProductCreateStates.upload_images, lambda m: m.photo)
    async def product_create_photo(message: Message, state: FSMContext):
        data = await state.get_data()
        imgs = data.get("images", [])
        imgs.append(message.photo[-1].file_id)
        await state.update_data(images=imgs)
        await safe_delete_message(message)
        await message.answer(f"Добавлено фото #{len(imgs)}. Отправьте /done, когда закончите.")
        return

    @dp.message(ProductCreateStates.upload_images, lambda m: m.text and m.text == "/done")
    async def product_create_done_preview(message: Message, state: FSMContext):
        data = await state.get_data()
        title = data.get("title")
        description = data.get("description")
        price = data.get("price")
        ptype = data.get("product_type") or "digital"
        images = data.get("images", [])
        extra_text = data.get("extra_text")
        file_id = data.get("file_id")
        if not title or price is None:
            await message.answer("Некоторые данные отсутствуют. Отменено.")
            await state.clear()
            return
        caption = f"<b>{title}</b>\n{description}\n\nЦена: {await io_price_to_display(price)}\nТип: {ptype}"
        kb = build_kb([
            [InlineKeyboardButton(text="Сохранить товар", callback_data="admin:product_save_preview")],
            [InlineKeyboardButton(text="Отменить", callback_data="admin:product_cancel_preview")]
        ])
        if images:
            msg = await message.answer_photo(images[0], caption=caption, reply_markup=kb)
            for im in images[1:]:
                await message.answer_photo(im)
        else:
            msg = await message.answer(caption, reply_markup=kb)
        await state.update_data(preview_message_id=msg.message_id)
        await state.set_state(ProductCreateStates.preview)
        await safe_delete_message(message)
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:product_cancel_preview")
    async def admin_product_cancel_preview(callback: CallbackQuery, state: FSMContext):
        await safe_edit_message(callback, "Создание товара отменено.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="admin:products")]]))
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:product_save_preview")
    async def admin_product_save_preview(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        title = data.get("title")
        description = data.get("description")
        price = data.get("price")
        images = data.get("images", [])
        ptype = data.get("product_type") or "digital"
        extra_text = data.get("extra_text")
        file_id = data.get("file_id")
        created_at = int(time.time())
        cur = await db.execute("INSERT INTO products (category_id, title, description, price, product_type, file_id, extra_text, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                              (None, title, description, price, ptype, file_id, extra_text, created_at))
        product_id = cur.lastrowid
        for idx, fid in enumerate(images):
            await db.execute("INSERT INTO product_images (product_id, file_id, position) VALUES (?, ?, ?)", (product_id, fid, idx))
        await safe_edit_message(callback, f"Товар создан с id {product_id}.", reply_markup=build_kb([[InlineKeyboardButton(text="К списку товаров", callback_data="admin:product_list")]]))
        await add_log(db, "INFO", "admin", f"created product {product_id} type={ptype}")
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:product_list")
    async def admin_product_list(callback: CallbackQuery):
        rows = await db.fetchall("SELECT id, title, price, product_type FROM products ORDER BY id DESC")
        if not rows:
            await safe_edit_message(callback, "Товаров нет.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
            return
        kb_rows = [[InlineKeyboardButton(text=f"{r['id']}: {r['title']} — {await io_price_to_display(r['price'])} ({r['product_type']})", callback_data=f"admin:product_edit:{r['id']}")] for r in rows]
        kb_rows.append([InlineKeyboardButton(text="Назад", callback_data="menu:admin")])
        await safe_edit_message(callback, "Список товаров:", reply_markup=build_kb(kb_rows))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:product_edit:"))
    async def admin_product_edit_menu(callback: CallbackQuery):
        _, _, pid = callback.data.split(":", 2)
        p = await db.fetchone("SELECT id, title, description, price, product_type, extra_text, file_id FROM products WHERE id = ?", (int(pid),))
        if not p:
            await callback.answer("Товар не найден", show_alert=True)
            return
        kb = build_kb([
            [InlineKeyboardButton(text="Изменить название", callback_data=f"admin:product_edit_field:{pid}:title")],
            [InlineKeyboardButton(text="Изменить описание", callback_data=f"admin:product_edit_field:{pid}:description")],
            [InlineKeyboardButton(text="Изменить цену", callback_data=f"admin:product_edit_field:{pid}:price")],
            [InlineKeyboardButton(text="Изменить тип выдачи", callback_data=f"admin:product_edit_field:{pid}:product_type")],
            [InlineKeyboardButton(text="Заменить файл (для file)", callback_data=f"admin:product_replace_file:{pid}")],
            [InlineKeyboardButton(text="Заменить фото", callback_data=f"admin:product_replace_images:{pid}")],
            [InlineKeyboardButton(text="Удалить товар", callback_data=f"admin:product_delete:{pid}")],
            [InlineKeyboardButton(text="Назад", callback_data="admin:product_list")]
        ])
        info = (f"Товар {p['id']}\n{p['title']}\nТип: {p['product_type']}\nЦена: {await io_price_to_display(p['price'])}\n{p['description'] or ''}\n")
        if p["product_type"] == "text":
            info += f"\nТекст для выдачи:\n{p['extra_text'] or '—'}"
        if p["product_type"] == "file":
            info += f"\nФайл id: {p['file_id'] or '—'}"
        await safe_edit_message(callback, info, reply_markup=kb)
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:product_edit_field:"))
    async def admin_product_edit_field(callback: CallbackQuery, state: FSMContext):
        _, _, pid, field = callback.data.split(":", 3)
        await state.update_data(edit_product_id=int(pid), edit_field=field)
        # For product_type or file/extra_text, we need specific flows
        if field == "product_type":
            kb = build_kb([
                [InlineKeyboardButton(text="manual", callback_data=f"admin:product_set_type:{pid}:manual")],
                [InlineKeyboardButton(text="text", callback_data=f"admin:product_set_type:{pid}:text")],
                [InlineKeyboardButton(text="file", callback_data=f"admin:product_set_type:{pid}:file")],
                [InlineKeyboardButton(text="digital", callback_data=f"admin:product_set_type:{pid}:digital")],
            ])
            await safe_edit_message(callback, "Выберите новый тип выдачи:", reply_markup=kb)
            return
        await state.set_state(ProductEditStates.new_value)
        await safe_edit_message(callback, f"Введите новое значение для {field} (введите текст):", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="admin:product_list")]]))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:product_set_type:"))
    async def admin_product_set_type(callback: CallbackQuery):
        _, _, pid, newtype = callback.data.split(":", 3)
        await db.execute("UPDATE products SET product_type = ? WHERE id = ?", (newtype, int(pid)))
        await safe_edit_message(callback, f"Тип товара обновлён на {newtype}.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="admin:product_edit:"+pid)]]))
        await add_log(db, "INFO", "admin", f"product {pid} type set {newtype}")
        return

    @dp.message(ProductEditStates.new_value)
    async def admin_product_apply_field_edit(message: Message, state: FSMContext):
        data = await state.get_data()
        pid = data.get("edit_product_id")
        field = data.get("edit_field")
        await safe_delete_message(message)
        if not pid or not field:
            await message.answer("Нет данных. Отменено.")
            await state.clear()
            return
        val = message.text.strip()
        if field == "price":
            try:
                rub = float(val.replace(",", "."))
                price_int = int(round(rub * 100))
            except Exception:
                await message.answer("Некорректная цена.")
                return
            await db.execute("UPDATE products SET price = ? WHERE id = ?", (price_int, pid))
        else:
            # title or description or extra_text
            await db.execute(f"UPDATE products SET {field} = ? WHERE id = ?", (val, pid))
        await message.answer("Поле обновлено.")
        await add_log(db, "INFO", "admin", f"product {pid} field {field} updated")
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:product_replace_file:"))
    async def admin_product_replace_file_start(callback: CallbackQuery, state: FSMContext):
        _, _, pid = callback.data.split(":", 2)
        await state.update_data(replace_file_pid=int(pid))
        await state.set_state(ProductEditStates.upload_images)
        await safe_edit_message(callback, "Загрузите новый файл (документ/фото) для товара:", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="admin:product_list")]]))
        return

    @dp.message(ProductEditStates.upload_images, lambda m: m.document or m.photo or m.video)
    async def admin_product_replace_file_receive(message: Message, state: FSMContext):
        data = await state.get_data()
        pid = data.get("replace_file_pid")
        if not pid:
            await message.answer("Нет product_id. Отменено.")
            await state.clear()
            return
        fid = None
        if message.document:
            fid = message.document.file_id
        elif message.photo:
            fid = message.photo[-1].file_id
        elif message.video:
            fid = message.video.file_id
        else:
            await message.answer("Нераспознанный файл.")
            return
        await db.execute("UPDATE products SET file_id = ? WHERE id = ?", (fid, pid))
        await safe_delete_message(message)
        await message.answer("Файл заменён.")
        await add_log(db, "INFO", "admin", f"product {pid} file replaced")
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:product_replace_images:"))
    async def admin_product_replace_images_start(callback: CallbackQuery, state: FSMContext):
        _, _, pid = callback.data.split(":", 2)
        await state.update_data(replace_product_id=int(pid))
        await state.set_state(ProductEditStates.upload_images)
        await safe_edit_message(callback, "Загрузите новые фото (несколько). После завершения отправьте /done.", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="admin:product_list")]]))
        return

    @dp.message(ProductEditStates.upload_images, lambda m: m.photo)
    async def admin_product_replace_images_receive(message: Message, state: FSMContext):
        data = await state.get_data()
        imgs = data.get("new_images", [])
        imgs.append(message.photo[-1].file_id)
        await state.update_data(new_images=imgs)
        await safe_delete_message(message)
        await message.answer(f"Добавлено фото ({len(imgs)}).")

    @dp.message(ProductEditStates.upload_images, lambda m: m.text and m.text == "/done")
    async def admin_product_replace_images_done(message: Message, state: FSMContext):
        data = await state.get_data()
        pid = data.get("replace_product_id")
        imgs = data.get("new_images", [])
        await safe_delete_message(message)
        if not pid:
            await message.answer("Нет product_id. Отменено.")
            await state.clear()
            return
        await db.execute("DELETE FROM product_images WHERE product_id = ?", (pid,))
        for idx, fid in enumerate(imgs):
            await db.execute("INSERT INTO product_images (product_id, file_id, position) VALUES (?, ?, ?)", (pid, fid, idx))
        await message.answer("Фото заменены.")
        await add_log(db, "INFO", "admin", f"product {pid} images replaced")
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:product_delete:"))
    async def admin_product_delete(callback: CallbackQuery):
        _, _, pid = callback.data.split(":", 2)
        await db.execute("DELETE FROM products WHERE id = ?", (int(pid),))
        await safe_edit_message(callback, "Товар удалён.", reply_markup=build_kb([[InlineKeyboardButton(text="К списку товаров", callback_data="admin:product_list")]]))
        await add_log(db, "INFO", "admin", f"admin deleted product {pid}")
        return

    # ---------------------------
    # Admin texts editor
    # ---------------------------
    @dp.callback_query(lambda c: c.data and c.data == "admin:texts")
    async def admin_texts_menu(callback: CallbackQuery):
        rows = await db.fetchall("SELECT key, value FROM texts")
        kb_rows = [[InlineKeyboardButton(text=r["key"], callback_data=f"admin:texts_edit:{r['key']}")] for r in rows]
        kb_rows.append([InlineKeyboardButton(text="Добавить новый ключ", callback_data="admin:texts_add")])
        kb_rows.append([InlineKeyboardButton(text="Назад", callback_data="menu:admin")])
        await safe_edit_message(callback, "Тексты в боте (кликните для редактирования):", reply_markup=build_kb(kb_rows))
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:texts_add")
    async def admin_texts_add(callback: CallbackQuery, state: FSMContext):
        await state.set_state(TextEditStates.key)
        await safe_edit_message(callback, "Введите ключ нового текста (например: welcome_message):", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="menu:admin")]]))
        return

    @dp.message(TextEditStates.key)
    async def admin_texts_add_key(message: Message, state: FSMContext):
        await state.update_data(new_text_key=message.text.strip())
        await safe_delete_message(message)
        await state.set_state(TextEditStates.value)
        await message.answer("Введите значение для этого ключа (текст). Оно будет сначала показано в превью, и вы сможете сохранить или отменить.")
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:texts_edit:"))
    async def admin_texts_edit(callback: CallbackQuery, state: FSMContext):
        _, _, key = callback.data.split(":", 2)
        row = await db.fetchone("SELECT value FROM texts WHERE key = ?", (key,))
        row_val = row["value"] if row else ""
        await state.update_data(edit_text_key=key)
        await state.set_state(TextEditStates.value)
        await safe_edit_message(callback, f"Текущий текст для {key}:\n\n{row_val}\n\nОтправьте новый текст для превью (я использую превью и только после сохранения текст изменится).", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="admin:texts")]]))
        return

    @dp.message(TextEditStates.value)
    async def admin_texts_value_enter(message: Message, state: FSMContext):
        data = await state.get_data()
        key = data.get("new_text_key") or data.get("edit_text_key")
        if not key:
            await message.answer("Ключ не задан. Отмена.")
            await state.clear()
            return
        new_text = message.text
        await safe_delete_message(message)
        prev = await bot.send_message(ADMIN_ID, f"Превью для ключа <b>{key}</b>:\n\n{new_text}", parse_mode=ParseMode.HTML, reply_markup=build_kb([
            [InlineKeyboardButton(text="Сохранить", callback_data=f"admin:texts_preview_save:{key}")],
            [InlineKeyboardButton(text="Отменить", callback_data="admin:texts_preview_cancel")]
        ]))
        await state.update_data(preview_message_id=prev.message_id, preview_text=new_text, preview_key=key)
        await state.set_state(TextEditStates.preview)
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:texts_preview_cancel")
    async def admin_texts_preview_cancel(callback: CallbackQuery, state: FSMContext):
        await safe_edit_message(callback, "Редактирование текста отменено.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:texts_preview_save:"))
    async def admin_texts_preview_save(callback: CallbackQuery, state: FSMContext):
        _, _, key = callback.data.split(":", 2)
        data = await state.get_data()
        text = data.get("preview_text")
        if not text:
            await callback.answer("Нет текста для сохранения.", show_alert=True)
            await state.clear()
            return
        await set_text(db, key, text)
        await safe_edit_message(callback, f"Текст для ключа {key} сохранён.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
        await add_log(db, "INFO", "admin", f"text saved {key}")
        await state.clear()
        return

    # ---------------------------
    # Admin orders / export / logs
    # ---------------------------
    @dp.callback_query(lambda c: c.data and c.data == "admin:orders")
    async def admin_orders_menu(callback: CallbackQuery):
        rows = await db.fetchall("SELECT id, order_number, user_id, status, total FROM orders ORDER BY created_at DESC LIMIT 50")
        kb_rows = [[InlineKeyboardButton(text=f"#{r['order_number']} ({STATUS_RU.get(r['status'], r['status'])})", callback_data=f"admin:order_view:{r['id']}")] for r in rows]
        kb_rows.append([InlineKeyboardButton(text="Назад", callback_data="menu:admin")])
        await safe_edit_message(callback, "Заказы:", reply_markup=build_kb(kb_rows))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:order_view:"))
    async def admin_order_view(callback: CallbackQuery):
        _, _, oid = callback.data.split(":", 2)
        o = await db.fetchone("SELECT * FROM orders WHERE id = ?", (int(oid),))
        if not o:
            await callback.answer("Заказ не найден", show_alert=True)
            return
        items = json.loads(o["items_json"])
        items_txt = "\n".join([f"{it['title']} × {it['qty']} — {await io_price_to_display(it['price'])}" for it in items])
        status_ru = STATUS_RU.get(o["status"], o["status"])
        created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(o["created_at"])) if o["created_at"] else ""
        kb_rows = [
            [InlineKeyboardButton(text="В обработке", callback_data=f"admin:order_setstatus:{oid}:processing")],
            [InlineKeyboardButton(text="Отправлен", callback_data=f"admin:order_setstatus:{oid}:shipped")],
            [InlineKeyboardButton(text="Оплачен", callback_data=f"admin:order_setstatus:{oid}:paid")],
            [InlineKeyboardButton(text="Назад к списку", callback_data="admin:orders")]
        ]
        await safe_edit_message(callback, f"Заказ #{o['order_number']} (id={o['id']})\nПользователь: {o['user_id']}\nСтатус: {status_ru}\nВремя: {created}\nСумма: {await io_price_to_display(o['total'])}\nКонтакт: {o['contact']}\nКоммент: {o['comment'] or '—'}\n\nТовары:\n{items_txt}", reply_markup=build_kb(kb_rows))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:order_setstatus:"))
    async def admin_order_set_status(callback: CallbackQuery):
        _, _, oid, status = callback.data.split(":", 3)
        await db.execute("UPDATE orders SET status = ? WHERE id = ?", (status, int(oid)))
        await safe_edit_message(callback, "Статус обновлён.", reply_markup=build_kb([[InlineKeyboardButton(text="К заказам", callback_data="admin:orders")]]))
        await add_log(db, "INFO", "admin", f"order {oid} status -> {status}")
        # if admin set to paid, also deliver
        if status == "paid":
            await deliver_order_products(int(oid))
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:export_orders")
    async def admin_export_orders_cb(callback: CallbackQuery):
        await admin_export_orders(callback)
        return

    async def admin_export_orders(callback: CallbackQuery):
        rows = await db.fetchall("SELECT id, order_number, user_id, items_json, total, status, contact, comment, created_at FROM orders ORDER BY created_at DESC")
        if not rows:
            await safe_edit_message(callback, "Нет заказов для экспорта.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
            return
        fname = f"orders_export_{int(time.time())}.csv"
        with open(fname, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "order_number", "user_id", "items", "total_kopecks", "status", "contact", "comment", "created_at"])
            for r in rows:
                writer.writerow([r["id"], r["order_number"], r["user_id"], r["items_json"], r["total"], r["status"], r["contact"], r["comment"], r["created_at"]])
        try:
            await bot.send_document(ADMIN_ID, FSInputFile(fname))
            await safe_edit_message(callback, "Экспорт готов и отправлен администратору.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
        finally:
            try:
                os.remove(fname)
            except Exception:
                pass
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:logs")
    async def admin_logs_view(callback: CallbackQuery):
        rows = await db.fetchall("SELECT ts, level, source, message FROM logs ORDER BY ts DESC LIMIT 50")
        if not rows:
            await safe_edit_message(callback, "Логов нет.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
            return
        txt = "\n".join([f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['ts']))} [{r['level']}] {r['source']}: {r['message']}" for r in rows])
        await safe_edit_message(callback, "Логи последних событий:\n\n" + txt, reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
        return

    # ---------------------------
    # Admin tickets list & reply
    # ---------------------------
    @dp.callback_query(lambda c: c.data and c.data == "admin:tickets")
    async def admin_tickets_menu_cb(callback: CallbackQuery):
        rows = await db.fetchall("SELECT id, user_id, subject, status FROM tickets ORDER BY created_at DESC")
        if not rows:
            await safe_edit_message(callback, "Тикетов нет.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
            return
        kb_rows = [[InlineKeyboardButton(text=f"{ticket_display_number(r['id'])} | {r['user_id']} | {r['status']}", callback_data=f"admin:ticket_view:{r['id']}")] for r in rows]
        kb_rows.append([InlineKeyboardButton(text="Назад", callback_data="menu:admin")])
        await safe_edit_message(callback, "Тикеты:", reply_markup=build_kb(kb_rows))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:ticket_view:"))
    async def admin_ticket_view(callback: CallbackQuery, state: FSMContext):
        _, _, tid = callback.data.split(":", 2)
        t = await db.fetchone("SELECT * FROM tickets WHERE id = ?", (int(tid),))
        if not t:
            await callback.answer("Тикет не найден", show_alert=True)
            return
        display = ticket_display_number(t["id"])
        created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t["created_at"])) if t["created_at"] else ""
        kb_rows = [
            [InlineKeyboardButton(text="Ответить пользователю", callback_data=f"admin:ticket_reply:{tid}")],
            [InlineKeyboardButton(text="Закрыть тикет", callback_data=f"admin:ticket_close:{tid}")],
            [InlineKeyboardButton(text="Назад", callback_data="admin:tickets")]
        ]
        await safe_edit_message(callback, f"{display}\nОт: {t['user_id']}\nСтатус: {t['status']}\nВремя: {created}\n\n{t['subject']}", reply_markup=build_kb(kb_rows))
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:ticket_reply:"))
    async def admin_ticket_reply_start(callback: CallbackQuery, state: FSMContext):
        _, _, tid = callback.data.split(":", 2)
        await state.update_data(reply_ticket_id=int(tid))
        await state.set_state(TicketReplyStates.reply)
        await safe_edit_message(callback, "Введите текст ответа (он будет отправлен пользователю в личные сообщения):", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="admin:tickets")]]))
        return

    @dp.message(TicketReplyStates.reply)
    async def admin_ticket_reply_send(message: Message, state: FSMContext):
        data = await state.get_data()
        tid = data.get("reply_ticket_id")
        if not tid:
            await message.answer("Нет связанного тикета.")
            await state.clear()
            return
        t = await db.fetchone("SELECT user_id FROM tickets WHERE id = ?", (int(tid),))
        if not t:
            await message.answer("Тикет не найден.")
            await state.clear()
            return
        user_id = t["user_id"]
        text = message.text
        await safe_delete_message(message)
        try:
            await bot.send_message(user_id, f"Ответ администратора по тикету {ticket_display_number(tid)}:\n\n{text}")
            await bot.send_message(ADMIN_ID, f"Ответ отправлен пользователю {user_id} по тикету {ticket_display_number(tid)}.")
            await add_log(db, "INFO", "admin", f"ticket {tid} replied")
        except Exception:
            await message.answer("Не удалось отправить сообщение пользователю.")
            logging.exception("Failed to send ticket reply")
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data.startswith("admin:ticket_close:"))
    async def admin_ticket_close(callback: CallbackQuery):
        _, _, tid = callback.data.split(":", 2)
        await db.execute("UPDATE tickets SET status = 'closed' WHERE id = ?", (int(tid),))
        await safe_edit_message(callback, "Тикет закрыт.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад в тикеты", callback_data="admin:tickets")]]))
        await add_log(db, "INFO", "admin", f"ticket {tid} closed")
        return

    # ---------------------------
    # Admin broadcast & send by username
    # ---------------------------
    @dp.callback_query(lambda c: c.data and c.data == "admin:broadcast")
    async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
        await state.set_state(BroadcastStates.message)
        await safe_edit_message(callback, "Введите сообщение для рассылки всем пользователям (будет превью + подтверждение):", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="menu:admin")]]))
        return

    @dp.message(BroadcastStates.message)
    async def admin_broadcast_message(message: Message, state: FSMContext):
        await state.update_data(broadcast_text=message.text)
        await safe_delete_message(message)
        prev = await bot.send_message(ADMIN_ID, f"Превью рассылки:\n\n{message.text}", reply_markup=build_kb([
            [InlineKeyboardButton(text="Подтвердить и отправить", callback_data="admin:broadcast_send")],
            [InlineKeyboardButton(text="Отменить", callback_data="admin:broadcast_cancel")]
        ]))
        await state.update_data(preview_message_id=prev.message_id)
        await state.set_state(BroadcastStates.preview)
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:broadcast_cancel")
    async def admin_broadcast_cancel(callback: CallbackQuery, state: FSMContext):
        await safe_edit_message(callback, "Рассылка отменена.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:broadcast_send")
    async def admin_broadcast_send(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        text = data.get("broadcast_text")
        if not text:
            await callback.answer("Нет текста для рассылки.", show_alert=True)
            await state.clear()
            return
        users = await db.fetchall("SELECT id FROM users")
        count = 0
        for u in users:
            try:
                await bot.send_message(u["id"], text)
                count += 1
            except Exception:
                logging.exception(f"Failed to send broadcast to {u['id']}")
        await safe_edit_message(callback, f"Рассылка отправлена ({count} получателей).", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
        await add_log(db, "INFO", "admin", f"broadcast sent to {count}")
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:send_user")
    async def admin_send_by_username_start(callback: CallbackQuery, state: FSMContext):
        await state.set_state(SendUserStates.username)
        await safe_edit_message(callback, "Введите username пользователя (без @):", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="menu:admin")]]))
        return

    @dp.message(SendUserStates.username)
    async def admin_send_by_username_username(message: Message, state: FSMContext):
        uname = message.text.strip().lstrip("@")
        row = await db.fetchone("SELECT id FROM users WHERE username = ?", (uname,))
        await safe_delete_message(message)
        if not row:
            await bot.send_message(ADMIN_ID, "Пользователь не найден в базе (он должен был ранее отправить /start).")
            await state.clear()
            return
        await state.update_data(target_user_id=row["id"])
        await state.set_state(SendUserStates.message)
        await bot.send_message(ADMIN_ID, "Введите сообщение для отправки пользователю (будет превью):")
        return

    @dp.message(SendUserStates.message)
    async def admin_send_by_username_message(message: Message, state: FSMContext):
        data = await state.get_data()
        target = data.get("target_user_id")
        if not target:
            await message.answer("Нет цели. Отмена.")
            await state.clear()
            return
        await state.update_data(preview_msg=message.text)
        await safe_delete_message(message)
        prev = await bot.send_message(ADMIN_ID, f"Превью сообщения пользователю ({target}):\n\n{message.text}", reply_markup=build_kb([
            [InlineKeyboardButton(text="Отправить", callback_data="admin:send_user_confirm")],
            [InlineKeyboardButton(text="Отменить", callback_data="admin:send_user_cancel")]
        ]))
        await state.set_state(SendUserStates.preview)
        await state.update_data(preview_message_id=prev.message_id)
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:send_user_cancel")
    async def admin_send_by_username_cancel(callback: CallbackQuery, state: FSMContext):
        await safe_edit_message(callback, "Отправка отменена.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
        await state.clear()
        return

    @dp.callback_query(lambda c: c.data and c.data == "admin:send_user_confirm")
    async def admin_send_by_username_confirm(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        target = data.get("target_user_id")
        text = data.get("preview_msg")
        if not target or not text:
            await callback.answer("Данные отсутствуют.", show_alert=True)
            await state.clear()
            return
        try:
            await bot.send_message(target, text)
            await safe_edit_message(callback, "Сообщение отправлено.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
            await add_log(db, "INFO", "admin", f"sent to user {target}")
        except Exception:
            await safe_edit_message(callback, "Не удалось отправить сообщение пользователю.", reply_markup=build_kb([[InlineKeyboardButton(text="Назад", callback_data="menu:admin")]]))
            logging.exception("Send user failed")
        await state.clear()
        return

    # ---------------------------
    # Promo create
    # ---------------------------
    @dp.callback_query(lambda c: c.data and c.data == "admin:create_promo")
    async def admin_create_promo_start_cb(callback: CallbackQuery, state: FSMContext):
        await state.set_state(PromoCreateStates.code)
        await safe_edit_message(callback, "Введите код промокода (например: DISCOUNT10):", reply_markup=build_kb([[InlineKeyboardButton(text="Отмена", callback_data="menu:admin")]]))
        return

    @dp.message(PromoCreateStates.code)
    async def admin_promo_code(message: Message, state: FSMContext):
        await state.update_data(code=message.text.strip())
        await safe_delete_message(message)
        await state.set_state(PromoCreateStates.percent)
        await bot.send_message(ADMIN_ID, "Введите процент скидки (целое число):")
        return

    @dp.message(PromoCreateStates.percent)
    async def admin_promo_percent(message: Message, state: FSMContext):
        data = await state.get_data()
        code = data.get("code")
        try:
            pct = int(message.text.strip())
        except Exception:
            await message.answer("Некорректный процент.")
            return
        await db.execute("INSERT OR REPLACE INTO promo_codes (code, discount_percent, active) VALUES (?, ?, 1)", (code, pct))
        await safe_delete_message(message)
        await bot.send_message(ADMIN_ID, f"Промокод {code} создан с {pct}% скидкой.")
        await add_log(db, "INFO", "admin", f"promo created {code} {pct}%")
        await state.clear()
        return

    # ---------------------------
    # Startup / Shutdown
    # ---------------------------
    async def on_startup():
        await db.execute("INSERT OR IGNORE INTO admins (tg_id) VALUES (?)", (ADMIN_ID,))
        logging.info("Bot started. Admin id = %s", ADMIN_ID)
        await add_log(db, "INFO", "system", "bot started")
        try:
            await bot.send_message(ADMIN_ID, "Магазин запущен.")
        except Exception:
            pass

    async def on_shutdown():
        try:
            await db.close()
        except Exception:
            logging.exception("Error closing DB")
        try:
            await session.close()
        except Exception:
            logging.exception("Error closing aiohttp session")
        try:
            await bot.session.close()
        except Exception:
            logging.exception("Error closing bot session")
        logging.info("Bot stopped.")

    # Fallback debug callback: logs unhandled callback data
    @dp.callback_query()
    async def _debug_log_callback(callback: CallbackQuery):
        logging.debug("Unhandled callback received: %s from %s", callback.data, callback.from_user.id)
        # do not answer here to avoid swallowing responses
        return

    await on_startup()
    try:
        logging.info("Start polling")
        await dp.start_polling(bot)
    finally:
        await on_shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Stopped by KeyboardInterrupt")
