import os
import sys
import asyncio
import json
import pickle
from datetime import datetime, timedelta

import aiohttp
import websockets
import pytz
from dotenv import load_dotenv
from telegram import Update, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    Application,
    CommandHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

# ================== LOAD ENV ==================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # "-1001234567890" hoặc "@channel_name"
ADMIN_IDS = set(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else set()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN chưa được set trong .env")

# ================== CONFIG MEXC ==================
FUTURES_BASE = "https://contract.mexc.co"
WEBSOCKET_URL = "wss://contract.mexc.co/edge"  # endpoint futures ticker

# Ngưỡng để báo động (%)
PUMP_THRESHOLD = 3.0      # Tăng >= 3%
DUMP_THRESHOLD = -3.0     # Giảm <= -3%
MODERATE_MAX = 5.0        # 3–5% = biến động trung bình
EXTREME_THRESHOLD = 10.0  # >=10% = biến động cực mạnh

# Volume tối thiểu để tránh coin ít thanh khoản
MIN_VOL_THRESHOLD = 100000

# ================== GLOBAL STATE ==================
SUBSCRIBERS: set[int] = set()          # chat_id nhận alert private
ALERT_MODE: dict[int, int] = {}        # {chat_id: 1|2|3}
MUTED_COINS: dict[int, set[str]] = {}  # {chat_id: {symbol,...}}

KNOWN_SYMBOLS: set[str] = set()        # danh sách symbol đã biết (USDT futures)
ALL_SYMBOLS: list[str] = []            # cache tất cả symbol

LAST_PRICES: dict[str, dict] = {}      # {symbol: {"price": float, "time": datetime}}
BASE_PRICES: dict[str, float] = {}     # {symbol: base_price}
ALERTED_SYMBOLS: dict[str, datetime] = {}  # {symbol: last_alert_time}
MAX_CHANGES: dict[str, dict] = {}      # {symbol: {"max_pct": float, "time": datetime}}
LAST_SIGNIFICANT_CHANGE: dict[str, datetime] = {}

DATA_FILE = "bot_data.pkl"

# Queue để thông báo WebSocket subscribe thêm coin mới (dynamic)
WS_SUB_QUEUE: asyncio.Queue | None = None


# ================== PERSISTENT DATA ==================
def save_data() -> None:
    data = {
        "subscribers": SUBSCRIBERS,
        "alert_mode": ALERT_MODE,
        "muted_coins": MUTED_COINS,
        "known_symbols": KNOWN_SYMBOLS,
    }
    try:
        with open(DATA_FILE, "wb") as f:
            pickle.dump(data, f)
        print(f"✅ Đã lưu dữ liệu: {len(SUBSCRIBERS)} subscribers, {len(KNOWN_SYMBOLS)} coins")
    except Exception as e:
        print(f"⚠️ Lỗi lưu dữ liệu: {e}")


def load_data() -> None:
    global SUBSCRIBERS, ALERT_MODE, MUTED_COINS, KNOWN_SYMBOLS

    if not os.path.exists(DATA_FILE):
        print("ℹ️ Chưa có file dữ liệu, bắt đầu từ trạng thái trống")
        return

    try:
        with open(DATA_FILE, "rb") as f:
            data = pickle.load(f)

        SUBSCRIBERS = data.get("subscribers", set())
        ALERT_MODE = data.get("alert_mode", {})
        MUTED_COINS = data.get("muted_coins", {})
        KNOWN_SYMBOLS = data.get("known_symbols", set())
        print(
            f"✅ Đã tải dữ liệu: {len(SUBSCRIBERS)} subscribers, "
            f"{len(KNOWN_SYMBOLS)} coins"
        )
    except Exception as e:
        print(f"⚠️ Lỗi tải dữ liệu: {e}")


# ================== HTTP / MEXC UTIL ==================
async def fetch_json(session: aiohttp.ClientSession, url: str, params=None, retry: int = 3):
    """Gọi API, có retry nhẹ cho case lỗi mạng / 429."""
    import random

    for attempt in range(retry):
        try:
            async with session.get(url, params=params, timeout=10) as r:
                if r.status == 429:
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    print(f"⚠️ Rate limit {url}, retry sau {wait:.1f}s…")
                    await asyncio.sleep(wait)
                    continue

                r.raise_for_status()
                data = await r.json()
                # nhiều API của MEXC bọc data trong key "data"
                return data.get("data", data)
        except Exception as e:
            if attempt == retry - 1:
                print(f"❌ Error calling {url}: {e}")
                raise
            await asyncio.sleep(random.uniform(0.5, 1.5))

    raise RuntimeError(f"Failed after {retry} retries for {url}")


async def get_all_contracts(session: aiohttp.ClientSession):
    url = f"{FUTURES_BASE}/api/v1/contract/detail"
    data = await fetch_json(session, url)
    if isinstance(data, dict):
        data = [data]
    return [
        c for c in data
        if c.get("settleCoin") == "USDT" and c.get("state") == 0
    ]


async def get_all_symbols(session: aiohttp.ClientSession) -> list[str]:
    """Lấy danh sách tất cả USDT futures đang active."""
    contracts = await get_all_contracts(session)
    symbols = [c["symbol"] for c in contracts if c.get("symbol")]
    print(f"✅ Lấy được {len(symbols)} symbol USDT futures")
    return symbols


# ================== FORMAT MESSAGE ==================
def fmt_alert(symbol: str, old_price: float, new_price: float, change_pct: float) -> str:
    color = "🟢" if change_pct >= 0 else "🔴"
    abs_change = abs(change_pct)

    if abs_change >= EXTREME_THRESHOLD:
        icon = "🚀🚀🚀" if change_pct >= 0 else "💥💥💥"
        highlight = "⚠️*BIẾN ĐỘNG CỰC MẠNH*⚠️\n"
        size_tag = f"*{change_pct:+.2f}%*"
    else:
        icon = "🚀🚀" if change_pct >= 0 else "💥💥"
        highlight = ""
        size_tag = f"{change_pct:+.2f}%"

    coin = symbol.replace("_USDT", "")
    link = f"https://www.mexc.co/futures/{symbol}"

    return (
        f"{highlight}"
        f"┌{icon} [{coin}]({link}) ⚡ {size_tag} {color}\n"
        f"└ {old_price:.6g} → {new_price:.6g}"
    )


# ================== ADMIN CHECK DECORATOR ==================
def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id

        # Nếu không set ADMIN_IDS thì cho mọi người dùng (backward compatible)
        if not ADMIN_IDS:
            return await func(update, context)

        if user_id not in ADMIN_IDS:
            msg = (
                "⛔ Lệnh này chỉ dành cho admin.\n\n"
                "Bạn vẫn có thể xem alert trong channel."
            )
            await update.effective_message.reply_text(msg)
            return

        return await func(update, context)

    return wrapper


# ================== TELEGRAM COMMANDS ==================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    SUBSCRIBERS.add(chat_id)
    if chat_id not in ALERT_MODE:
        ALERT_MODE[chat_id] = 1

    mode = ALERT_MODE.get(chat_id, 1)
    if mode == 1:
        mode_text = "Tất cả (3–5% + ≥10%)"
    elif mode == 2:
        mode_text = "Chỉ 3–5%"
    else:
        mode_text = "Chỉ ≥10%"

    text = (
        "🤖 *MEXC Futures Alert Bot*\n\n"
        "✅ Nhận giá *REALTIME* từ MEXC Futures\n"
        "✅ Báo NGAY khi biến động ≥3%\n"
        "✅ Dynamic base price – không miss pump/dump\n\n"
        f"📊 Chế độ hiện tại: *{mode_text}*\n\n"
        "Các lệnh:\n"
        "/subscribe – bật báo\n"
        "/unsubscribe – tắt báo\n"
        "/mode1 – báo tất cả (3–5% + ≥10%)\n"
        "/mode2 – chỉ báo 3–5%\n"
        "/mode3 – chỉ báo ≥10%\n"
        "/mute COIN – tắt thông báo coin (VD: /mute BTC)\n"
        "/unmute COIN – bật lại coin\n"
        "/mutelist – xem coin đang mute\n"
        "/timelist – xem lịch coin sắp list\n"
        "/coinlist – coin đã list 1 tuần qua\n"
    )
    await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    save_data()


@admin_only
async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    SUBSCRIBERS.add(chat_id)
    await update.effective_message.reply_text("✅ Đã bật báo!")
    save_data()


@admin_only
async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    SUBSCRIBERS.discard(chat_id)
    await update.effective_message.reply_text("✅ Đã tắt báo!")
    save_data()


@admin_only
async def cmd_mode1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ALERT_MODE[update.effective_chat.id] = 1
    await update.effective_message.reply_text(
        "✅ Mode 1: báo *TẤT CẢ* biến động (3–5% + ≥10%)",
        parse_mode=ParseMode.MARKDOWN,
    )
    save_data()


@admin_only
async def cmd_mode2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ALERT_MODE[update.effective_chat.id] = 2
    await update.effective_message.reply_text(
        "✅ Mode 2: *chỉ báo 3–5%*, bỏ qua ≥10%",
        parse_mode=ParseMode.MARKDOWN,
    )
    save_data()


@admin_only
async def cmd_mode3(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ALERT_MODE[update.effective_chat.id] = 3
    await update.effective_message.reply_text(
        "✅ Mode 3: *chỉ báo ≥10%*, bỏ qua 3–5%",
        parse_mode=ParseMode.MARKDOWN,
    )
    save_data()


@admin_only
async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not context.args:
        await update.effective_message.reply_text(
            "❌ Vui lòng nhập tên coin\n"
            "Ví dụ: /mute BTC hoặc /mute xion"
        )
        return

    coin = context.args[0].upper().strip()
    symbol = f"{coin}_USDT" if not coin.endswith("_USDT") else coin

    MUTED_COINS.setdefault(chat_id, set()).add(symbol)
    await update.effective_message.reply_text(
        f"🔇 Đã tắt thông báo cho `{coin}`",
        parse_mode=ParseMode.MARKDOWN,
    )
    save_data()


@admin_only
async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if not context.args:
        await update.effective_message.reply_text(
            "❌ Vui lòng nhập tên coin\n"
            "Ví dụ: /unmute BTC"
        )
        return

    coin = context.args[0].upper().strip()
    symbol = f"{coin}_USDT" if not coin.endswith("_USDT") else coin

    if chat_id in MUTED_COINS and symbol in MUTED_COINS[chat_id]:
        MUTED_COINS[chat_id].remove(symbol)
        await update.effective_message.reply_text(
            f"🔔 Đã bật lại `{coin}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        save_data()
    else:
        await update.effective_message.reply_text(
            f"ℹ️ `{coin}` hiện chưa bị mute",
            parse_mode=ParseMode.MARKDOWN,
        )


@admin_only
async def cmd_mutelist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in MUTED_COINS or not MUTED_COINS[chat_id]:
        await update.effective_message.reply_text("ℹ️ Chưa có coin nào bị mute")
        return

    coins = sorted(sym.replace("_USDT", "") for sym in MUTED_COINS[chat_id])
    msg = "🔇 *DANH SÁCH COIN ĐÃ MUTE*\n\n"
    msg += "\n".join(f"• `{c}`" for c in coins)
    msg += f"\n\n_Tổng: {len(coins)} coin_"
    await update.effective_message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# =============== TIMELIST / COINLIST (SPOT NEW COINS) ===============
async def cmd_timelist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("⏳ Đang lấy lịch listing…")
    try:
        async with aiohttp.ClientSession() as session:
            timestamp = int(datetime.now().timestamp() * 1000)
            url = f"https://www.mexc.co/api/operation/new_coin_calendar?timestamp={timestamp}"

            async with session.get(url, timeout=15) as r:
                if r.status != 200:
                    raise RuntimeError(f"HTTP {r.status}")
                data = await r.json()

        coins = data.get("data", {}).get("newCoins", [])
        if not coins:
            raise RuntimeError("Không thấy dữ liệu listing")

        vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
        now = datetime.now(vn_tz)
        one_week_later = now + timedelta(days=7)

        msg_lines = ["📅 *LỊCH COIN SẮP LIST (7 NGÀY)*\n"]
        count = 0

        weekdays = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]

        for coin in coins:
            symbol = coin.get("vcoinName")
            full_name = coin.get("vcoinNameFull", symbol)
            ts_ms = coin.get("firstOpenTime")
            if not ts_ms:
                continue

            dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=pytz.UTC)
            dt = dt_utc.astimezone(vn_tz)

            if now <= dt <= one_week_later:
                weekday = weekdays[dt.weekday()]
                date_str = dt.strftime("%d/%m/%Y %H:%M")
                msg_lines.append(f"🆕 `{symbol}` ({full_name})")
                msg_lines.append(f"   ⏰ {weekday}, {date_str}\n")
                count += 1

        if count == 0:
            await update.effective_message.reply_text(
                "📅 Chưa có coin nào sắp list trong 7 ngày tới"
            )
        else:
            await update.effective_message.reply_text(
                "\n".join(msg_lines), parse_mode=ParseMode.MARKDOWN
            )
    except Exception as e:
        print(f"❌ timelist error: {e}")
        await update.effective_message.reply_text(
            "❌ Không thể lấy dữ liệu từ MEXC\n"
            "Xem trực tiếp tại: https://www.mexc.co/vi-VN/announcements/new-listings"
        )


async def cmd_coinlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text("⏳ Đang lấy danh sách coin mới…")
    try:
        async with aiohttp.ClientSession() as session:
            timestamp = int(datetime.now().timestamp() * 1000)
            url = f"https://www.mexc.co/api/operation/new_coin_calendar?timestamp={timestamp}"

            async with session.get(url, timeout=15) as r:
                if r.status != 200:
                    raise RuntimeError(f"HTTP {r.status}")
                data = await r.json()

        coins = data.get("data", {}).get("newCoins", [])
        if not coins:
            raise RuntimeError("Không thấy dữ liệu listing")

        vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
        now = datetime.now(vn_tz)
        one_week_ago = now - timedelta(days=7)

        msg_lines = ["📋 *COIN ĐÃ LIST (7 NGÀY QUA)*\n"]
        count = 0

        weekdays = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]

        for coin in coins:
            symbol = coin.get("vcoinName")
            full_name = coin.get("vcoinNameFull", symbol)
            ts_ms = coin.get("firstOpenTime")
            if not ts_ms:
                continue

            dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=pytz.UTC)
            dt = dt_utc.astimezone(vn_tz)

            if one_week_ago <= dt <= now:
                weekday = weekdays[dt.weekday()]
                date_str = dt.strftime("%d/%m/%Y %H:%M")
                msg_lines.append(f"✅ `{symbol}` ({full_name})")
                msg_lines.append(f"   ⏰ {weekday}, {date_str}\n")
                count += 1

        if count == 0:
            await update.effective_message.reply_text(
                "📋 Không có coin nào list trong 7 ngày qua"
            )
        else:
            await update.effective_message.reply_text(
                "\n".join(msg_lines), parse_mode=ParseMode.MARKDOWN
            )
    except Exception as e:
        print(f"❌ coinlist error: {e}")
        await update.effective_message.reply_text(
            "❌ Không thể lấy dữ liệu từ MEXC\n"
            "Xem trực tiếp tại: https://www.mexc.co/vi-VN/announcements/new-listings"
        )


# ================== WEBSOCKET & PUMP/DUMP LOGIC ==================
async def process_ticker(bot, ticker_data: dict):
    """Xử lý 1 gói ticker và gửi alert nếu vượt ngưỡng (không hạn chế lặp)."""
    symbol = ticker_data.get("symbol")
    if not symbol:
        return

    try:
        current_price = float(ticker_data.get("lastPrice", 0))
        
        # Lấy volume USDT 24h (không phải volume coin)
        # MEXC API: amount24 = volume tính theo USDT
        volume_usdt = float(ticker_data.get("amount24", 0))
        
        # Fallback: nếu không có amount24, tính từ volume24 * price
        if volume_usdt == 0:
            volume_coin = float(ticker_data.get("volume24", 0))
            volume_usdt = volume_coin * current_price

        if current_price <= 0 or volume_usdt < MIN_VOL_THRESHOLD:
            return

        now = datetime.now()

        # lưu giá gần nhất
        LAST_PRICES[symbol] = {"price": current_price, "time": now}

        # tạo base price nếu chưa có
        if symbol not in BASE_PRICES:
            BASE_PRICES[symbol] = current_price
            return

        base_price = BASE_PRICES[symbol]
        price_change = (current_price - base_price) / base_price * 100
        abs_change = abs(price_change)

        # track max change (chỉ để log)
        if symbol not in MAX_CHANGES:
            MAX_CHANGES[symbol] = {"max_pct": price_change, "time": now}
        else:
            if abs(price_change) > abs(MAX_CHANGES[symbol]["max_pct"]):
                MAX_CHANGES[symbol]["max_pct"] = price_change
                MAX_CHANGES[symbol]["time"] = now
                LAST_SIGNIFICANT_CHANGE[symbol] = now

        # điều kiện reset base (để không bị drift quá xa)
        should_reset_base = False
        if abs_change < 1.5:
            should_reset_base = True
        elif symbol in LAST_SIGNIFICANT_CHANGE:
            if (now - LAST_SIGNIFICANT_CHANGE[symbol]).total_seconds() > 50:
                should_reset_base = True

        if should_reset_base:
            BASE_PRICES[symbol] = current_price
            MAX_CHANGES[symbol] = {"max_pct": 0.0, "time": now}

        # kiểm tra có nên alert không
        if not (price_change >= PUMP_THRESHOLD or price_change <= DUMP_THRESHOLD):
            return

        # lưu lại thời điểm alert để job reset base dùng
        ALERTED_SYMBOLS[symbol] = now

        msg = fmt_alert(symbol, base_price, current_price, price_change)
        if price_change >= PUMP_THRESHOLD:
            print(f"🚀 PUMP {symbol}: {price_change:+.2f}% (max {MAX_CHANGES[symbol]['max_pct']:+.2f}%)")
        else:
            print(f"💥 DUMP {symbol}: {price_change:+.2f}% (max {MAX_CHANGES[symbol]['max_pct']:+.2f}%)")

        # gửi vào channel nếu có
        tasks = []

        if CHANNEL_ID:
            tasks.append(
                bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=msg,
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True,
                )
            )

        # gửi cho subscribers riêng (KHÔNG chặn lặp – mỗi lần tick mà đủ % là gửi)
        for chat_id in list(SUBSCRIBERS):
            if chat_id in MUTED_COINS and symbol in MUTED_COINS[chat_id]:
                continue

            mode = ALERT_MODE.get(chat_id, 1)
            # Mode 2: chỉ 3–5%
            if mode == 2 and not (PUMP_THRESHOLD <= abs_change <= MODERATE_MAX):
                continue
            # Mode 3: chỉ ≥10%
            if mode == 3 and abs_change < EXTREME_THRESHOLD:
                continue

            tasks.append(
                bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True,
                )
            )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

            # nếu biến động cực mạnh thì reset base ngay
            if abs_change >= EXTREME_THRESHOLD:
                BASE_PRICES[symbol] = current_price
                MAX_CHANGES[symbol] = {"max_pct": 0.0, "time": now}
                print(f"🔁 Reset base price cho {symbol} sau alert cực mạnh {abs_change:.2f}%")

    except Exception as e:
        print(f"❌ Error processing ticker for {symbol}: {e}")


async def websocket_stream(application: Application):
    """Lắng nghe WebSocket ticker của MEXC và gọi process_ticker()."""
    global ALL_SYMBOLS, KNOWN_SYMBOLS, WS_SUB_QUEUE

    reconnect_delay = 5

    while True:
        try:
            # Khởi tạo queue nếu chưa có
            if WS_SUB_QUEUE is None:
                WS_SUB_QUEUE = asyncio.Queue()

            # Nếu chưa có danh sách symbol thì fetch
            if not ALL_SYMBOLS:
                async with aiohttp.ClientSession() as session:
                    ALL_SYMBOLS = await get_all_symbols(session)
                    if not KNOWN_SYMBOLS:
                        KNOWN_SYMBOLS = set(ALL_SYMBOLS)

            async with websockets.connect(
                WEBSOCKET_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10,
            ) as ws:
                print("✅ Kết nối WebSocket thành công")
                reconnect_delay = 5

                # Subscribe tất cả symbol hiện có
                for sym in ALL_SYMBOLS:
                    sub_msg = {
                        "method": "sub.ticker",
                        "param": {"symbol": sym},
                    }
                    await ws.send(json.dumps(sub_msg))
                    await asyncio.sleep(0.005)

                print(f"✅ Đã subscribe {len(ALL_SYMBOLS)} coin futures")

                # Vòng lặp nhận dữ liệu
                async for message in ws:
                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError:
                        continue

                    # Ping/pong
                    if "ping" in data:
                        await ws.send(json.dumps({"pong": data["ping"]}))
                        continue

                    # Ticker data
                    if data.get("channel") == "push.ticker" and "data" in data:
                        await process_ticker(application.bot, data["data"])

                    # SAU KHI XỬ LÝ TICKER → CHECK XEM CÓ COIN MỚI CẦN SUB KHÔNG
                    if WS_SUB_QUEUE is not None:
                        while not WS_SUB_QUEUE.empty():
                            try:
                                new_sym = await WS_SUB_QUEUE.get()
                            except Exception:
                                break

                            # tránh subscribe trùng
                            if new_sym not in ALL_SYMBOLS:
                                ALL_SYMBOLS.append(new_sym)

                            sub_msg = {
                                "method": "sub.ticker",
                                "param": {"symbol": new_sym},
                            }
                            try:
                                await ws.send(json.dumps(sub_msg))
                                print(f"📡 Đã subscribe thêm coin mới: {new_sym}")
                            except Exception as e:
                                print(f"⚠️ Lỗi khi subscribe thêm {new_sym}: {e}")
                                # nếu lỗi, cho vào queue lại để thử ở vòng sau
                                try:
                                    WS_SUB_QUEUE.put_nowait(new_sym)
                                except Exception:
                                    pass

        except Exception as e:
            print(f"❌ WebSocket error: {e}")
            print(f"🔄 Thử reconnect sau {reconnect_delay}s…")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)


# ================== JOBS ==================
async def job_reset_base_prices(context: ContextTypes.DEFAULT_TYPE):
    """Job backup: mỗi 5 phút reset base price cho coin không alert gần đây."""
    now = datetime.now()
    reset_count = 0
    for symbol, info in list(LAST_PRICES.items()):
        last_price = info["price"]
        last_alert = ALERTED_SYMBOLS.get(symbol)
        if not last_alert or (now - last_alert).total_seconds() > 300:
            BASE_PRICES[symbol] = last_price
            reset_count += 1
    if reset_count:
        print(f"🔄 Backup reset base price cho {reset_count} symbol")


async def job_new_listing(context: ContextTypes.DEFAULT_TYPE):
    """Job: mỗi vài phút check coin mới list (so với KNOWN_SYMBOLS)."""
    if not SUBSCRIBERS and not CHANNEL_ID:
        return

    async with aiohttp.ClientSession() as session:
        try:
            symbols = await get_all_symbols(session)
        except Exception as e:
            print(f"❌ job_new_listing: get_all_symbols error {e}")
            return

    global KNOWN_SYMBOLS
    if not KNOWN_SYMBOLS:
        KNOWN_SYMBOLS = set(symbols)
        print(f"✅ job_new_listing: init {len(KNOWN_SYMBOLS)} coins")
        save_data()
        return

    new_coins = set(symbols) - KNOWN_SYMBOLS
    if not new_coins:
        return

    KNOWN_SYMBOLS.update(new_coins)
    save_data()

    lines = []
    for sym in sorted(new_coins):
        coin = sym.replace("_USDT", "")
        lines.append(f"🆕 *COIN MỚI LIST FUTURES:* `{coin}`")

    text = "\n".join(lines)
    bot = context.bot

    # gửi vào channel
    if CHANNEL_ID:
        try:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as e:
            print(f"❌ job_new_listing: send to channel error {e}")

    # gửi cho subscribers
    for chat_id in list(SUBSCRIBERS):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as e:
            print(f"❌ job_new_listing: send to {chat_id} error {e}")

    # ======= DYNAMIC SUBSCRIBE CHO COIN MỚI (KHÔNG CẦN RESTART) =======
    global WS_SUB_QUEUE, ALL_SYMBOLS

    for sym in new_coins:
        if sym not in ALL_SYMBOLS:
            ALL_SYMBOLS.append(sym)

        if WS_SUB_QUEUE is not None:
            try:
                WS_SUB_QUEUE.put_nowait(sym)
                print(f"🧩 Queue subscribe coin mới: {sym}")
            except Exception as e:
                print(f"⚠️ Không thể queue {sym} để subscribe: {e}")


async def websocket_job(context: ContextTypes.DEFAULT_TYPE):
    """Job wrapper để chạy websocket_stream sau khi Application đã chạy."""
    app = context.application
    await websocket_stream(app)


# ================== APP SETUP ==================
async def post_init(application: Application):
    """Hàm chạy sau khi Application build xong nhưng trước khi polling."""
    # load dữ liệu persist
    load_data()

    # preload ALL_SYMBOLS
    try:
        async with aiohttp.ClientSession() as session:
            global ALL_SYMBOLS, KNOWN_SYMBOLS
            ALL_SYMBOLS = await get_all_symbols(session)
            if not KNOWN_SYMBOLS:
                KNOWN_SYMBOLS = set(ALL_SYMBOLS)
    except Exception as e:
        print(f"⚠️ Không preload được symbols: {e}")

    # chạy WebSocket trong background bằng job_queue (tránh warning PTB)
    application.job_queue.run_once(
        websocket_job,
        when=2,
        name="websocket_stream",
    )

    # job reset base price mỗi 5 phút
    application.job_queue.run_repeating(
        job_reset_base_prices,
        interval=300,
        first=300,
        name="reset_base_prices",
    )

    # job check coin mới list mỗi 10 phút
    application.job_queue.run_repeating(
        job_new_listing,
        interval=600,
        first=120,
        name="new_listing",
    )

    # Đăng ký menu lệnh cho bot (với retry để xử lý lỗi mạng tạm thời)
    commands = [
        BotCommand("start", "Bắt đầu & xem hướng dẫn"),
        BotCommand("subscribe", "Bật thông báo"),
        BotCommand("unsubscribe", "Tắt thông báo"),
        BotCommand("mode1", "Báo tất cả (3–5% + ≥10%)"),
        BotCommand("mode2", "Chỉ báo 3–5%"),
        BotCommand("mode3", "Chỉ báo ≥10%"),
        BotCommand("mute", "Tắt thông báo 1 coin"),
        BotCommand("unmute", "Bật lại thông báo 1 coin"),
        BotCommand("mutelist", "Danh sách coin bị mute"),
        BotCommand("timelist", "Coin sắp list 7 ngày tới"),
        BotCommand("coinlist", "Coin đã list 7 ngày qua"),
    ]
    
    for attempt in range(3):
        try:
            await application.bot.set_my_commands(commands)
            print("✅ Đã đăng ký menu lệnh thành công")
            break
        except Exception as e:
            if attempt < 2:
                print(f"⚠️ Lỗi set_my_commands, thử lại ({attempt + 1}/3): {e}")
                await asyncio.sleep(3)
            else:
                print(f"⚠️ Không thể set_my_commands sau 3 lần thử, bỏ qua: {e}")

    print("✅ post_init hoàn tất – bot sẵn sàng quét MEXC Futures realtime")


def main():
    from telegram.request import HTTPXRequest
    
    # Tăng timeout để xử lý mạng chậm trên Railway
    request = HTTPXRequest(
        connect_timeout=60.0,
        read_timeout=60.0,
        write_timeout=60.0,
        pool_timeout=60.0,
    )
    
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(request)
        .post_init(post_init)
        .build()
    )

    # command handlers
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("subscribe", cmd_subscribe))
    application.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    application.add_handler(CommandHandler("mode1", cmd_mode1))
    application.add_handler(CommandHandler("mode2", cmd_mode2))
    application.add_handler(CommandHandler("mode3", cmd_mode3))
    application.add_handler(CommandHandler("mute", cmd_mute))
    application.add_handler(CommandHandler("unmute", cmd_unmute))
    application.add_handler(CommandHandler("mutelist", cmd_mutelist))
    application.add_handler(CommandHandler("timelist", cmd_timelist))
    application.add_handler(CommandHandler("coinlist", cmd_coinlist))

    print("🔥 Bot MEXC Futures Alert đang chạy…")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except KeyboardInterrupt:
        print("⏸️ Bot dừng")
    except Exception as e:
        print(f"❌ Lỗi bot: {e}")
        raise

if __name__ == "__main__":
    main()