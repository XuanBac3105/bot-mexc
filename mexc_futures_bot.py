import os
import aiohttp
import asyncio
import json
import websockets
from statistics import mean
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
from collections import defaultdict
import pickle
import os.path
import signal
import sys  # Thêm import sys để sử dụng os.execv

# Load biến môi trường từ file .env
load_dotenv()

# ================== CONFIG ==================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # ID của channel (ví dụ: -1001234567890 hoặc @channel_name)
# OWNER_ID: chỉ định 1 user (user id) là chủ bot — bot chỉ phục vụ user này khi set
OWNER_ID = int(os.getenv("OWNER_ID")) if os.getenv("OWNER_ID") else None

ADMIN_IDS = set(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else set()  # Admin user IDs
# Nếu OWNER_ID được set, đảm bảo OWNER_ID luôn là admin
if OWNER_ID:
    ADMIN_IDS.add(OWNER_ID)

FUTURES_BASE = "https://contract.mexc.co"
WEBSOCKET_URL = "wss://contract.mexc.com/edge"  # MEXC Futures WebSocket endpoint

# Ngưỡng để báo động (%)
PUMP_THRESHOLD = 3.0      # Tăng >= 3%
DUMP_THRESHOLD = -3.0     # Giảm >= 3%
MODERATE_MAX = 5.0        # Ngưỡng giữa (3-5%)
EXTREME_THRESHOLD = 10.0  # Ngưỡng cực mạnh >= 10%

# Volume tối thiểu để tránh coin ít thanh khoản
MIN_VOL_THRESHOLD = 100000

SUBSCRIBERS = set()  # User IDs (cho private chat)
ALERT_MODE = {}  # {chat_id: mode} - 1: tất cả, 2: chỉ biến động mạnh ≥3%
MUTED_COINS = {}  # {chat_id: set(symbols)} - danh sách coin bị mute
KNOWN_SYMBOLS = set()  # Danh sách coin đã biết
ALL_SYMBOLS = []  # Cache danh sách coin

# WebSocket price tracking
LAST_PRICES = {}  # {symbol: {"price": float, "time": datetime}}
BASE_PRICES = {}  # {symbol: base_price} - Dynamic reset: chỉ reset sau khi alert
ALERTED_SYMBOLS = {}  # {symbol: timestamp} - tránh spam alert
MAX_CHANGES = {}  # {symbol: {"max_pct": float, "time": datetime}} - Track max % change trong đợt pump/dump
LAST_SIGNIFICANT_CHANGE = {}  # {symbol: timestamp} - Lần cuối có biến động mạnh

# Scheduled restart tracking
SCHEDULED_RESTARTS = set()  # Set of timestamps đã schedule restart

# File để lưu dữ liệu persist
DATA_FILE = "bot_data.pkl"


# ================== PERSISTENT DATA ==================
def save_data():
    """Lưu dữ liệu quan trọng vào file"""
    data = {
        "subscribers": SUBSCRIBERS,
        "alert_mode": ALERT_MODE,
        "muted_coins": MUTED_COINS,
        "known_symbols": KNOWN_SYMBOLS
    }
    try:
        with open(DATA_FILE, "wb") as f:
            pickle.dump(data, f)
        print(f"✅ Đã lưu dữ liệu: {len(SUBSCRIBERS)} subscribers")
    except Exception as e:
        print(f"⚠️ Lỗi lưu dữ liệu: {e}")


def load_data():
    """Tải dữ liệu từ file"""
    global SUBSCRIBERS, ALERT_MODE, MUTED_COINS, KNOWN_SYMBOLS
    
    if not os.path.exists(DATA_FILE):
        print("ℹ️ Chưa có dữ liệu lưu trữ")
        return
    
    try:
        with open(DATA_FILE, "rb") as f:
            data = pickle.load(f)
        
        SUBSCRIBERS = data.get("subscribers", set())
        ALERT_MODE = data.get("alert_mode", {})
        MUTED_COINS = data.get("muted_coins", {})
        KNOWN_SYMBOLS = data.get("known_symbols", set())
        # Nếu OWNER_ID được cấu hình, buộc chỉ một subscriber là OWNER_ID
        if OWNER_ID is not None:
            SUBSCRIBERS = {OWNER_ID}

        print(f"✅ Đã tải dữ liệu: {len(SUBSCRIBERS)} subscribers, {len(KNOWN_SYMBOLS)} coins")
    except Exception as e:
        print(f"⚠️ Lỗi tải dữ liệu: {e}")


# ================== UTIL ==================
async def fetch_json(session, url, params=None, retry=3):
    """Fetch JSON với retry logic cho 429 errors"""
    import random
    
    for attempt in range(retry):
        try:
            async with session.get(url, params=params, timeout=10) as r:
                if r.status == 429:
                    # Rate limit - đợi exponential backoff
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    print(f"⚠️ Rate limit {url}, retry sau {wait:.1f}s...")
                    await asyncio.sleep(wait)
                    continue
                
                r.raise_for_status()
                data = await r.json()
                return data.get("data", data)
        except Exception as e:
            if attempt == retry - 1:  # Lần thử cuối
                print(f"❌ Error calling {url}: {e}")
                raise
            # Thử lại với delay
            await asyncio.sleep(random.uniform(0.5, 1.5))
    
    raise Exception(f"Failed after {retry} retries")


async def get_kline(session, symbol, interval="Min5", limit=10):
    url = f"{FUTURES_BASE}/api/v1/contract/kline/{symbol}"
    data = await fetch_json(session, url, {"interval": interval})
    closes = [float(x) for x in data["close"][-limit:]]
    highs = [float(x) for x in data["high"][-limit:]]
    lows = [float(x) for x in data["low"][-limit:]]
    vols = [float(v) for v in data["vol"][-limit:]]
    return closes, highs, lows, vols


async def get_ticker(session, symbol):
    """Lấy giá ticker hiện tại (realtime)"""
    url = f"{FUTURES_BASE}/api/v1/contract/ticker/{symbol}"
    data = await fetch_json(session, url)
    return float(data["lastPrice"]) if data and "lastPrice" in data else None


async def get_all_contracts(session):
    url = f"{FUTURES_BASE}/api/v1/contract/detail"
    data = await fetch_json(session, url)
    if isinstance(data, dict): data = [data]

    return [
        c for c in data
        if c.get("settleCoin") == "USDT" and c.get("state") == 0
    ]


async def get_all_symbols(session):
    """Lấy danh sách TẤT CẢ symbol USDT Futures đang active"""
    contracts = await get_all_contracts(session)
    return [c["symbol"] for c in contracts if c.get("symbol")]


def fmt_top(title, data):
    txt = [f"🔥 *{title}*"]
    for i, (sym, chg) in enumerate(data, start=1):
        icon = "🚀" if chg > 0 else "💥"
        txt.append(f"{i}. {icon} `{sym}` → {chg:+.2f}%")
    return "\n".join(txt)


def fmt_alert(symbol, old_price, new_price, change_pct):
    """Format báo động pump/dump với 2 mức độ: trung bình (3-5%) và cực mạnh (≥10%)"""
    color = "🟢" if change_pct >= 0 else "🔴"
    
    # Phân loại 2 mức độ biến động
    abs_change = abs(change_pct)
    
    if abs_change >= 10.0:
        # Mức 2: BIẾN ĐỘNG CỰC MẠNH >= 10%
        icon = "🚀🚀🚀" if change_pct >= 0 else "💥💥💥"
        highlight = "⚠️BIẾN ĐỘNG CỰC MẠNH⚠️"
        size_tag = f"*{change_pct:+.2f}%*"  # Bold cho số %
    else:
        # Mức 1: Trung bình 3-9.9%
        icon = "🚀🚀" if change_pct >= 0 else "💥💥"
        highlight = ""
        size_tag = f"{change_pct:+.2f}%"
    
    # Lấy tên coin (bỏ _USDT)
    coin_name = symbol.replace("_USDT", "")
    
    # Link ẩn để không hiển thị URL
    link = f"https://www.mexc.co/futures/{symbol}"
    
    return (
        f"{highlight}"
        f"┌{icon} [{coin_name}]({link}) ⚡ {size_tag} {color}\n"
        f"└ {old_price:.6g} → {new_price:.6g}"
    )


# ================== ADMIN CHECK ==================
def admin_only(func):
    """Decorator để giới hạn command chỉ cho admin"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        # Nếu OWNER_ID đã được set → chỉ OWNER_ID mới được phép
        if OWNER_ID is not None:
            if user_id != OWNER_ID:
                msg = (
                    "⛔ Lệnh này chỉ dành cho chủ bot.\n\n"
                    "Bot này chỉ dùng riêng cho 1 người."
                )
                if getattr(update, "effective_message", None):
                    await update.effective_message.reply_text(msg)
                else:
                    print("⛔ Lệnh admin bị từ chối (no message object)")
                return
            return await func(update, context)

        # Nếu không set ADMIN_IDS → cho phép tất cả (backward compatibility)
        if not ADMIN_IDS:
            return await func(update, context)

        # Nếu không phải admin → từ chối
        if user_id not in ADMIN_IDS:
            msg = (
                "⛔ Lệnh này chỉ dành cho admin.\n\n"
                "Bạn có thể xem alert trong channel!"
            )
            if getattr(update, "effective_message", None):
                await update.effective_message.reply_text(msg)
            else:
                print("⛔ Lệnh admin bị từ chối (no message object)")
            return

        return await func(update, context)
    
    return wrapper


# ================== COMMANDS ==================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    # Chỉ chấp nhận private chat và (nếu OWNER_ID set) chỉ chủ bot
    chat = update.effective_chat
    if getattr(chat, 'type', None) != 'private':
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text("⚠️ Bot chỉ hoạt động trong cuộc trò chuyện riêng tư (private chat).")
        return

    if OWNER_ID is not None and chat_id != OWNER_ID:
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text("⛔ Bot này chỉ phục vụ chủ sở hữu. Bạn không có quyền sử dụng.")
        return

    SUBSCRIBERS.add(chat_id)
    if chat_id not in ALERT_MODE:
        ALERT_MODE[chat_id] = 1  # Mặc định: tất cả

    current_mode = ALERT_MODE.get(chat_id, 1)
    if current_mode == 1:
        mode_text = "Tất cả (3-5% + ≥10%)"
    elif current_mode == 2:
        mode_text = "Chỉ trung bình (3-5%)"
    else:
        mode_text = "Chỉ cực mạnh (≥10%)"

    text = (
        "🤖 Bot Quét MEXC Futures !\n\n"
        "✅ Nhận giá REALTIME từ server\n"
        "✅ Báo NGAY LẬP TỨC khi ≥3%\n"
        "✅ Dynamic base price - không miss pump/dump\n\n"
        f"📊 Chế độ hiện tại: {mode_text}\n\n"
        "Các lệnh:\n"
        "/subscribe – bật báo động\n"
        "/unsubscribe – tắt báo động\n"
        "/mode1 – báo tất cả (3-5% + ≥10%)\n"
        "/mode2 – chỉ báo 3-5%\n"
        "/mode3 – chỉ báo ≥10%\n"
        "/mute COIN – tắt thông báo coin\n"
        "/unmute COIN – bật lại thông báo coin\n"
        "/mutelist – xem danh sách coin đã mute\n"
        "/timelist – lịch coin sắp list\n"
        "/coinlist – coin vừa list gần đây"
    )

    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text(text)
    else:
        print("Start command invoked but no message to reply to")


@admin_only
async def subscribe(update, context):
    # Subscribe chỉ dành cho chủ bot (được kiểm tra bởi decorator)
    SUBSCRIBERS.add(update.effective_chat.id)
    save_data()  # Lưu ngay sau khi subscribe
    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text("✅ Bạn đã bật báo (private).")
    else:
        print("Subscribe executed (no message to reply)")


@admin_only
async def unsubscribe(update, context):
    # Unsubscribe chỉ dành cho chủ bot (được kiểm tra bởi decorator)
    SUBSCRIBERS.discard(update.effective_chat.id)
    save_data()  # Lưu sau khi unsubscribe
    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text("✅ Bạn đã tắt báo (private).")
    else:
        print("Unsubscribe executed (no message to reply)")


@admin_only
async def mode1(update, context):
    chat_id = update.effective_chat.id
    ALERT_MODE[chat_id] = 1
    save_data()  # Lưu sau khi đổi mode
    text = (
        "✅ Đã chuyển sang Mode 1\n\n"
        "📊 Báo TẤT CẢ biến động:\n"
        "  🔸 Trung bình (3-5%)\n"
        "  🔥 Cực mạnh (≥10%)"
    )
    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text(text)
    else:
        print("Mode1 set (no message to reply)")


@admin_only
async def mode2(update, context):
    chat_id = update.effective_chat.id
    ALERT_MODE[chat_id] = 2
    save_data()  # Lưu sau khi đổi mode
    text = (
        "✅ Đã chuyển sang Mode 2\n\n"
        "📊 CHỊ báo biến động trung bình:\n"
        "  🔸 3-5% (bỏ qua cực mạnh ≥10%)"
    )
    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text(text)
    else:
        print("Mode2 set (no message to reply)")


@admin_only
async def mode3(update, context):
    chat_id = update.effective_chat.id
    ALERT_MODE[chat_id] = 3
    save_data()  # Lưu sau khi đổi mode
    text = (
        "✅ Đã chuyển sang Mode 3\n\n"
        "📊 CHỊ báo biến động CỰC MẠNH:\n"
        "  🔥 ≥10% (bỏ qua 3-5%)"
    )
    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text(text)
    else:
        print("Mode3 set (no message to reply)")


@admin_only
async def mute_coin(update, context):
    chat_id = update.effective_chat.id
    
    if not context.args:
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text(
                "❌ Vui lòng nhập tên coin\n\n"
                "Ví dụ: /mute XION hoặc /mute xion"
            )
        else:
            print("❌ Mute command thiếu args (không có message object)")
        return
    
    coin = context.args[0].upper().strip()  # Tự động chuyển thành chữ hoa
    symbol = f"{coin}_USDT" if not coin.endswith("_USDT") else coin
    
    if chat_id not in MUTED_COINS:
        MUTED_COINS[chat_id] = set()
    
    MUTED_COINS[chat_id].add(symbol)
    save_data()  # Lưu sau khi mute
    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text(f"🔇 Đã tắt thông báo cho `{coin}`", parse_mode="Markdown")
    else:
        try:
            await context.bot.send_message(chat_id, f"🔇 Đã tắt thông báo cho `{coin}`", parse_mode="Markdown")
        except Exception:
            print("🔇 Đã mute coin nhưng không thể gửi tin xác nhận")


@admin_only
async def unmute_coin(update, context):
    chat_id = update.effective_chat.id
    
    if not context.args:
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text(
                "❌ Vui lòng nhập tên coin\n\n"
                "Ví dụ: /unmute XION hoặc /unmute xion"
            )
        else:
            print("❌ Unmute command thiếu args (không có message object)")
        return
    
    coin = context.args[0].upper().strip()  # Tự động chuyển thành chữ hoa
    symbol = f"{coin}_USDT" if not coin.endswith("_USDT") else coin
    
    if chat_id in MUTED_COINS and symbol in MUTED_COINS[chat_id]:
        MUTED_COINS[chat_id].remove(symbol)
        save_data()  # Lưu sau khi unmute
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text(f"🔔 Đã bật lại thông báo cho `{coin}`", parse_mode="Markdown")
        else:
            try:
                await context.bot.send_message(chat_id, f"🔔 Đã bật lại thông báo cho `{coin}`", parse_mode="Markdown")
            except Exception:
                print("🔔 Đã unmute coin nhưng không thể gửi tin xác nhận")
    else:
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text(f"ℹ️ `{coin}` chưa bị mute", parse_mode="Markdown")
        else:
            try:
                await context.bot.send_message(chat_id, f"ℹ️ `{coin}` chưa bị mute", parse_mode="Markdown")
            except Exception:
                print("ℹ️ Trạng thái unmute không thể gửi (không có message)")


@admin_only
async def mutelist(update, context):
    chat_id = update.effective_chat.id
    
    if chat_id not in MUTED_COINS or not MUTED_COINS[chat_id]:
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text("ℹ️ Chưa có coin nào bị mute")
        else:
            try:
                await context.bot.send_message(chat_id, "ℹ️ Chưa có coin nào bị mute")
            except Exception:
                print("ℹ️ Không có coin mute (không thể gửi message)")
        return
    
    coins = [sym.replace("_USDT", "") for sym in MUTED_COINS[chat_id]]
    msg = "🔇 *DANH SÁCH COIN ĐÃ MUTE*\n\n"
    msg += "\n".join([f"• `{coin}`" for coin in sorted(coins)])
    msg += f"\n\n_Tổng: {len(coins)} coin_"
    
    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text(msg, parse_mode="Markdown")
    else:
        try:
            await context.bot.send_message(chat_id, msg, parse_mode="Markdown")
        except Exception:
            print("ℹ️ Không thể gửi danh sách mute (no message object)")


async def websocket_stream(context):
    """WebSocket stream để nhận giá realtime từ MEXC Futures"""
    reconnect_delay = 5
    
    while True:
        try:
            # Tăng timeout và thêm ping interval
            async with websockets.connect(
                WEBSOCKET_URL,
                ping_interval=20,  # Ping server mỗi 20s để giữ kết nối
                ping_timeout=10,   # Timeout cho pong response
                close_timeout=10
            ) as ws:
                print(f"✅ Kết nối WebSocket thành công")
                
                # Subscribe tất cả ticker streams - MEXC Futures format
                for symbol in ALL_SYMBOLS:
                    # MEXC Futures WebSocket format: sub.ticker
                    sub_msg = {
                        "method": "sub.ticker",
                        "param": {
                            "symbol": symbol
                        }
                    }
                    await ws.send(json.dumps(sub_msg))
                    await asyncio.sleep(0.005)  # 5ms delay giữa subscriptions
                
                print(f"✅ Đã subscribe {len(ALL_SYMBOLS)} coin qua WebSocket")
                
                # Reset reconnect delay sau khi connect thành công
                reconnect_delay = 5
                
                # Lắng nghe messages
                async for message in ws:
                    try:
                        data = json.loads(message)
                        
                        # Xử lý ping/pong
                        if "ping" in data:
                            await ws.send(json.dumps({"pong": data["ping"]}))
                            continue
                        
                        # Xử lý ticker data
                        if "channel" in data and data.get("channel") == "push.ticker":
                            if "data" in data:
                                await process_ticker(data["data"], context)
                            
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"❌ Error processing message: {e}")
                        continue
                        
        except Exception as e:
            print(f"❌ WebSocket error: {e}")
            print(f"🔄 Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            
            # Exponential backoff: 5s -> 10s -> 20s -> max 60s
            reconnect_delay = min(reconnect_delay * 2, 60)


async def process_ticker(ticker_data, context):
    """Xử lý ticker data từ WebSocket và phát hiện pump/dump - DUAL BASE PRICE"""
    symbol = ticker_data.get("symbol")
    if not symbol:
        return
    
    try:
        current_price = float(ticker_data.get("lastPrice", 0))
        volume = float(ticker_data.get("volume24", 0))
        
        if current_price == 0 or volume < MIN_VOL_THRESHOLD:
            return
        
        now = datetime.now()
        
        # Lưu giá hiện tại
        LAST_PRICES[symbol] = {
            "price": current_price,
            "time": now
        }
        
        # Thiết lập base price nếu chưa có
        if symbol not in BASE_PRICES:
            BASE_PRICES[symbol] = current_price
            return
        
        # Tính % thay đổi từ BASE_PRICE (dynamic - chỉ reset sau alert)
        base_price = BASE_PRICES[symbol]
        price_change = (current_price - base_price) / base_price * 100
        abs_change = abs(price_change)
        
        # Track max change trong đợt pump/dump
        if symbol not in MAX_CHANGES:
            MAX_CHANGES[symbol] = {"max_pct": 0, "time": now}
        
        # Cập nhật max change nếu vượt qua
        if abs_change > abs(MAX_CHANGES[symbol]["max_pct"]):
            MAX_CHANGES[symbol] = {"max_pct": price_change, "time": now}
            LAST_SIGNIFICANT_CHANGE[symbol] = now
        
        # Kiểm tra xem có nên reset base price không
        # Reset nếu: giá quay về gần base (< 1.5%) HOẶC đã qua 3 phút không có biến động mạnh
        should_reset_base = False
        if abs_change < 1.5:  # Giá đã quay về gần base price
            should_reset_base = True
        elif symbol in LAST_SIGNIFICANT_CHANGE:
            time_since_last = (now - LAST_SIGNIFICANT_CHANGE[symbol]).total_seconds()
            if time_since_last > 50:  # 50 giây không có biến động mạnh
                should_reset_base = True
        
        if should_reset_base and symbol in MAX_CHANGES:
            BASE_PRICES[symbol] = current_price
            MAX_CHANGES[symbol] = {"max_pct": 0, "time": now}
        
        # Kiểm tra ngưỡng và alert ngay khi vượt
        should_alert = False
        if (price_change >= PUMP_THRESHOLD or price_change <= DUMP_THRESHOLD):
            last_alert = ALERTED_SYMBOLS.get(symbol)
            last_max = MAX_CHANGES[symbol].get("last_alerted_pct")
            # Báo ngay lần đầu vượt ngưỡng
            if last_alert is None:
                should_alert = True
            else:
                # Nếu đã báo rồi, chỉ báo lại khi tăng thêm >=1.5%
                if last_max is None:
                    last_max = 0.0
                if abs_change >= abs(last_max) + 1.5:
                    should_alert = True
            if should_alert:
                ALERTED_SYMBOLS[symbol] = now
                MAX_CHANGES[symbol]["last_alerted_pct"] = price_change

        if should_alert and SUBSCRIBERS:
            # Dùng BASE_PRICE và hiển thị % thay đổi TỔNG
            msg = fmt_alert(symbol, base_price, current_price, price_change)
            if price_change >= PUMP_THRESHOLD:
                print(f"🚀 PUMP: {symbol} +{price_change:.2f}% (max: +{MAX_CHANGES[symbol]['max_pct']:.2f}%)")
            else:
                print(f"💥 DUMP: {symbol} {price_change:.2f}% (max: {MAX_CHANGES[symbol]['max_pct']:.2f}%)")

            # Gửi alert
            tasks = []
            
            # Channel sending disabled for single-owner private mode (no group/channel notifications)
            
            # Gửi cho subscribers cá nhân (nếu có)
            for chat in SUBSCRIBERS:
                # Kiểm tra coin có bị mute không
                if chat in MUTED_COINS and symbol in MUTED_COINS[chat]:
                    continue
                
                mode = ALERT_MODE.get(chat, 1)  # Mặc định mode 1
                abs_change = abs(price_change)

                # Mode 1: Báo tất cả (3-5% + ≥10%)
                # Mode 2: Chỉ báo 3-5%
                # Mode 3: Chỉ báo ≥10%
                
                if mode == 2:
                    # Mode 2: Chỉ 3-5%, bỏ qua ≥10%
                    if abs_change > MODERATE_MAX:
                        continue
                elif mode == 3:
                    # Mode 3: Chỉ ≥10%
                    if abs_change < EXTREME_THRESHOLD:
                        continue
                # Mode 1: Không filter, báo tất cả

                tasks.append(
                    context.bot.send_message(
                        chat,
                        msg,
                        parse_mode="Markdown",
                        disable_web_page_preview=True
                    )
                )

            if tasks:
                try:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    # Nếu đây là alert cực mạnh (>= EXTREME_THRESHOLD) -> reset base ngay lập tức
                    try:
                        if abs_change >= EXTREME_THRESHOLD:
                            BASE_PRICES[symbol] = current_price
                            MAX_CHANGES[symbol] = {"max_pct": 0, "time": now}
                            print(f"🔁 Reset base price for {symbol} after extreme alert ({abs_change:.2f}%)")
                    except Exception:
                        pass
                except Exception as e:
                    print(f"❌ Lỗi gửi tin nhắn: {e}")
            
    except Exception as e:
        print(f"❌ Error processing ticker for {symbol}: {e}")


async def reset_base_prices(context):
    """Job backup reset base prices mỗi 5 phút"""
    global BASE_PRICES
    
    # Cập nhật base prices từ last prices (chỉ cho coin không có alert gần đây)
    for symbol, data in LAST_PRICES.items():
        # Chỉ reset nếu không có alert trong 5 phút qua
        if symbol not in ALERTED_SYMBOLS or \
           (datetime.now() - ALERTED_SYMBOLS[symbol]).seconds > 300:
            BASE_PRICES[symbol] = data["price"]
    
    print(f"🔄 Backup reset {len(BASE_PRICES)} base prices")


async def calc_movers(session, interval, symbols):
    """Tính % thay đổi giá cho danh sách symbols - BATCH để tránh rate limit"""
    import asyncio
    
    async def get_single_mover(sym):
        """Lấy dữ liệu cho 1 coin - so sánh giá HIỆN TẠI vs candle cuối (bao gồm HIGH/LOW để bắt râu)"""
        try:
            # Lấy candle đã đóng (close, high, low, volume)
            closes, highs, lows, vols = await get_kline(session, sym, interval, 2)
            if len(closes) < 1 or closes[-1] == 0:
                return None
            
            # Lấy giá REALTIME hiện tại
            current_price = await get_ticker(session, sym)
            if not current_price:
                return None
            
            # Giá base để tính % thay đổi
            base_price = closes[-1]  # Candle đóng cửa
            high_price = highs[-1]   # Giá cao nhất của candle
            low_price = lows[-1]     # Giá thấp nhất của candle
            vol = vols[-1]
            
            # Tính % thay đổi so với close
            chg_from_close = (current_price - base_price) / base_price * 100
            
            # Kiểm tra xem giá hiện tại có vượt HIGH hoặc LOW không (phát hiện breakout)
            chg_from_high = (current_price - high_price) / high_price * 100
            chg_from_low = (current_price - low_price) / low_price * 100
            
            # Chọn % thay đổi lớn nhất để phát hiện các spike/wick
            if abs(chg_from_close) >= abs(chg_from_high) and abs(chg_from_close) >= abs(chg_from_low):
                chg = chg_from_close
                old_price = base_price
            elif abs(chg_from_high) > abs(chg_from_low):
                chg = chg_from_high
                old_price = high_price
            else:
                chg = chg_from_low
                old_price = low_price
            
            return (sym, chg, old_price, current_price, vol)
        except Exception as e:
            return None
    
    # CHIA NHỎ THÀNH BATCH để tránh 429 Too Many Requests
    BATCH_SIZE = 50  # Quét 50 coins/lần
    BATCH_DELAY_MIN = 0.6  # Random delay 0.6-1.0s giữa các batch
    BATCH_DELAY_MAX = 1.0
    
    all_movers = []
    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i+BATCH_SIZE]
        tasks = [get_single_mover(sym) for sym in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Lọc bỏ None và exceptions
        movers = [r for r in results if r is not None and not isinstance(r, Exception)]
        all_movers.extend(movers)
        
        # Random delay giữa các batch (trừ batch cuối)
        if i + BATCH_SIZE < len(symbols):
            import random
            delay = random.uniform(BATCH_DELAY_MIN, BATCH_DELAY_MAX)
            await asyncio.sleep(delay)
    
    return all_movers


async def timelist(update, context):
    """Lệnh xem lịch coin sẽ list trong 1 tuần - API Calendar"""
    # Chỉ cho phép private chat (không trả lời group/channel)
    chat = update.effective_chat
    if getattr(chat, 'type', None) != 'private':
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text("⚠️ Lệnh chỉ hoạt động trong cuộc trò chuyện riêng tư (private chat).")
        return

    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text("⏳ Đang lấy lịch listing...")
    else:
        try:
            await context.bot.send_message(update.effective_chat.id, "⏳ Đang lấy lịch listing...")
        except Exception:
            print("⏳ Timelist requested (no message object)")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Gọi API calendar
            timestamp = int(datetime.now().timestamp() * 1000)
            url = f"https://www.mexc.co/api/operation/new_coin_calendar?timestamp={timestamp}"
            
            async with session.get(url, timeout=15) as r:
                if r.status != 200:
                    raise Exception(f"HTTP {r.status}")
                
                data = await r.json()
                coins = data.get('data', {}).get('newCoins', [])
                
                if not coins:
                    raise Exception("Không tìm thấy dữ liệu listing")
                
                vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
                now = datetime.now(vn_tz)
                one_week_later = now + timedelta(days=7)
                
                msg = "📅 *LỊCH COIN SẮP LIST (1 TUẦN)*\n\n"
                count = 0
                
                for coin in coins:
                    symbol = coin.get('vcoinName')
                    full_name = coin.get('vcoinNameFull', symbol)
                    timestamp_ms = coin.get('firstOpenTime')
                    
                    if not timestamp_ms:
                        continue
                    
                    # Convert timestamp to datetime - API trả UTC, convert sang VN
                    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                    dt = dt_utc.astimezone(vn_tz)
                    
                    # Chỉ hiển thị coin list trong 1 tuần tới
                    if now <= dt <= one_week_later:
                        weekdays = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
                        weekday = weekdays[dt.weekday()]
                        date_str = dt.strftime("%d/%m/%Y %H:%M")
                        
                        msg += f"🆕 `{symbol}` ({full_name})\n"
                        msg += f"   ⏰ {weekday}, {date_str}\n\n"
                        count += 1
                
                if count == 0:
                    if getattr(update, "effective_message", None):
                        await update.effective_message.reply_text("📅 Chưa có coin nào sắp list trong tuần tới")
                    else:
                        try:
                            await context.bot.send_message(update.effective_chat.id, "📅 Chưa có coin nào sắp list trong tuần tới")
                        except Exception:
                            print("📅 Không thể gửi thông báo timelist")
                else:
                    if getattr(update, "effective_message", None):
                        await update.effective_message.reply_text(msg, parse_mode="Markdown")
                    else:
                        try:
                            await context.bot.send_message(update.effective_chat.id, msg, parse_mode="Markdown")
                        except Exception:
                            print("📅 Không thể gửi danh sách timelist")
    
    except Exception as e:
        print(f"❌ Lỗi scrape Futures listing: {e}")
        msg = (
            "❌ Không thể lấy dữ liệu từ MEXC\n\n"
            "Vui lòng xem trực tiếp tại:\n"
            "🔗 https://www.mexc.co/vi-VN/announcements/new-listings"
        )
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text(msg, parse_mode="Markdown")
        else:
            try:
                await context.bot.send_message(update.effective_chat.id, msg, parse_mode="Markdown")
            except Exception:
                print("❌ Timelist: không thể gửi lỗi đến user")


async def coinlist(update, context):
    """Lệnh xem các coin đã list trong 1 tuần - API Calendar"""
    # Chỉ cho phép private chat (không trả lời group/channel)
    chat = update.effective_chat
    if getattr(chat, 'type', None) != 'private':
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text("⚠️ Lệnh chỉ hoạt động trong cuộc trò chuyện riêng tư (private chat).")
        return

    if getattr(update, "effective_message", None):
        await update.effective_message.reply_text("⏳ Đang lấy danh sách coin mới...")
    else:
        try:
            await context.bot.send_message(update.effective_chat.id, "⏳ Đang lấy danh sách coin mới...")
        except Exception:
            print("⏳ Coinlist requested (no message object)")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Gọi API calendar
            timestamp = int(datetime.now().timestamp() * 1000)
            url = f"https://www.mexc.co/api/operation/new_coin_calendar?timestamp={timestamp}"
            
            async with session.get(url, timeout=15) as r:
                if r.status != 200:
                    raise Exception(f"HTTP {r.status}")
                
                data = await r.json()
                coins = data.get('data', {}).get('newCoins', [])
                
                if not coins:
                    raise Exception("Không tìm thấy dữ liệu listing")
                
                vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
                now = datetime.now(vn_tz)
                one_week_ago = now - timedelta(days=7)
                
                msg = "📋 *COIN ĐÃ LIST (1 TUẦN QUA)*\n\n"
                count = 0
                
                for coin in coins:
                    symbol = coin.get('vcoinName')
                    full_name = coin.get('vcoinNameFull', symbol)
                    timestamp_ms = coin.get('firstOpenTime')
                    
                    if not timestamp_ms:
                        continue
                    
                    # Convert timestamp to datetime - API trả UTC, convert sang VN
                    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                    dt = dt_utc.astimezone(vn_tz)
                    
                    # Chỉ hiển thị coin list trong 1 tuần qua
                    if one_week_ago <= dt <= now:
                        weekdays = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
                        weekday = weekdays[dt.weekday()]
                        date_str = dt.strftime("%d/%m/%Y %H:%M")
                        
                        msg += f"✅ `{symbol}` ({full_name})\n"
                        msg += f"   ⏰ {weekday}, {date_str}\n\n"
                        count += 1
                
                if count == 0:
                    if getattr(update, "effective_message", None):
                        await update.effective_message.reply_text("📋 Không có coin nào list trong tuần qua")
                    else:
                        try:
                            await context.bot.send_message(update.effective_chat.id, "📋 Không có coin nào list trong tuần qua")
                        except Exception:
                            print("📋 Không thể gửi coinlist (no message)")
                else:
                    if getattr(update, "effective_message", None):
                        await update.effective_message.reply_text(msg, parse_mode="Markdown")
                    else:
                        try:
                            await context.bot.send_message(update.effective_chat.id, msg, parse_mode="Markdown")
                        except Exception:
                            print("📋 Không thể gửi danh sách coinlist")
    
    except Exception as e:
        print(f"❌ Lỗi scrape Futures listing: {e}")
        msg = (
            "❌ Không thể lấy dữ liệu từ MEXC\n\n"
            "Vui lòng xem trực tiếp tại:\n"
            "🔗 https://www.mexc.co/vi-VN/announcements/new-listings"
        )
        if getattr(update, "effective_message", None):
            await update.effective_message.reply_text(msg, parse_mode="Markdown")
        else:
            try:
                await context.bot.send_message(update.effective_chat.id, msg, parse_mode="Markdown")
            except Exception:
                print("❌ Coinlist: không thể gửi lỗi đến user")


# ================== JOBS ==================
async def job_scan_pumps_dumps(context):
    """Job chính: Quét TẤT CẢ coin và báo khi có pump/dump"""
    if not SUBSCRIBERS:
        return
    
    print("🔍 Đang quét tất cả coin...")
    
    async with aiohttp.ClientSession() as session:
        # Lấy danh sách tất cả symbols
        global ALL_SYMBOLS
        if not ALL_SYMBOLS:
            ALL_SYMBOLS = await get_all_symbols(session)
            print(f"✅ Tìm thấy {len(ALL_SYMBOLS)} coin")
        
        # Tính movers cho tất cả coin
        movers = await calc_movers(session, "Min1", ALL_SYMBOLS)
    
    if not movers:
        return
    
    # Lọc coin có volume đủ và biến động mạnh
    alerts = []
    for sym, chg, old_price, new_price, vol in movers:
        if vol < MIN_VOL_THRESHOLD:
            continue
        
        # PUMP: tăng >= ngưỡng
        if chg >= PUMP_THRESHOLD:
            msg = fmt_alert(sym, old_price, new_price, chg)
            alerts.append(msg)
            print(f"🚀 PUMP: {sym} {chg:+.2f}%")
        
        # DUMP: giảm >= ngưỡng
        elif chg <= DUMP_THRESHOLD:
            msg = fmt_alert(sym, old_price, new_price, chg)
            alerts.append(msg)
            print(f"� DUMP: {sym} {chg:+.2f}%")
    
    # Gửi alert đến tất cả subscribers
    if alerts:
        # Gom nhóm để tránh spam
        text = "\n\n".join(alerts[:10])  # Chỉ gửi tối đa 10 alert mỗi lần
        if len(alerts) > 10:
            text += f"\n\n... và {len(alerts) - 10} coin khác"
        
        for chat in SUBSCRIBERS:
            try:
                await context.bot.send_message(
                    chat, 
                    text, 
                    parse_mode="Markdown",
                    disable_web_page_preview=True  # Tắt preview link
                )
            except Exception as e:
                print(f"❌ Lỗi gửi tin nhắn: {e}")


async def job_new_listing(context):
    """Job phát hiện coin mới list bằng cách so sánh danh sách"""
    if not SUBSCRIBERS:
        return

    async with aiohttp.ClientSession() as session:
        try:
            symbols = await get_all_symbols(session)
        except:
            return
    
    global KNOWN_SYMBOLS
    
    # Lần đầu chạy: lưu danh sách hiện tại
    if not KNOWN_SYMBOLS:
        KNOWN_SYMBOLS = set(symbols)
        print(f"✅ Đã lưu {len(KNOWN_SYMBOLS)} coin ban đầu")
        return
    
    # So sánh với danh sách cũ
    new_coins = set(symbols) - KNOWN_SYMBOLS
    
    if new_coins:
        alerts = []
        for sym in new_coins:
            KNOWN_SYMBOLS.add(sym)
            coin = sym.replace("_USDT", "")
            alerts.append(f"🆕 *COIN MỚI LIST:* `{coin}`")
            print(f"🆕 NEW LISTING: {sym}")
        
        save_data()  # Lưu danh sách coin mới
        
        # Gửi thông báo
        text = "\n".join(alerts)
        # Channel sending disabled for single-owner private mode (no group/channel notifications)
        # Gửi cho subscribers cá nhân
        for chat in SUBSCRIBERS:
            try:
                await context.bot.send_message(chat, text, parse_mode="Markdown")
            except Exception as e:
                print(f"❌ Lỗi gửi thông báo coin mới: {e}")


async def job_schedule_restarts(context):
    """Job lên lịch restart bot khi có coin mới list"""
    async with aiohttp.ClientSession() as session:
        try:
            # Gọi API calendar để lấy lịch listing
            timestamp = int(datetime.now().timestamp() * 1000)
            url = f"https://www.mexc.co/api/operation/new_coin_calendar?timestamp={timestamp}"
            
            async with session.get(url, timeout=15) as r:
                if r.status != 200:
                    return
                
                data = await r.json()
                coins = data.get('data', {}).get('newCoins', [])
                
                if not coins:
                    return
                
                vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
                now = datetime.now(vn_tz)
                next_24h = now + timedelta(hours=24)
                
                for coin in coins:
                    timestamp_ms = coin.get('firstOpenTime')
                    if not timestamp_ms:
                        continue
                    
                    # Convert timestamp sang giờ VN
                    dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                    list_time = dt_utc.astimezone(vn_tz)
                    
                    # Chỉ schedule cho coin list trong 24h tới
                    if now <= list_time <= next_24h:
                        # Tránh schedule trùng
                        if timestamp_ms in SCHEDULED_RESTARTS:
                            continue
                        
                        SCHEDULED_RESTARTS.add(timestamp_ms)
                        
                        # Tính thời gian chờ
                        wait_seconds = (list_time - now).total_seconds()
                        wait_seconds_plus_1h = wait_seconds + 3600  # +1 tiếng
                        
                        if wait_seconds > 0:
                            coin_name = coin.get('vcoinName', 'Unknown')
                            print(f"📅 Đã lên lịch restart cho {coin_name}:")
                            print(f"   - Restart 1: {list_time.strftime('%d/%m %H:%M')} ({wait_seconds/60:.0f} phút)")
                            print(f"   - Restart 2: {(list_time + timedelta(hours=1)).strftime('%d/%m %H:%M')} (sau 1h)")
                            
                            # Schedule restart lần 1 (đúng giờ list)
                            context.job_queue.run_once(
                                restart_bot,
                                wait_seconds,
                                data={"reason": f"Coin mới list: {coin_name}"}
                            )
                            
                            # Schedule restart lần 2 (sau 1 tiếng)
                            context.job_queue.run_once(
                                restart_bot,
                                wait_seconds_plus_1h,
                                data={"reason": f"Restart lần 2 sau khi {coin_name} list"}
                            )
        
        except Exception as e:
            print(f"❌ Lỗi schedule restart: {e}")


async def restart_bot(reason="Scheduled restart"):
    """Khởi động lại bot bằng cách tái chạy script hiện tại."""
    print(f"🔄 {reason} - Đang khởi động lại bot...")
    save_data()  # Lưu dữ liệu trước khi restart
    os.execv(sys.executable, [sys.executable] + sys.argv)


async def check_new_coins_and_restart():
    """Kiểm tra coin mới và khởi động lại bot nếu cần."""
    global KNOWN_SYMBOLS
    async with aiohttp.ClientSession() as session:
        new_symbols = set(await get_all_symbols(session))
        if not KNOWN_SYMBOLS.issuperset(new_symbols):
            print("🔄 Phát hiện coin mới, khởi động lại bot...")
            KNOWN_SYMBOLS = new_symbols  # Cập nhật danh sách coin
            await restart_bot("Phát hiện coin mới")


async def schedule_periodic_restarts():
    """Lên lịch khởi động lại bot mỗi 1 tiếng."""
    while True:
        await asyncio.sleep(3600)  # Chờ 1 tiếng
        await restart_bot("Khởi động lại định kỳ")


async def restart_bot_at_specific_times():
    """Khởi động lại bot vào các thời điểm cụ thể (14h và 15h giờ Việt Nam)."""
    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    while True:
        now = datetime.now(vn_tz)
        next_14h = now.replace(hour=14, minute=0, second=0, microsecond=0)
        next_15h = now.replace(hour=15, minute=0, second=0, microsecond=0)

        # Nếu đã qua giờ 14h hoặc 15h, đặt lịch cho ngày hôm sau
        if now >= next_14h:
            next_14h += timedelta(days=1)
        if now >= next_15h:
            next_15h += timedelta(days=1)

        # Tính thời gian chờ đến lần restart tiếp theo
        wait_until_14h = (next_14h - now).total_seconds()
        wait_until_15h = (next_15h - now).total_seconds()

        # Chờ đến 14h để restart
        print(f"⏳ Chờ đến 14h (giờ VN) để khởi động lại bot...")
        await asyncio.sleep(wait_until_14h)
        await restart_bot("Khởi động lại lúc 14h giờ VN")

        # Chờ đến 15h để restart lần nữa
        print(f"⏳ Chờ đến 15h (giờ VN) để khởi động lại bot...")
        await asyncio.sleep(wait_until_15h - wait_until_14h)  # Chỉ chờ thêm thời gian giữa 14h và 15h
        await restart_bot("Khởi động lại lúc 15h giờ VN")

        # Sau 15h, không restart nữa trong ngày
        print("✅ Đã hoàn thành lịch restart cho hôm nay.")


async def fetch_coin_list_schedule(session):
    """Lấy lịch coin list từ API và trả về danh sách thời gian list (giờ VN)."""
    url = f"{FUTURES_BASE}/api/v1/contract/coinlist"  # Giả sử đây là endpoint lấy lịch coin list
    data = await fetch_json(session, url)
    
    # Giả sử API trả về danh sách các coin với thời gian list dạng timestamp
    schedules = []
    for coin in data.get("coins", []):
        list_time = datetime.fromtimestamp(coin["list_time"], pytz.utc).astimezone(pytz.timezone("Asia/Ho_Chi_Minh"))
        schedules.append(list_time)
    
    return schedules

async def schedule_restarts_based_on_coin_list():
    """Lên lịch khởi động lại dựa trên lịch coin list."""
    async with aiohttp.ClientSession() as session:
        schedules = await fetch_coin_list_schedule(session)

    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    now = datetime.now(vn_tz)

    for list_time in schedules:
        # Bỏ qua các lịch đã qua
        if list_time <= now:
            continue

        # Lên lịch khởi động lại 1 tiếng sau khi có lịch coin list
        restart_after_schedule = (list_time + timedelta(hours=1)).astimezone(vn_tz)
        delay_after_schedule = (restart_after_schedule - now).total_seconds()
        if delay_after_schedule > 0:
            print(f"⏳ Lên lịch khởi động lại 1 tiếng sau khi có lịch coin list: {restart_after_schedule}")
            asyncio.create_task(schedule_restart(delay_after_schedule, "1 tiếng sau khi có lịch coin list"))

        # Lên lịch khởi động lại thêm 1 tiếng sau lần khởi động đầu tiên
        restart_final = (list_time + timedelta(hours=2)).astimezone(vn_tz)
        delay_final = (restart_final - now).total_seconds()
        if delay_final > 0:
            print(f"⏳ Lên lịch khởi động lại lần cuối cùng: {restart_final}")
            asyncio.create_task(schedule_restart(delay_final, "Lần khởi động lại cuối cùng"))

async def schedule_restart(delay, reason):
    """Chờ một khoảng thời gian và khởi động lại bot."""
    await asyncio.sleep(delay)
    await restart_bot(reason)

# ...existing code...

async def main():
    """Hàm chính để chạy bot."""
    load_data()  # Tải dữ liệu khi khởi động bot

    # Nếu OWNER_ID được cấu hình, đảm bảo chỉ owner là subscriber
    if OWNER_ID is not None:
        SUBSCRIBERS.clear()
        SUBSCRIBERS.add(OWNER_ID)
        save_data()

    # Chạy các task song song
    await asyncio.gather(
        schedule_restarts_based_on_coin_list(),  # Lên lịch khởi động lại dựa trên lịch coin list
        start_websocket_listener(),  # Giả sử đây là hàm lắng nghe WebSocket
    )

# Đảm bảo rằng hàm start_websocket_listener được định nghĩa hoặc thay thế bằng hàm phù hợp
async def start_websocket_listener():
    """Giả sử đây là hàm lắng nghe WebSocket, cần được định nghĩa."""
    pass


if __name__ == "__main__":
    asyncio.run(main())
