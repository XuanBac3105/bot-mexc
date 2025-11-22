# 🤖 MEXC Futures Alert Bot

Bot Telegram tự động quét **TẤT CẢ coin** trên MEXC Futures và gửi thông báo ngay lập tức khi phát hiện biến động giá mạnh.

![Telegram Bot](https://img.shields.io/badge/Telegram-Bot-blue?logo=telegram)
![Python](https://img.shields.io/badge/Python-3.10-green?logo=python)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## ✨ Tính năng

- 🔍 **Quét tự động** tất cả USDT Futures trên MEXC (hàng trăm coin)
- ⚡ **Alert Pump/Dump** ngay lập tức khi coin tăng/giảm >= 5% trong 5 phút
- 🆕 **Phát hiện coin mới list**
- 📊 **Lọc volume** - Chỉ báo coin có thanh khoản tốt (>100k USDT)
- 💬 **Giao diện đẹp** với emoji và format rõ ràng

## 📊 Format Thông Báo

```
┌🚀🚀🚀 SOL ⚡ +8.45% 🟢
└ 145.50 → 157.79

┌💥💥💥 DOGE ⚡ -6.23% 🔴
└ 0.0875 → 0.0820
```

---

## 🚀 Deploy lên Cloud (Khuyến nghị - Chạy 24/7 miễn phí)

### Railway.app - Tốt nhất! ⭐⭐⭐⭐⭐

1. **Push code lên GitHub:**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/YOUR_USERNAME/mexc-futures-bot.git
   git push -u origin main
   ```

2. **Deploy lên Railway:**
   - Vào: https://railway.app
   - Login with GitHub
   - New Project → Deploy from GitHub repo
   - Chọn repo `mexc-futures-bot`
   - Click Deploy!

3. **(Tùy chọn) Ẩn BOT_TOKEN:**
   - Variables → Add Variable
   - Key: `BOT_TOKEN`
   - Value: `YOUR_TELEGRAM_BOT_TOKEN`

4. **Test bot:**
   - Mở Telegram, tìm bot của bạn
   - `/start` → `/subscribe`
   - Done! ✅

### So sánh các platform:

| Platform | Free | Sleep? | Setup | Rating |
|----------|------|--------|-------|--------|
| [Railway](https://railway.app) | $5/tháng | ❌ | 5 phút | ⭐⭐⭐⭐⭐ |
| [Render](https://render.com) | Free | ✅ 15p | 10 phút | ⭐⭐⭐ |
| [Fly.io](https://fly.io) | Free | ❌ | 15 phút | ⭐⭐⭐⭐ |

---

## 🖥️ Chạy trên máy local

### Yêu cầu:
- Python 3.10+
- VPN (nếu ở Việt Nam)

### Cài đặt:

```bash
# Clone repo
git clone https://github.com/YOUR_USERNAME/mexc-futures-bot.git
cd mexc-futures-bot

# Install dependencies
pip install -r requirements.txt

# Set BOT_TOKEN (Windows)
set BOT_TOKEN=YOUR_TELEGRAM_BOT_TOKEN

# Hoặc sửa trực tiếp trong mexc_futures_bot.py

# Run bot
python mexc_futures_bot.py
```

⚠️ **Lưu ý:** Cần VPN và máy bật 24/7

---

## 🔧 Cấu hình

Chỉnh sửa trong `mexc_futures_bot.py`:

```python
# Ngưỡng báo động
PUMP_THRESHOLD = 5.0    # Báo khi tăng >= 5%
DUMP_THRESHOLD = -5.0   # Báo khi giảm >= 5%

# Volume tối thiểu
MIN_VOL_THRESHOLD = 100000  # Chỉ báo coin volume >= 100k
```

---

## 💬 Lệnh Telegram

- `/start` - Bắt đầu & xem hướng dẫn
- `/subscribe` - Bật báo động tự động
- `/unsubscribe` - Tắt báo động
- `/top10` - Xem top 10 gainers/losers

---

## 📱 Tạo Telegram Bot

1. Telegram → Tìm **@BotFather**
2. Gửi `/newbot`
3. Đặt tên & username
4. Copy TOKEN
5. Paste vào code hoặc environment variable

---

## 🎯 Cách hoạt động

1. **Quét mỗi 2 phút:**
   - Lấy danh sách tất cả USDT Futures từ MEXC
   - Lấy giá 5 phút gần nhất
   - Tính % thay đổi

2. **Phát hiện Pump/Dump:**
   - Tăng >= 5% → Alert 🚀
   - Giảm >= 5% → Alert 💥

3. **Lọc chất lượng:**
   - Chỉ báo coin volume >= 100k
   - Tránh coin ít thanh khoản

---

## 📁 Files trong project

```
├── mexc_futures_bot.py    # Code chính
├── requirements.txt        # Dependencies
├── runtime.txt            # Python version
├── Procfile              # Deploy command
├── .gitignore            # Git exclude
└── README.md             # Docs này
```

---

## 🔍 Troubleshooting

**NetworkError: Không kết nối Telegram**
- ✅ Dùng VPN hoặc deploy lên cloud

**Bot không gửi alert**
- ✅ Đã `/subscribe`?
- ✅ Ngưỡng có quá cao?
- ✅ Check logs

**Token invalid**
- ✅ Lấy token mới từ @BotFather

---

## 📈 Nâng cao

- Monitor nhiều exchanges (Binance, Bybit)
- Thêm RSI, MACD, Bollinger Bands
- Lưu database lịch sử pump/dump
- Backtest strategies

---

## ⚠️ Disclaimer

- Bot CHỈ cung cấp thông tin, KHÔNG phải lời khuyên đầu tư
- Crypto có rủi ro cao
- Tự chịu trách nhiệm với quyết định của mình
- Bot có thể có bug hoặc miss signals

---

## 📜 License

MIT License - Free to use at your own risk

---

## 🤝 Contributing

PRs welcome!

1. Fork repo
2. Create branch (`git checkout -b feature/AmazingFeature`)
3. Commit (`git commit -m 'Add AmazingFeature'`)
4. Push (`git push origin feature/AmazingFeature`)
5. Open Pull Request

---

## ⭐ Star this repo!

Nếu thấy hữu ích, hãy star ⭐ repo này!

---

**Made with ❤️ for crypto traders**

🚀 Happy Trading! 🚀
