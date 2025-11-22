# MEXC Futures Alert Bot - Hướng dẫn sử dụng Channel

## Tại sao dùng Channel?

✅ **Quản lý dễ dàng**: Kick/ban user ngay lập tức  
✅ **Tập trung**: Tất cả alert ở 1 nơi  
✅ **Kiểm soát**: Xem ai đang subscribe  
✅ **Có thể charge phí**: Dùng Telegram Premium Channel  

---

## Cách setup Channel

### 1. Tạo Channel riêng tư

1. Mở Telegram → **New Channel**
2. Đặt tên: `MEXC Futures Alerts` (hoặc tên bạn thích)
3. Chọn **Private** (riêng tư)
4. Tạo link mời (vd: `https://t.me/+AbCdEfGh123`)

### 2. Thêm Bot vào Channel

1. Vào Channel → **Manage Channel**
2. **Administrators** → **Add Administrator**
3. Tìm bot của bạn
4. Cho quyền: **Post Messages** (đủ rồi)

### 3. Lấy Channel ID

**Cách 1: Dùng bot @RawDataBot**
1. Forward 1 tin nhắn từ channel vào bot @RawDataBot
2. Tìm `"chat":{"id":-1001234567890}` → đó là Channel ID
3. Copy số `-1001234567890`

**Cách 2: Dùng username (nếu là public channel)**
1. Nếu channel có username: `@your_channel`
2. Dùng luôn `@your_channel` làm CHANNEL_ID

### 4. Lấy Admin ID (BẮT BUỘC khi dùng channel!)

**Tại sao cần Admin ID?**
- Bảo vệ bot: chỉ admin mới dùng được `/mute`, `/subscribe`, `/mode1`, `/mode2`
- Người khác chỉ xem alert trong channel, không điều khiển bot được

**Cách lấy:**
1. Chat với bot @userinfobot
2. Copy số `Id: 123456789` → đó là User ID của bạn
3. Nhiều admin: cách nhau bằng dấu phẩy

### 5. Cấu hình Bot

Sửa file `.env`:
```env
BOT_TOKEN=123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11
CHANNEL_ID=-1001234567890
ADMIN_IDS=123456789,987654321
```

**⚠️ QUAN TRỌNG**: Nếu không set `ADMIN_IDS`, bất kỳ ai cũng điều khiển bot được!

### 6. Deploy và Test

```bash
# Local
python mexc_futures_bot.py

# Railway
git push origin main
```

Bot sẽ gửi alert vào channel thay vì PM từng user!

---

## Quản lý Members

### Mời người dùng
1. Share link mời: `https://t.me/+AbCdEfGh123`
2. Họ click vào → tự động join → nhận alert

### Kick người dùng
1. **Channel** → **Subscribers**
2. Tìm người đó → **Remove from channel**
3. Họ mất quyền xem ngay lập tức

### Revoke link mời
1. **Channel** → **Invite Links**
2. Xóa link cũ → tạo link mới
3. Link cũ không dùng được nữa

---

## Tính năng nâng cao

### Charge phí (Premium Channel)
- Tạo **Paid Channel** trong Telegram
- Set giá: $1-10/tháng
- Telegram tự động thu phí

### Multiple Channels
Tạo nhiều channel cho level khác nhau:
- **Free Channel**: Alert cơ bản (≥3%)
- **Premium Channel**: Alert tất cả (≥2.5%) + coin mới

Trong code, thêm:
```python
CHANNEL_ID_FREE = "-1001111111111"
CHANNEL_ID_PREMIUM = "-1002222222222"
```

---

## Hybrid Mode (Channel + Private)

Bot có thể gửi **ĐỒNG THỜI** vào:
- Channel (cho đám đông)
- Private chat (cho admin/VIP)

Chỉ cần:
1. Set `CHANNEL_ID` trong `.env`
2. Admin vẫn `/subscribe` để nhận riêng
3. Có thể `/mute COIN` riêng cho mình

---

## Troubleshooting

### Bot không gửi được vào channel?
- ✅ Check bot đã là Admin của channel
- ✅ Check bot có quyền **Post Messages**
- ✅ Check CHANNEL_ID đúng format (-100xxx hoặc @xxx)

### Làm sao biết Channel ID?
- Forward tin nhắn từ channel vào @RawDataBot
- Hoặc dùng @userinfobot

### Làm sao biết User ID của mình?
- Chat với @userinfobot
- Copy số `Id: 123456789`

### Người khác vẫn dùng được bot commands?
- ✅ Phải set `ADMIN_IDS` trong `.env`
- ✅ Restart bot sau khi thêm ADMIN_IDS
- ✅ Test: người khác chat `/mute BTC` → nhận "⛔ Lệnh này chỉ dành cho admin"

### Muốn gửi vào nhiều channel?
- Sửa code thêm `CHANNEL_ID_2`, `CHANNEL_ID_3`
- Hoặc dùng array: `CHANNEL_IDS = [id1, id2, id3]`

---

## So sánh Private Chat vs Channel

| Tính năng | Private Chat | Channel |
|-----------|-------------|---------|
| Quản lý user | Khó (không thấy list) | Dễ (xem subscribers) |
| Kick user | Không thể | Ngay lập tức |
| Charge phí | Thủ công | Tự động (Premium) |
| Scale | Tốt (1000+ users) | Tốt (unlimited) |
| Privacy | Cao (1-1) | Vừa (nhìn thấy members) |

---

## Tips

💡 **Tạo 2 channel**: 1 Free (test), 1 Premium (real money)  
💡 **Pin thông báo quan trọng** trong channel  
💡 **Tắt comments** nếu không muốn spam  
💡 **Backup link mời** để không mất khi revoke  
