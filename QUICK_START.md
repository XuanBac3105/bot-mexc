# 🚀 PUSH LÊN GITHUB - 3 BƯỚC ĐƠN GIẢN

## ✅ Đã dọn dẹp xong!

Thư mục `d:\boy` giờ chỉ có **6 files** cần thiết:

```
d:\boy/
├── .gitignore              (122 bytes)   - Loại trừ files không cần
├── mexc_futures_bot.py     (9 KB)        - Code bot chính
├── Procfile                (33 bytes)    - Start command
├── README.md               (5 KB)        - Hướng dẫn đầy đủ
├── requirements.txt        (55 bytes)    - Dependencies
└── runtime.txt             (15 bytes)    - Python version
```

---

## 📤 BƯỚC 1: Push lên GitHub

### Cách 1: Dùng GitHub Desktop (Dễ nhất - Khuyến nghị)

1. **Tải GitHub Desktop:** https://desktop.github.com/
2. **Đăng nhập** GitHub account
3. **File → Add Local Repository:**
   - Path: `D:\boy`
   - Click **Add Repository**
4. **Publish repository:**
   - Uncheck "Keep this code private" (để public)
   - Name: `mexc-futures-bot`
   - Click **Publish repository**

✅ **Done! Code đã lên GitHub**

---

### Cách 2: Dùng Command Line

```powershell
cd D:\boy

# Khởi tạo Git
git init

# Add tất cả files
git add .

# Commit
git commit -m "Initial commit: MEXC Futures Alert Bot"

# Tạo repo trên GitHub Web (https://github.com/new)
# Sau đó link repo:
git remote add origin https://github.com/YOUR_USERNAME/mexc-futures-bot.git

# Push lên GitHub
git branch -M main
git push -u origin main
```

---

## ⚡ BƯỚC 2: Deploy lên Railway

1. **Vào:** https://railway.app
2. **Login with GitHub**
3. **New Project** → **Deploy from GitHub repo**
4. **Chọn repo:** `mexc-futures-bot`
5. **Click Deploy Now**

Đợi 3-5 phút → Bot sẽ online!

---

## 📱 BƯỚC 3: Test Bot

1. **Mở Telegram**
2. **Tìm bot** (tên bạn đã tạo ở @BotFather)
3. **Gửi:** `/start`
4. **Gửi:** `/subscribe`

✅ **Bot sẽ tự động gửi thông báo khi có pump/dump!**

---

## 🎉 HOÀN THÀNH!

Bot giờ chạy 24/7 trên cloud, tự động gửi thông báo:

```
┌🚀🚀🚀 SOL ⚡ +8.45% 🟢
└ 145.50 → 157.79
```

---

## 🔐 BẢO MẬT (Tùy chọn)

Để ẩn BOT_TOKEN trong Railway:

1. **Railway Dashboard** → Project
2. **Variables** → **New Variable**
3. Key: `BOT_TOKEN`
4. Value: `YOUR_TELEGRAM_BOT_TOKEN`
5. **Deploy lại**

Code đã tự động đọc environment variable!

---

## 📝 CẬP NHẬT CODE SAU NÀY

```powershell
cd D:\boy

# Sửa code...

git add .
git commit -m "Update: mô tả thay đổi"
git push

# Railway tự động deploy lại!
```

---

## 💡 MẸO HAY

### Kiểm tra bot có chạy không:
- Railway Dashboard → **Logs**
- Xem dòng: "🔥 Bot quét MEXC Futures đang chạy..."

### Restart bot:
- Railway → **Settings** → **Redeploy**

### Xem logs real-time:
- Railway → **Logs** tab

---

## 🆘 NẾU GẶP VẤN ĐỀ

**Build failed trên Railway:**
- Check file `requirements.txt` có đúng không
- Check file `runtime.txt` (python-3.10.0)

**Bot không phản hồi:**
- Check logs có lỗi gì
- Verify BOT_TOKEN đúng chưa
- Test lại với @BotFather

---

## ✨ THÀNH CÔNG!

Bot của bạn giờ:
- ✅ Chạy 24/7 trên cloud
- ✅ Không cần máy tính bật
- ✅ Miễn phí ($5 credit/tháng)
- ✅ Tự động update khi push code

**Chúc bạn trade thành công! 🚀💰**
