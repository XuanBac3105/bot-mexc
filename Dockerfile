FROM python:3.11-slim

WORKDIR /app

# Copy requirements và cài đặt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY mexc_futures_bot.py .
COPY .env .

# Chạy bot
CMD ["python", "-u", "mexc_futures_bot.py"]
