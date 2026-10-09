# ScanB Messaging API (and, with a different command, the reminder worker).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

RUN useradd --create-home --uid 10001 messaging
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY messaging ./messaging
COPY dev ./dev

USER messaging
EXPOSE 8010

# API:     default command
# Worker:  python -m messaging.worker   (or --once as a scheduled job)
CMD ["uvicorn", "messaging.main:app", "--host", "0.0.0.0", "--port", "8010", "--proxy-headers", "--no-server-header"]
