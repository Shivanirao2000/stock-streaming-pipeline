FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    faust-streaming==0.10.14 \
    asyncpg==0.29.0

COPY app.py .

CMD ["faust", "-A", "app", "worker", "-l", "info"]
