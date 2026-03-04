FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY . /app

# Render sets PORT automatically. Default local fallback is 8000.
ENV PORT=8000

CMD ["sh", "-c", "mkdir -p data/artifacts static/generated && uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
