FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir pymongo requests python-dateutil

COPY src/ ./src/

CMD ["python", "-u", "src/velib_ingest.py"]
