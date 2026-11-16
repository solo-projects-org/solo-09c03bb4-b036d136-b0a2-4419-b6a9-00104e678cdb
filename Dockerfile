# Shared image: runs either the API or a SCPI simulator via the command.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY instrument ./instrument

# API port (uvicorn).  Simulators expose raw TCP 5025 instead.
EXPOSE 8000 5025

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
