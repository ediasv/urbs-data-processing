FROM python:3.10-slim

RUN apt-get -qq update \
    && apt-get install -y --no-install-recommends openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install --no-cache-dir -e .
