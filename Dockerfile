FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# The core is pure standard library. Only the tracker's optional PostgreSQL
# metadata store needs a driver.
COPY requirements-postgres.txt .
RUN pip install --no-cache-dir -r requirements-postgres.txt
COPY common/ common/
COPY node/ node/
COPY tracker/ tracker/
COPY client/ client/

# Run unprivileged; /data is where volumes get mounted.
RUN useradd --create-home vektor && mkdir /data /transfer && chown vektor /data /transfer
USER vektor
EXPOSE 9000 9100
