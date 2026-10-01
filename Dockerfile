FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# The runtime is pure standard library: nothing to pip install.
COPY common/ common/
COPY node/ node/
COPY tracker/ tracker/
COPY client/ client/

# Run unprivileged; /data is where volumes get mounted.
RUN useradd --create-home vektor && mkdir /data /transfer && chown vektor /data /transfer
USER vektor
EXPOSE 9000 9100
