# Base image
FROM python:3.9-slim-buster

ARG NOMBRE_PROYECTO

ENV http_proxy "http://proxy-azure.grupo.ypf.com:80"
ENV https_proxy "http://proxy-azure.grupo.ypf.com:80"
ENV EP="src/api.py"
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 

# Instalar dependencias
WORKDIR $NOMBRE_PROYECTO
COPY setup.py setup.py
COPY requirements.txt requirements.txt
COPY requirements-docs.txt requirements-docs.txt
COPY requirements-quality.txt requirements-quality.txt
COPY requirements-security.txt requirements-security.txt
COPY requirements-test.txt requirements-test.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m pip install --upgrade pip setuptools wheel \
    && python3 -m pip install -e . --no-cache-dir \
    && python3 -m pip install protobuf==3.20.1 --no-cache-dir \
    && apt-get purge -y --auto-remove gcc build-essential

# Copy
COPY src src
COPY models models
COPY config config
COPY fflag.txt fflag.txt

ENTRYPOINT "python3" $EP
