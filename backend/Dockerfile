# Stage 1: Build
FROM python:3.10.8 as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev

COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt && \
    pip install --upgrade pip

# Stage 2: Runtime
FROM python:3.10.8

LABEL maintainer="Basudev Samanta <basudev@klizos.com>"
LABEL description="Talentsync Backend"

WORKDIR /app

# Create non-root user
RUN adduser --disabled-password --gecos '' talentsync

# Install runtime dependencies and pdftotext binary
RUN wget --no-verbose -O /tmp/xpdf-tools.tar.gz https://dl.xpdfreader.com/xpdf-tools-linux-4.05.tar.gz && \
    mkdir /tmp/xpdf && \
    tar -xzf /tmp/xpdf-tools.tar.gz -C /tmp/xpdf --strip-components=1 && \
    cp /tmp/xpdf/bin64/pdftotext /usr/bin/ && \
    cp /tmp/xpdf/bin64/pdfinfo /usr/bin/ && \
    rm -rf /tmp/xpdf-tools.tar.gz /tmp/xpdf

# Install OCR and PDF processing dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-vie \
    tesseract-ocr-spa \
    ghostscript

# Copy wheels and requirements from builder
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

# Install dependencies from wheels
RUN pip install --no-cache-dir /wheels/*

# Copy application and templates
COPY ./app ./app
COPY ./templates ./templates

# Change ownership
RUN chown -R talentsync:talentsync /app

USER talentsync

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
