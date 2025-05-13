# Stage 1: Build
FROM python:3.10.8 as builder

WORKDIR /app

# Copy requirements.txt (we assume build context is project root!)
COPY requirements.txt .

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev

# Build wheels and upgrade pip
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

# Install OCR and PDF dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-vie \
    tesseract-ocr-spa \
    ghostscript

# Copy built wheels and install Python dependencies
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

RUN pip install --no-cache-dir /wheels/*

# Copy backend folder into /app/backend/
COPY backend/ backend/

#  Set PYTHONPATH so backend package can be found
ENV PYTHONPATH=/app

# Change ownership to non-root user
RUN chown -R talentsync:talentsync /app

USER talentsync

#  Use correct module path from backend/
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
