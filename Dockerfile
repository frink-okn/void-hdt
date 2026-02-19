# Build stage: compile dependencies
# Use bullseye (Debian 11) with GCC 10 instead of bookworm with GCC 12+
# rdflib-hdt 3.2 has C++ code missing #include <cstdint>, which fails on newer GCC
FROM python:3.12-bullseye AS builder

# Set working directory
WORKDIR /app

# Install uv for fast, reliable dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set environment variables for uv
ENV UV_COMPILE_BYTECODE=1

# Copy dependency files first for better caching
COPY pyproject.toml uv.lock ./

# Install dependencies (this creates .venv with compiled packages)
# --frozen ensures we use the exact versions from uv.lock
# --no-dev skips development dependencies
RUN uv sync --frozen --no-dev

# Copy the application code
COPY void_hdt/ ./void_hdt/

# Install the project itself into the venv
RUN uv sync --frozen --no-dev

# Runtime stage: slim image with only runtime dependencies
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install runtime C++ libraries (needed to run the compiled rdflib-hdt)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Copy the virtual environment
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/void_hdt /app/void_hdt

# Place the venv binaries on the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Ensure Python can find the module in /app
ENV PYTHONPATH="/app"

# Set the entrypoint to use uv run (handles environment automatically)
ENTRYPOINT ["void-hdt"]

# Default help command if no args provided
CMD ["--help"]
