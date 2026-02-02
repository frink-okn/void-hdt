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

# Runtime stage: slim image with only runtime dependencies
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install uv in runtime image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy the virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy the application code
COPY --from=builder /app/void_hdt /app/void_hdt

# Copy project files needed by uv
COPY pyproject.toml uv.lock ./

# Set the entrypoint to use uv run (handles environment automatically)
ENTRYPOINT ["uv", "run", "void-hdt"]

# Default help command if no args provided
CMD ["--help"]
