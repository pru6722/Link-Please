FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy and install python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code files
COPY . .

# Set production environment defaults
ENV PORT=8000
ENV HOST=0.0.0.0
ENV DATABASE_URL="sqlite:////Users/prudhvisailingineni/Desktop/LinkPlease/linkplease.db"

# Expose port
EXPOSE 8000

# Start application using uvicorn
CMD uvicorn main:app --host $HOST --port $PORT
