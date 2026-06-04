# --- Stage 1: Build React Dashboard ---
FROM node:20-alpine AS frontend-builder
WORKDIR /app

# Install dependencies first for better caching
COPY ml/dashboard/package*.json ./ml/dashboard/
RUN cd ml/dashboard && npm install

# Copy dashboard source and build it
COPY ml/dashboard/ ./ml/dashboard/
RUN cd ml/dashboard && npm run build

# --- Stage 2: Serve python model server ---
FROM python:3.12-slim
WORKDIR /app

# Install python dependencies
COPY ml/requirements_ml.txt .
RUN pip install --no-cache-dir -r requirements_ml.txt

# Copy ml package code
COPY ml/ ./ml/

# Copy the compiled static assets from the builder stage
# (Vite output dir is ../static relative to dashboard, which resolves to /app/ml/static)
COPY --from=frontend-builder /app/ml/static/ ./ml/static/

# Ensure directory for models is present
RUN mkdir -p ml/models

# Expose model server port
EXPOSE 8001

# Run the FastAPI server in module mode
CMD ["python", "-m", "ml.model_server"]
