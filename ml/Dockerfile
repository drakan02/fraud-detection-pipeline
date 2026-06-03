FROM python:3.12-slim

WORKDIR /app

# Copy requirements
COPY ml/requirements_ml.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements_ml.txt

# Copy ml package code
COPY ml/ ./ml/

# Ensure directory for models is present
RUN mkdir -p ml/models

# Expose model server port
EXPOSE 8001

# Run the FastAPI server in module mode
CMD ["python", "-m", "ml.model_server"]
