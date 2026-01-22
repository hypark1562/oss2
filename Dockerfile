# Use slim variant to minimize image size and reduce attack surface
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker layer caching
# This prevents re-installing dependencies if only source code changes
COPY requirements.txt .

# Install dependencies with no-cache-dir to reduce image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Define the entry point for the pipeline
CMD ["python", "main.py"]
