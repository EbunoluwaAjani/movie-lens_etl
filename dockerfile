# Use Python 3.11-slim as the base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /etl

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --upgrade --no-cache-dir gdown

# Copy the .env file into the container
COPY .env .

# Copy the rest of the application code
COPY ml-100k-data/ data/
COPY etl/ .

# Define the entrypoint for running the ETL script
ENTRYPOINT ["python", "etl.py"]