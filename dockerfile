FROM python:3.11-slim

WORKDIR /etl
RUN ls -la

COPY etl/requirements.txt .
COPY ml-100k-data/ data/

RUN pip install --upgrade pip 
RUN pip install --no-cache-dir -r requirements.txt

# Copy the .env file into the container
COPY .env .

# Copy the rest of the application code
COPY etl/ .

ENTRYPOINT ["python", "etl.py"]
