FROM python:3.11-slim

WORKDIR /etl


COPY requirements.txt .


RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --upgrade --no-cache-dir gdown

COPY .env .

COPY ml-100k-data/ data/
COPY etl/ .

ENTRYPOINT ["python", "etl.py"]