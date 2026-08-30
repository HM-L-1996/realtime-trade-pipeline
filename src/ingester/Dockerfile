FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/ingester/
ENV PYTHONUNBUFFERED=1
EXPOSE 9200

CMD ["python", "-m", "ingester.main"]
