FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --retries 3 --timeout 30 -r requirements.txt

COPY . .

CMD ["python", "-u", "test_compat.py"]
