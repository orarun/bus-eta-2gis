FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn requests httpx
COPY app/app.py .

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

