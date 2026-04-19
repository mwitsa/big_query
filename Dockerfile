FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py scheduler.py drive_upload.py ./

CMD ["python", "scheduler.py"]
