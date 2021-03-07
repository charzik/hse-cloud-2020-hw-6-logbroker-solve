FROM python:3.8-slim

COPY requirements.txt /
RUN pip install -r requirements.txt

RUN mkdir /app/
COPY . /app/
WORKDIR /app/worker

EXPOSE 8000
CMD ["python", "worker.py"]
