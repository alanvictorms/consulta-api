FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /data

ENV PORT=80
ENV DB_PATH=/data/dataflow.db
ENV SECRET_KEY=change-me-in-production

EXPOSE 80

VOLUME ["/data"]

CMD ["gunicorn", "--bind", "0.0.0.0:80", "--workers", "1", "--threads", "4", "--timeout", "120", "--access-logfile", "-", "consulta_nome_app:app"]
