FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY *.py /app/
COPY run_sonarr.sh run_radarr.sh run_lidarr.sh /app/
RUN chmod +x /app/run_sonarr.sh /app/run_radarr.sh /app/run_lidarr.sh

CMD ["/app/run_sonarr.sh"]
