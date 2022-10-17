FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 fastparquet

WORKDIR /app
COPY taxi_data_ingest.py taxi_data_ingest.py

ENTRYPOINT [ "python", "taxi_data_ingest.py" ]