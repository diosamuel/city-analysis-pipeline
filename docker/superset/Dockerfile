ARG SUPERSET_VERSION=4.1.4
FROM apache/superset:${SUPERSET_VERSION}

USER root
RUN pip install --no-cache-dir \
    "psycopg2-binary>=2.9.9" \
    "duckdb-engine>=0.13.0" \
    "duckdb>=1.0.0" \
    "sqlalchemy-bigquery>=1.11.0" \
    "google-cloud-bigquery>=3.25.0" \
    "pyarrow>=14.0.0"

COPY superset/superset_config.py /app/pythonpath/superset_config.py
COPY superset/bootstrap.sh /app/bootstrap-custom.sh
RUN chmod +x /app/bootstrap-custom.sh && chown superset:superset /app/pythonpath/superset_config.py /app/bootstrap-custom.sh

USER superset
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py

ENTRYPOINT []
CMD ["/app/bootstrap-custom.sh"]
