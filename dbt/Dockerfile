FROM python:3.11-slim

WORKDIR /dbt
COPY . .

RUN pip install --no-cache-dir dbt-bigquery==1.9.1

RUN dbt deps --profiles-dir .

ENTRYPOINT ["dbt", "build", "--profiles-dir", "."]
