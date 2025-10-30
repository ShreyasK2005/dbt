FROM python:3.13-slim-bullseye

# Set environment variables
ENV DBT_PROFILES_DIR="/usr/src/app/container_profile"

ARG DBT_ENVIRONMENT
ARG DBT_SNOWFLAKE_ACCOUNT
ARG DBT_DATABASE
ARG DBT_ROLE
ARG DBT_SCHEMA
ARG DBT_WAREHOUSE
ARG DBT_USER
ARG DBT_PK
ARG DBT_PK_PWD

# Need to set this to either dev, test or prod depending on the container environment
ENV DBT_ENVIRONMENT=${DBT_ENVIRONMENT}
ENV DBT_SNOWFLAKE_ACCOUNT=${DBT_SNOWFLAKE_ACCOUNT}
ENV DBT_DATABASE=${DBT_DATABASE}
ENV DBT_ROLE=${DBT_ROLE}
ENV DBT_SCHEMA=${DBT_SCHEMA}
ENV DBT_WAREHOUSE=${DBT_WAREHOUSE}
ENV DBT_USER=${DBT_USER}
ENV DBT_PK=${DBT_PK}
ENV DBT_PK_PWD=${DBT_PK_PWD}

# Install system dependencies (if needed for your dbt adapter)
# Example for Snowflake:
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dbt Core and your adapter(s)
RUN pip install --no-cache-dir dbt-core dbt-snowflake

# Replaced by DBT_PROFILES_DIR
# COPY profiles.yml /root/.dbt/profiles.yml

# Copy your dbt project files (optional - if you're not mounting a volume)
COPY . /usr/src/app
WORKDIR /usr/src/app

RUN dbt deps

# Command to run if no arguments are provided (dbt compile)
CMD ["dbt", "compile"]