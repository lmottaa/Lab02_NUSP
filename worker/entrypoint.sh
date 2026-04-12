#!/bin/sh

echo "dropando schema..."

psql postgresql://postgres:postgres@db:5432/postgres -f /app/worker/schema/drop_schema.sql

echo "criando schema..."

psql postgresql://postgres:postgres@db:5432/postgres -f /app/worker/schema/gold_schema.sql

echo "executando pipeline..."

uv run python /app/worker/worker.py