FROM python:3.12

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY . .

RUN apt-get update && apt-get install -y postgresql-client

RUN sed -i 's/\r$//' /app/worker/entrypoint.sh

ENTRYPOINT ["/app/worker/entrypoint.sh"]