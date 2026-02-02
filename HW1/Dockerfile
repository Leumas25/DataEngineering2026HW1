FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml .python-version uv.lock ./


COPY green_tripdata_2025-11.parquet .
RUN uv sync --locked

COPY Pipeline.py Pipeline.py

ENTRYPOINT [ "python", "Pipeline.py" ]
