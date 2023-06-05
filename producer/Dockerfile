FROM python:3.9-alpine

RUN apk add --no-cache \
    gcc \
    musl-dev \
    librdkafka-dev

RUN adduser -D app
USER app

WORKDIR /app

COPY --chown=app:app requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app pyproject.toml /app/

COPY --chown=app:app producer/ /app/producer/
COPY --chown=app:app config.ini /app/producer/

RUN pip install --no-cache-dir .

ENTRYPOINT [ "python", "/app/producer/main.py" ]
CMD ["/app/producer/config.ini"]
