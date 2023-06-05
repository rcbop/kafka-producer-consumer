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

COPY --chown=app:app consumer/ /app/consumer/
COPY --chown=app:app config.ini /app/consumer/

RUN pip install --no-cache-dir .

ENTRYPOINT [ "python", "/app/consumer/main.py" ]
CMD ["/app/consumer/config.ini"]
