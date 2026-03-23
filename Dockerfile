FROM python:3.14.3-slim@sha256:fb83750094b46fd6b8adaa80f66e2302ecbe45d513f6cece637a841e1025b4ca

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    HOME=/home/app

ARG APP_USER=app
ARG APP_UID=10001
ARG APP_GID=10001

WORKDIR /app

RUN addgroup --system --gid ${APP_GID} ${APP_USER} \
    && adduser --system --uid ${APP_UID} --ingroup ${APP_USER} --home /home/${APP_USER} ${APP_USER}

RUN apt-get update \
    && apt-get install --only-upgrade -y libc-bin libc6 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock \
    && pip check

COPY . .
RUN chown -R ${APP_UID}:${APP_GID} /app /home/${APP_USER}

USER ${APP_UID}:${APP_GID}

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
