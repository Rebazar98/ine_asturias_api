FROM python:3.12.13-slim@sha256:ccc7089399c8bb65dd1fb3ed6d55efa538a3f5e7fca3f5988ac3b5b87e593bf0

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

COPY requirements.txt requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock \
    && pip check

COPY . .
RUN chown -R ${APP_UID}:${APP_GID} /app /home/${APP_USER}

USER ${APP_UID}:${APP_GID}

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]