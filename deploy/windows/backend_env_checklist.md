# FinFlow Backend `.env` Checklist

Use this checklist after pulling code onto a server.

## 1. Create the real env file

Copy:

`backend/.env.example` -> `backend/.env`

## 2. Required for normal backend startup

Fill these keys with server values:

- `DB_DIALECT`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `SECRET_KEY`
- `APP_HOST`
- `APP_PORT`

Choose one encryption method:

- `ENCRYPTION_KEY`
- `ENCRYPTION_KEY_FILE`

## 3. Required only if related features are used

Marki sync:

- `MARKI_USER`
- `MARKI_PASSWORD`
- `MARKI_SYSTEM_ID`

Kingdee integration:

- `KINGDEE_APP_ID`
- `KINGDEE_APP_SECRET`

## 4. Common server-side checks

- `backend/.env` exists on the server
- `backend/.encryption.key` exists if `ENCRYPTION_KEY_FILE` is used
- SQL Server ODBC driver matches `DB_DRIVER`
- `APP_PORT` is not occupied
- `ALLOWED_ORIGINS` includes the real frontend access origin

## 5. Important note

`backend/.env` is intentionally ignored by Git, so `git pull` will not sync your real server configuration.

## 6. Production-only service management

On the server, FinFlow Manager should start:

- backend in production runtime mode
- frontend as built static files from `frontend/dist`

Do not use Vite dev server or any hot-reload workflow in server-side service management.
