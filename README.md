# bot_service

Outbound fallback bot workers for Wordness. They poll the orchestrator queue API and only join a compatible fallback-eligible request.

Copy `.env.example` to `.env`, then run:

```sh
docker compose -f compose.yml up --build -d
```

The compose file exposes no ports. It persists pool and active-match state in `bot_state` and mounts localization from `../localization/dist`.

`BOT_MATCH_SERVER_URL` is intentionally empty: the assignment's `server_url` is used. The remote queue endpoint must be HTTPS and have the same host as `ORCHESTRATOR_URL`; `host.docker.internal` may use HTTP/WS for local debug. Queue redirects are rejected.

Pool target may be changed with `python -m services.bot_service --pool on|off|COUNT` inside the container.
