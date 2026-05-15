# Gibran UI — Production Deployment

The Phase 4 UI ships as a FastAPI app + bundled React SPA. By default
it runs locally (`127.0.0.1`) with header-based identity, which is
fine for the developer-on-their-laptop case but **not safe to expose
on a network**. For prod, you want:

1. JWT-mode auth instead of dev headers
2. A reverse proxy in front (nginx / Caddy / Cloudflare) for TLS + rate limiting
3. The bundled frontend served from the wheel (no Node toolchain on prod boxes)

## Quick recipe

### 1. Install with the UI extras

```bash
pip install 'gibran[ui]'
```

If you didn't run `npm run build` in `frontend/` before publishing the
wheel, the `/` path serves a JSON placeholder instead of the SPA. Build
locally and republish, or copy the static bundle in.

### 2. Configure JWT auth

Set these env vars wherever you run `gibran ui` (systemd unit,
docker-compose `environment:`, k8s ConfigMap, etc.):

```bash
export GIBRAN_UI_AUTH_MODE=jwt
export GIBRAN_UI_JWT_JWKS_URL=https://your-idp.example.com/.well-known/jwks.json
# Optional:
export GIBRAN_UI_JWT_AUDIENCE=gibran
export GIBRAN_UI_JWT_ISSUER=https://your-idp.example.com/
```

Or, for HS256 with a shared secret (testing only):

```bash
export GIBRAN_UI_AUTH_MODE=jwt
export GIBRAN_UI_JWT_STATIC_KEY=<base64-32-bytes>
```

The JWT must carry a `sub` claim (user_id), `role` claim (role_id),
and an optional `attrs` claim (object with string values).

### 3. Bind to localhost; put nginx in front

```bash
gibran ui --host 127.0.0.1 --port 8000 --db /var/lib/gibran/gibran.duckdb
```

nginx config:

```nginx
server {
    listen 443 ssl http2;
    server_name gibran.example.com;
    ssl_certificate     /etc/letsencrypt/live/gibran.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/gibran.example.com/privkey.pem;

    # Rate limit per IP -- 60 req/min for queries, 600 for catalog reads
    limit_req_zone $binary_remote_addr zone=gibran_api:10m rate=10r/s;

    location /api/query {
        limit_req zone=gibran_api burst=20 nodelay;
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Authorization $http_authorization;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Authorization $http_authorization;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

The `gibran ui` CLI refuses to bind to non-localhost in dev auth mode
(`GIBRAN_UI_AUTH_MODE=dev`); flipping to JWT mode is the only way to
satisfy `--host 0.0.0.0`. This is intentional -- dev-mode headers are
unverified.

## What's NOT in V0.1

- **No screenshots in this docs page.** Take them locally after `gibran
  ui` is running; the original handoff (`PHASE_4_UI_HANDOFF.md`)
  documents the screenshot list.
- **No onboarding tour.** First-time `gibran ui` opens directly to the
  Workbench view with an identity-setup modal.
- **No dark-mode toggle UI.** The CSS respects `prefers-color-scheme:
  dark`; OS-level dark mode flips the theme automatically.
- **No CSRF tokens.** Same-origin policy + JWT bearer covers the
  defensive ground for the V0.1 endpoints. If you embed gibran inside
  a third-party app (`GIBRAN_UI_CORS_ORIGINS=...`), revisit.
- **No Lighthouse / axe-core CI integration.** Manual axe-core run via
  the browser extension before each release is the V0.1 recommendation.

## End-to-end smoke check

```bash
# 1. Bootstrap a DuckDB instance with the sample
gibran init --sample
gibran sync
gibran check

# 2. Start the UI
gibran ui --no-open

# 3. Open http://127.0.0.1:<port> in your browser, set identity to
#    user=adam role=analyst_west attrs=region=west, and run:
#    "top 5 region by gross revenue"
#
# Expected: 3 rows (west / east / north -- north has 1, west has 2,
# east has 1), each with a numeric revenue total.
```
