# CPA Protocol Proxy Migration Note

## Purpose

This service is a transparent Python front proxy for CLIProxyAPI.

Default flow after migration:

```text
newapi/external clients -> original CPA port 8317 -> protocol proxy -> CPA backend on 127.0.0.1:8318
```

It forwards all HTTP paths and WebSocket upgrades to CPA. The only intentional
mutation is for OpenAI-compatible streaming `POST /v1/chat/completions`: it
adds one empty assistant SSE chunk before the first upstream chunk. This avoids
NewAPI producing a duplicate Anthropic `message_start` when NewAPI converts
`/v1/messages` requests through an OpenAI-compatible channel.

## Files

```bash
/opt/cpa-protocol-proxy/cpa_protocol_proxy.py
/opt/cpa-protocol-proxy/requirements.txt
/etc/cpa-protocol-proxy.env
/etc/systemd/system/cpa-protocol-proxy.service
```

## Service Commands

```bash
systemctl status cpa-protocol-proxy --no-pager
journalctl -u cpa-protocol-proxy -f
systemctl restart cpa-protocol-proxy
```

Health check:

```bash
curl -sS http://127.0.0.1:8317/_health
```

## NewAPI Channel Migration

With the proxy owning the original CPA port, NewAPI channels do not need to
change. Keep existing CPA-backed channels on:

```text
http://172.18.0.1:8317
```

If you deploy on a different port instead, update CPA-backed channels:

```sql
update channels
set base_url = 'http://172.18.0.1:<proxy-port>'
where base_url = 'http://172.18.0.1:<old-cpa-port>';
```

If NewAPI keeps using an old channel cache after a direct SQL update, restart
the container:

```bash
docker restart new-api
```

## New Server Migration Checklist

1. Install Python 3 and venv support.
2. Copy `/opt/cpa-protocol-proxy` and `/etc/cpa-protocol-proxy.env`.
3. Recreate the virtualenv:

   ```bash
   cd /opt/cpa-protocol-proxy
   python3 -m venv venv
   ./venv/bin/pip install -r requirements.txt
   ```

4. Install and enable the service:

   ```bash
   cp /opt/cpa-protocol-proxy/cpa-protocol-proxy.service /etc/systemd/system/cpa-protocol-proxy.service
   systemctl daemon-reload
   systemctl enable --now cpa-protocol-proxy
   ```

5. Confirm CPA is reachable from the proxy host at `UPSTREAM_BASE_URL`.
6. Point NewAPI CPA-backed channels to the proxy address. If you preserve the
   original CPA port, keep `http://172.18.0.1:8317`. If you choose a different
   proxy port, use that port with the Docker host gateway IP.
7. Test:

   ```bash
   curl -sS http://127.0.0.1:8317/_health
   ```

## Important Config

`/etc/cpa-protocol-proxy.env`:

```bash
LISTEN_HOST=0.0.0.0
LISTEN_PORT=8317
UPSTREAM_BASE_URL=http://127.0.0.1:8318
INJECT_OPENAI_STREAM_BOOTSTRAP=1
EMPTY_OUTPUT_RETRY_ATTEMPTS=1
EMPTY_OUTPUT_PREFETCH_LIMIT=1048576
EMPTY_OUTPUT_TREAT_REASONING_AS_OUTPUT=0
SHUTDOWN_TIMEOUT=3
```

CPA itself should listen only on the backend port:

```yaml
host: "127.0.0.1"
port: 8318
```

Set `INJECT_OPENAI_STREAM_BOOTSTRAP=0` only if NewAPI fixes its Anthropic
stream converter and you want a fully byte-for-byte OpenAI stream.

`EMPTY_OUTPUT_RETRY_ATTEMPTS=1` retries likely NewAPI `/v1/messages` converted
OpenAI streams once if CPA ends the stream without any user-visible text, tool
call, refusal, or audio output. Hidden reasoning does not count by default,
which guards the intermittent successful-but-empty response pattern.

`SHUTDOWN_TIMEOUT=3` and `TimeoutStopSec=5` keep service restarts short. During
a restart, the proxy stops accepting new traffic, so prefer restarting during a
quiet window or use a second temporary port if zero downtime is required.

## Rollback To Direct CPA

```bash
systemctl stop cpa-protocol-proxy
perl -0pi -e 's/^host: "127\\.0\\.0\\.1"\\nport: 8318/host: "0.0.0.0"\\nport: 8317/m' /etc/cliproxyapi/config.yaml
systemctl restart cliproxyapi
```
