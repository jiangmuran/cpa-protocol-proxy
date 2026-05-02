# CPA Protocol Proxy

Transparent Python front proxy for CPA / CLIProxyAPI with compatibility fixes
for NewAPI Anthropic Messages streaming conversions.

It is designed for this traffic shape:

```text
clients or NewAPI -> original CPA port -> cpa-protocol-proxy -> CLIProxyAPI backend
```

The proxy forwards all HTTP paths and WebSocket upgrades to the upstream CPA
service. It does not need to understand every model API route, so OpenAI,
Anthropic, Responses, embeddings, files, and provider-specific paths can pass
through unchanged. The only intentional mutation is for OpenAI-compatible
streaming `POST /v1/chat/completions` responses: one empty assistant role SSE
chunk is inserted before the first upstream chunk.

## Why This Exists

Some NewAPI versions can convert Anthropic `/v1/messages` calls to an
OpenAI-compatible `/v1/chat/completions` channel and emit a malformed Anthropic
SSE sequence when the upstream stream has too few chunks before the terminal
usage/finish chunk. Strict Anthropic clients then fail with:

```text
API Error: Content block not found
```

Adding an OpenAI-style empty assistant bootstrap chunk gives NewAPI's converter
the expected stream shape and prevents the duplicate `message_start` sequence.

The proxy can optionally retry a streaming chat completion if the upstream ends
successfully without any user-visible text, tool call, refusal, or audio
output. This is intended for intermittent successful-but-empty streams,
including streams that only contain hidden reasoning. It is disabled by default
because retrying sends another upstream model request and can increase cost.

## Features

- Transparent HTTP pass-through for all paths and methods.
- WebSocket upgrade forwarding.
- OpenAI SSE bootstrap injection for streaming chat completions.
- Optional empty-output retry for streaming chat completions.
- Hop-by-hop header filtering and uncompressed upstream SSE forwarding.
- Health endpoint at `/_health`.
- systemd unit and environment file included.

## Quick Install

```bash
install -d /opt/cpa-protocol-proxy
cp cpa_protocol_proxy.py requirements.txt /opt/cpa-protocol-proxy/
cp cpa-protocol-proxy.service /etc/systemd/system/cpa-protocol-proxy.service
cp cpa-protocol-proxy.env /etc/cpa-protocol-proxy.env

cd /opt/cpa-protocol-proxy
python3 -m venv venv
./venv/bin/pip install -r requirements.txt

systemctl daemon-reload
systemctl enable --now cpa-protocol-proxy
curl -sS http://127.0.0.1:8317/_health
```

Example layout when preserving the original CPA public port:

```text
cpa-protocol-proxy listens on 0.0.0.0:8317
CLIProxyAPI listens on 127.0.0.1:8318
NewAPI Docker reaches the proxy at http://172.18.0.1:8317
```

For a safer sidecar deployment, keep CLIProxyAPI on `8317`, run this proxy on
`8320`, and point only selected NewAPI channels to
`http://172.18.0.1:8320`. If UFW is enabled, allow the Docker bridge subnet to
that sidecar port.

In CLIProxyAPI config:

```yaml
host: "127.0.0.1"
port: 8318
```

## Configuration

`cpa-protocol-proxy.env`:

```bash
LISTEN_HOST=0.0.0.0
LISTEN_PORT=8317
UPSTREAM_BASE_URL=http://127.0.0.1:8318
INJECT_OPENAI_STREAM_BOOTSTRAP=1
BOOTSTRAP_BUFFER_LIMIT=1048576
EMPTY_OUTPUT_RETRY_ATTEMPTS=0
EMPTY_OUTPUT_PREFETCH_LIMIT=1048576
EMPTY_OUTPUT_TREAT_REASONING_AS_OUTPUT=0
CLIENT_MAX_SIZE=1073741824
UPSTREAM_CONN_LIMIT=0
LOG_REQUESTS=0
LOG_SLOW_SECONDS=30
SHUTDOWN_TIMEOUT=3
LOG_LEVEL=INFO
```

Set `INJECT_OPENAI_STREAM_BOOTSTRAP=0` if your NewAPI version no longer needs
the Anthropic stream compatibility fix. A single request can also opt out with:

```text
X-CPA-Proxy-No-Bootstrap: 1
```

Set `EMPTY_OUTPUT_RETRY_ATTEMPTS=1` only if you explicitly accept the extra
upstream request cost for successful-but-empty stream retries.

Set `EMPTY_OUTPUT_TREAT_REASONING_AS_OUTPUT=1` if your clients display
`reasoning_content` as useful output and you do not want reasoning-only streams
to be retried.

`UPSTREAM_CONN_LIMIT=0` removes aiohttp's default total connection cap of 100.
This matters for long-lived streaming workloads, where a capped connector can
turn into a hidden queue. `LOG_REQUESTS=0` avoids writing one journal line per
successful request; errors and requests slower than `LOG_SLOW_SECONDS` are still
logged.

## CLIProxyAPI Empty-Stream Mitigation

If CLIProxyAPI itself returns this error, the Python proxy has not seen the first
upstream SSE frame yet and cannot repair the stream shape:

```text
empty_stream: upstream stream closed before first payload
```

Use a narrow CLIProxyAPI-side mitigation before enabling broad request retries:

```yaml
request-retry: 0
max-retry-credentials: 0
routing:
  strategy: "round-robin"
streaming:
  keepalive-seconds: 15
  bootstrap-retries: 1
```

`streaming.bootstrap-retries: 1` only retries a streaming request before the
first byte has been sent downstream. This is the lowest-blast-radius retry knob
for intermittent first-payload disconnects. Keep `request-retry` at `0` unless
you intentionally accept broader duplicate upstream calls for normal HTTP 5xx
failures. `round-robin` also prevents one unhealthy credential from absorbing all
traffic under `fill-first`.

## Testing

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests
python -m py_compile cpa_protocol_proxy.py
```

## Operational Notes

- Keep secrets in your CPA/NewAPI services, not in this repository.
- Restarting this single-process proxy briefly stops accepting new connections.
  The included unit keeps stop time short with `TimeoutStopSec=5`.
- If you update NewAPI channels directly in the database, restart NewAPI or
  clear its channel cache so it picks up the new base URL.
