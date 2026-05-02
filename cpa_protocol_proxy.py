#!/usr/bin/env python3
"""Transparent CPA front proxy with a NewAPI Anthropic-stream compatibility fix.

The proxy forwards all HTTP paths to CLIProxyAPI. For OpenAI-compatible
`/v1/chat/completions` streaming responses it injects one empty assistant
bootstrap chunk before the first upstream SSE chunk. This gives NewAPI's
`/v1/messages -> OpenAI-compatible channel -> /v1/chat/completions` converter
at least two upstream chunks before the final usage/finish chunk, avoiding the
duplicate `message_start` event that causes strict Anthropic clients to report
`Content block not found`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Iterable
from urllib.parse import urlsplit, urlunsplit

from aiohttp import ClientSession, ClientTimeout, TCPConnector, WSMsgType, web
from aiohttp.client_exceptions import ClientConnectionResetError


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

WEBSOCKET_HEADERS = {
    "sec-websocket-key",
    "sec-websocket-version",
    "sec-websocket-extensions",
    "sec-websocket-protocol",
}

DEFAULT_BOOTSTRAP_BUFFER_LIMIT = 1024 * 1024
DEFAULT_EMPTY_OUTPUT_PREFETCH_LIMIT = 1024 * 1024


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def should_log_request(request: web.Request, status: int, elapsed: float, client_closed: bool = False) -> bool:
    if status >= 400:
        return True
    if client_closed and request.app["log_client_closes"]:
        return True
    if request.app["log_requests"]:
        return True
    return elapsed >= request.app["log_slow_seconds"]


def filtered_headers(headers, extra_skip: Iterable[str] = ()) -> dict[str, str]:
    skip = HOP_BY_HOP_HEADERS | {h.lower() for h in extra_skip}
    return {
        key: value
        for key, value in headers.items()
        if key.lower() not in skip
    }


def build_forward_headers(request: web.Request, websocket: bool = False) -> dict[str, str]:
    extra_skip = {"host", "content-length", "x-cpa-proxy-no-bootstrap"}
    if websocket:
        extra_skip |= WEBSOCKET_HEADERS

    headers = filtered_headers(request.headers, extra_skip)
    headers["X-Forwarded-Host"] = request.host
    headers["X-Forwarded-Proto"] = request.scheme

    peer = request.transport.get_extra_info("peername") if request.transport else None
    if peer:
        remote_ip = peer[0]
        prior = request.headers.get("X-Forwarded-For")
        headers["X-Forwarded-For"] = f"{prior}, {remote_ip}" if prior else remote_ip

    # Keep upstream responses uncompressed so SSE boundaries remain visible.
    headers["Accept-Encoding"] = "identity"
    return headers


def build_upstream_url(base_url: str, raw_path: str, websocket: bool = False) -> str:
    base = base_url.rstrip("/")
    if not websocket:
        return f"{base}{raw_path}"

    parts = urlsplit(base)
    scheme = "wss" if parts.scheme == "https" else "ws"
    return urlunsplit((scheme, parts.netloc, parts.path.rstrip("/") + raw_path, "", ""))


def parse_json_body(body: bytes) -> dict:
    if not body:
        return {}
    try:
        parsed = json.loads(body.decode("utf-8"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def is_openai_chat_stream_request(request: web.Request, body: bytes) -> bool:
    if request.method.upper() != "POST":
        return False
    if not request.path.rstrip("/").endswith("/chat/completions"):
        return False
    payload = parse_json_body(body)
    return payload.get("stream") is True


def looks_like_newapi_claude_messages_conversion(request: web.Request, body: bytes) -> bool:
    if not is_openai_chat_stream_request(request, body):
        return False
    payload = parse_json_body(body)
    # NewAPI's Claude Messages -> OpenAI conversion uses max_tokens. Native
    # GPT-5 chat clients commonly use max_completion_tokens or Responses.
    return "max_tokens" in payload and "max_completion_tokens" not in payload


def is_chat_completions_stream(request: web.Request, body: bytes, content_type: str, status: int) -> bool:
    if request.app["inject_bootstrap"] is False:
        return False
    if request.headers.get("x-cpa-proxy-no-bootstrap", "").lower() in {"1", "true", "yes"}:
        return False
    if not is_openai_chat_stream_request(request, body):
        return False
    if status != 200:
        return False
    if "text/event-stream" not in content_type.lower():
        return False
    return is_openai_chat_stream_request(request, body)


def first_sse_frame(buffer: bytes) -> tuple[bytes | None, bytes]:
    positions = []
    for sep in (b"\n\n", b"\r\n\r\n"):
        pos = buffer.find(sep)
        if pos >= 0:
            positions.append((pos, len(sep)))
    if not positions:
        return None, buffer
    pos, sep_len = min(positions, key=lambda item: item[0])
    end = pos + sep_len
    return buffer[:end], buffer[end:]


def sse_data(frame: bytes) -> str | None:
    text = frame.decode("utf-8", errors="replace").replace("\r\n", "\n")
    data_lines = []
    for line in text.split("\n"):
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip(" "))
    if not data_lines:
        return None
    return "\n".join(data_lines)


def parse_sse_json(frame: bytes) -> dict:
    data = sse_data(frame)
    if not data or data == "[DONE]":
        return {}
    try:
        parsed = json.loads(data)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def openai_frame_has_visible_output(frame: bytes, treat_reasoning_as_output: bool = False) -> bool:
    payload = parse_sse_json(frame)
    choices = payload.get("choices")
    if not isinstance(choices, list):
        return False
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        delta = choice.get("delta")
        message = choice.get("message")
        if isinstance(delta, dict):
            if delta.get("content"):
                return True
            if treat_reasoning_as_output and (delta.get("reasoning_content") or delta.get("reasoning")):
                return True
            if delta.get("refusal") or delta.get("audio"):
                return True
            if delta.get("tool_calls") or delta.get("function_call"):
                return True
        if isinstance(message, dict):
            if message.get("content"):
                return True
            if message.get("tool_calls") or message.get("function_call"):
                return True
        if choice.get("text"):
            return True
    return False


def openai_frame_is_terminal(frame: bytes) -> bool:
    data = sse_data(frame)
    if data == "[DONE]":
        return True
    payload = parse_sse_json(frame)
    choices = payload.get("choices")
    if not isinstance(choices, list):
        return False
    for choice in choices:
        if isinstance(choice, dict) and choice.get("finish_reason"):
            return True
    return False


def build_bootstrap_chunk(request_body: bytes, first_frame: bytes | None) -> bytes | None:
    request_payload = parse_json_body(request_body)
    model = request_payload.get("model") or "unknown"
    first_payload = {}

    if first_frame is not None:
        data = sse_data(first_frame)
        if data == "[DONE]":
            return None
        if data:
            try:
                parsed = json.loads(data)
                if isinstance(parsed, dict):
                    first_payload = parsed
            except Exception:
                first_payload = {}

    choices = []
    upstream_choices = first_payload.get("choices")
    if isinstance(upstream_choices, list) and upstream_choices:
        for offset, choice in enumerate(upstream_choices):
            choice_index = choice.get("index", offset) if isinstance(choice, dict) else offset
            choices.append({
                "index": choice_index,
                "delta": {"role": "assistant"},
                "finish_reason": None,
            })
    else:
        choices.append({
            "index": 0,
            "delta": {"role": "assistant"},
            "finish_reason": None,
        })

    chunk = {
        "id": first_payload.get("id") or f"chatcmpl-cpa-proxy-{uuid.uuid4().hex}",
        "object": first_payload.get("object") or "chat.completion.chunk",
        "created": first_payload.get("created") or int(time.time()),
        "model": first_payload.get("model") or model,
        "choices": choices,
    }
    if first_payload.get("system_fingerprint"):
        chunk["system_fingerprint"] = first_payload["system_fingerprint"]

    encoded = json.dumps(chunk, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return b"data: " + encoded + b"\n\n"


async def stream_response(
    request: web.Request,
    upstream_resp,
    request_body: bytes,
    started_at: float,
    initial_chunks: list[bytes] | None = None,
    attempt: int = 1,
) -> web.StreamResponse:
    response_headers = filtered_headers(upstream_resp.headers, {"content-length"})
    downstream = web.StreamResponse(status=upstream_resp.status, headers=response_headers)
    await downstream.prepare(request)

    inject = is_chat_completions_stream(
        request,
        request_body,
        upstream_resp.headers.get("Content-Type", ""),
        upstream_resp.status,
    )
    injected = False
    buffered = b""
    bytes_sent = 0

    async def write_chunk(chunk: bytes) -> None:
        nonlocal buffered, bytes_sent, injected
        if not chunk:
            return
        if not inject or injected:
            await downstream.write(chunk)
            bytes_sent += len(chunk)
            return

        buffered += chunk
        frame, rest = first_sse_frame(buffered)
        if frame is None and len(buffered) < request.app["bootstrap_buffer_limit"]:
            return

        if frame is None:
            bootstrap = build_bootstrap_chunk(request_body, None)
            if bootstrap:
                await downstream.write(bootstrap)
                bytes_sent += len(bootstrap)
                request["bootstrap_injected"] = True
            await downstream.write(buffered)
            bytes_sent += len(buffered)
            buffered = b""
            injected = True
            return

        bootstrap = build_bootstrap_chunk(request_body, frame)
        if bootstrap:
            await downstream.write(bootstrap)
            bytes_sent += len(bootstrap)
            request["bootstrap_injected"] = True
        await downstream.write(frame)
        bytes_sent += len(frame)
        if rest:
            await downstream.write(rest)
            bytes_sent += len(rest)
        buffered = b""
        injected = True

    try:
        for chunk in initial_chunks or []:
            await write_chunk(chunk)

        async for chunk in upstream_resp.content.iter_chunked(64 * 1024):
            await write_chunk(chunk)

        if buffered:
            await downstream.write(buffered)
            bytes_sent += len(buffered)

        await downstream.write_eof()
    except ClientConnectionResetError:
        upstream_resp.close()
        elapsed = time.monotonic() - started_at
        if should_log_request(request, upstream_resp.status, elapsed, client_closed=True):
            logging.info(
                "%s %s -> client closed %.3fs bytes=%s bootstrap=%s attempt=%s",
                request.method,
                request.raw_path,
                elapsed,
                bytes_sent,
                request.get("bootstrap_injected", False),
                attempt,
            )
        return downstream
    finally:
        upstream_resp.release()

    elapsed = time.monotonic() - started_at
    if should_log_request(request, upstream_resp.status, elapsed):
        logging.info(
            "%s %s -> %s %.3fs bytes=%s bootstrap=%s attempt=%s",
            request.method,
            request.raw_path,
            upstream_resp.status,
            elapsed,
            bytes_sent,
            request.get("bootstrap_injected", False),
            attempt,
        )
    return downstream


async def prefetch_until_visible_or_terminal(request: web.Request, upstream_resp) -> tuple[list[bytes], bool, bool, bool]:
    chunks: list[bytes] = []
    parser_buffer = b""
    total = 0
    has_visible_output = False
    terminal = False
    overflow = False

    async for chunk in upstream_resp.content.iter_chunked(64 * 1024):
        if not chunk:
            continue
        chunks.append(chunk)
        parser_buffer += chunk
        total += len(chunk)

        while True:
            frame, rest = first_sse_frame(parser_buffer)
            if frame is None:
                break
            parser_buffer = rest
            if openai_frame_has_visible_output(frame, request.app["empty_output_treat_reasoning_as_output"]):
                has_visible_output = True
            if openai_frame_is_terminal(frame):
                terminal = True
            if has_visible_output or terminal:
                return chunks, has_visible_output, terminal, overflow

        if total >= request.app["empty_output_prefetch_limit"]:
            overflow = True
            return chunks, has_visible_output, terminal, overflow

    return chunks, has_visible_output, True, overflow


async def proxy_http_with_empty_retry(
    request: web.Request,
    body: bytes,
    upstream_url: str,
    headers: dict[str, str],
    started_at: float,
) -> web.StreamResponse:
    attempts = max(1, request.app["empty_output_retry_attempts"] + 1)
    last_result = None

    for attempt in range(1, attempts + 1):
        upstream_resp = await request.app["session"].request(
            request.method,
            upstream_url,
            data=body if body else None,
            headers=headers,
            allow_redirects=False,
        )
        try:
            content_type = upstream_resp.headers.get("Content-Type", "")
            should_prefetch = upstream_resp.status == 200 and "text/event-stream" in content_type.lower()
            if not should_prefetch:
                return await stream_response(request, upstream_resp, body, started_at, attempt=attempt)

            chunks, visible, terminal, overflow = await prefetch_until_visible_or_terminal(request, upstream_resp)
            last_result = (upstream_resp, chunks, visible, terminal, overflow, attempt)
            if terminal and not visible and not overflow and attempt < attempts:
                logging.warning(
                    "%s %s upstream ended with no visible output; retrying attempt=%s",
                    request.method,
                    request.raw_path,
                    attempt + 1,
                )
                upstream_resp.release()
                continue
            return await stream_response(
                request,
                upstream_resp,
                body,
                started_at,
                initial_chunks=chunks,
                attempt=attempt,
            )
        except Exception:
            upstream_resp.close()
            raise

    upstream_resp, chunks, _, _, _, attempt = last_result
    return await stream_response(
        request,
        upstream_resp,
        body,
        started_at,
        initial_chunks=chunks,
        attempt=attempt,
    )


async def proxy_http(request: web.Request) -> web.StreamResponse:
    if request.path in {"/_health", "/healthz"}:
        return web.json_response({
            "ok": True,
            "upstream": request.app["upstream_base_url"],
            "bootstrap_fix": request.app["inject_bootstrap"],
            "empty_output_retry_attempts": request.app["empty_output_retry_attempts"],
            "empty_output_treat_reasoning_as_output": request.app["empty_output_treat_reasoning_as_output"],
            "upstream_conn_limit": request.app["upstream_conn_limit"],
            "log_requests": request.app["log_requests"],
        })

    if request.headers.get("upgrade", "").lower() == "websocket":
        return await proxy_websocket(request)

    started_at = time.monotonic()
    body = await request.read()
    upstream_url = build_upstream_url(request.app["upstream_base_url"], request.raw_path)
    headers = build_forward_headers(request)

    try:
        if request.app["empty_output_retry_attempts"] > 0 and is_openai_chat_stream_request(request, body):
            return await proxy_http_with_empty_retry(request, body, upstream_url, headers, started_at)
        async with request.app["session"].request(
            request.method,
            upstream_url,
            data=body if body else None,
            headers=headers,
            allow_redirects=False,
        ) as upstream_resp:
            return await stream_response(request, upstream_resp, body, started_at)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logging.exception("proxy_http failed for %s %s: %s", request.method, request.raw_path, exc)
        return web.json_response(
            {"error": {"message": f"proxy upstream error: {exc}", "type": "proxy_error"}},
            status=502,
        )


async def proxy_websocket(request: web.Request) -> web.WebSocketResponse:
    started_at = time.monotonic()
    upstream_url = build_upstream_url(request.app["upstream_base_url"], request.raw_path, websocket=True)
    protocols_header = request.headers.get("Sec-WebSocket-Protocol", "")
    protocols = [item.strip() for item in protocols_header.split(",") if item.strip()]
    headers = build_forward_headers(request, websocket=True)

    try:
        async with request.app["session"].ws_connect(
            upstream_url,
            headers=headers,
            protocols=protocols,
            autoping=True,
            autoclose=True,
        ) as upstream_ws:
            downstream_ws = web.WebSocketResponse(protocols=protocols)
            await downstream_ws.prepare(request)

            async def client_to_upstream() -> None:
                async for msg in downstream_ws:
                    if msg.type == WSMsgType.TEXT:
                        await upstream_ws.send_str(msg.data)
                    elif msg.type == WSMsgType.BINARY:
                        await upstream_ws.send_bytes(msg.data)
                    elif msg.type == WSMsgType.PING:
                        await upstream_ws.ping(msg.data)
                    elif msg.type == WSMsgType.PONG:
                        await upstream_ws.pong(msg.data)
                    elif msg.type == WSMsgType.CLOSE:
                        await upstream_ws.close()

            async def upstream_to_client() -> None:
                async for msg in upstream_ws:
                    if msg.type == WSMsgType.TEXT:
                        await downstream_ws.send_str(msg.data)
                    elif msg.type == WSMsgType.BINARY:
                        await downstream_ws.send_bytes(msg.data)
                    elif msg.type == WSMsgType.PING:
                        await downstream_ws.ping(msg.data)
                    elif msg.type == WSMsgType.PONG:
                        await downstream_ws.pong(msg.data)
                    elif msg.type == WSMsgType.CLOSE:
                        await downstream_ws.close()

            await asyncio.gather(client_to_upstream(), upstream_to_client())
            logging.info(
                "WS %s -> closed %.3fs",
                request.raw_path,
                time.monotonic() - started_at,
            )
            return downstream_ws
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logging.exception("proxy_websocket failed for %s: %s", request.raw_path, exc)
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.close(code=1011, message=str(exc).encode("utf-8", errors="replace")[:120])
        return ws


async def create_app() -> web.Application:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=log_level, format="%(asctime)s %(levelname)s %(message)s")

    app = web.Application(client_max_size=env_int("CLIENT_MAX_SIZE", 1024 * 1024 * 1024))
    app["upstream_base_url"] = os.getenv("UPSTREAM_BASE_URL", "http://127.0.0.1:8317")
    app["inject_bootstrap"] = env_bool("INJECT_OPENAI_STREAM_BOOTSTRAP", True)
    app["bootstrap_buffer_limit"] = env_int("BOOTSTRAP_BUFFER_LIMIT", DEFAULT_BOOTSTRAP_BUFFER_LIMIT)
    app["empty_output_retry_attempts"] = env_int("EMPTY_OUTPUT_RETRY_ATTEMPTS", 0)
    app["empty_output_prefetch_limit"] = env_int("EMPTY_OUTPUT_PREFETCH_LIMIT", DEFAULT_EMPTY_OUTPUT_PREFETCH_LIMIT)
    app["empty_output_treat_reasoning_as_output"] = env_bool("EMPTY_OUTPUT_TREAT_REASONING_AS_OUTPUT", False)
    app["upstream_conn_limit"] = env_int("UPSTREAM_CONN_LIMIT", 0)
    app["log_requests"] = env_bool("LOG_REQUESTS", False)
    app["log_client_closes"] = env_bool("LOG_CLIENT_CLOSES", False)
    app["log_slow_seconds"] = env_float("LOG_SLOW_SECONDS", 30.0)

    timeout = ClientTimeout(total=None, connect=30, sock_connect=30, sock_read=None)
    connector = TCPConnector(
        limit=app["upstream_conn_limit"],
        limit_per_host=env_int("UPSTREAM_CONN_LIMIT_PER_HOST", 0),
        ttl_dns_cache=env_int("DNS_CACHE_TTL", 300),
        keepalive_timeout=env_float("UPSTREAM_KEEPALIVE_TIMEOUT", 75.0),
        enable_cleanup_closed=True,
    )
    app["session"] = ClientSession(
        timeout=timeout,
        connector=connector,
        auto_decompress=False,
        read_bufsize=env_int("READ_BUFFER_SIZE", 1024 * 1024),
    )

    app.router.add_route("*", "/{tail:.*}", proxy_http)

    async def close_session(app_: web.Application) -> None:
        await app_["session"].close()

    app.on_cleanup.append(close_session)
    return app


def main() -> None:
    host = os.getenv("LISTEN_HOST", "0.0.0.0")
    port = env_int("LISTEN_PORT", 8320)
    shutdown_timeout = env_int("SHUTDOWN_TIMEOUT", 3)
    web.run_app(create_app(), host=host, port=port, access_log=None, shutdown_timeout=shutdown_timeout)


if __name__ == "__main__":
    main()
