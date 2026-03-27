from __future__ import annotations

import time
from typing import Callable

import httpx

from .constants import MAX_RETRIES

ProgressCallback = Callable[[dict], None]


class MinerUError(Exception):
    pass


def api_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "*/*",
    }


def build_client(timeout_seconds: int, *, follow_redirects: bool = True) -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(timeout_seconds),
        follow_redirects=follow_redirects,
        http2=False,
        trust_env=False,
    )


def request_with_retry(method: str, url: str, **kwargs) -> httpx.Response:
    timeout_seconds = kwargs.pop("_timeout_seconds", 120)
    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with build_client(timeout_seconds) as client:
                response = client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError, httpx.TimeoutException) as exc:
            last_exc = exc
            if attempt == MAX_RETRIES:
                break
            time.sleep(min(2 * attempt, 5))
        except httpx.HTTPStatusError:
            raise

    raise MinerUError(f"Network request failed after {MAX_RETRIES} attempts: {last_exc}")


class ProgressEmitter:
    def __init__(self, progress_callback: ProgressCallback | None, provider: str = "mineru"):
        self._callback = progress_callback
        self._provider = provider

    def emit(self, stage: str, message: str, **payload) -> None:
        if not self._callback:
            return
        packet = {
            "stage": stage,
            "message": message,
            "provider": self._provider,
            **payload,
        }
        try:
            self._callback(packet)
        except Exception:
            return
