"""토스 OAuth2 토큰 관리.

WebSocket은 핸드셰이크 시점에만 토큰을 검증한다. 연결 중 만료돼도 끊기지 않는다.
문제는 재연결이다 — 끊긴 뒤 붙을 때 토큰이 만료돼 있으면 그때 실패한다.
그래서 재연결 경로에서 항상 유효 토큰을 보장해야 한다.
"""
from __future__ import annotations

import logging
import time

import httpx

log = logging.getLogger(__name__)


class TokenError(RuntimeError):
    pass


class TokenProvider:
    def __init__(self, api_base: str, client_id: str, client_secret: str,
                 refresh_margin_s: float = 3600.0) -> None:
        self._url = f"{api_base.rstrip('/')}/oauth2/token"
        self._client_id = client_id
        self._client_secret = client_secret
        self._margin = refresh_margin_s
        self._token: str | None = None
        self._expires_at: float = 0.0

    def _expired(self) -> bool:
        return self._token is None or time.time() >= self._expires_at - self._margin

    async def get(self, client: httpx.AsyncClient, force: bool = False) -> str:
        if force or self._expired():
            await self._issue(client)
        assert self._token is not None
        return self._token

    async def _issue(self, client: httpx.AsyncClient) -> None:
        try:
            r = await client.post(
                self._url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=10.0,
            )
        except httpx.HTTPError as e:
            # 예외 메시지에 자격 증명이 섞여 나가지 않도록 타입만 남긴다.
            raise TokenError(f"토큰 발급 요청 실패: {type(e).__name__}") from None

        if r.status_code != 200:
            raise TokenError(f"토큰 발급 실패: HTTP {r.status_code}")

        body = r.json()
        token = body.get("access_token")
        if not token:
            raise TokenError("응답에 access_token 이 없습니다")

        # 문서 기준 86400초. 없으면 보수적으로 1시간으로 본다.
        ttl = float(body.get("expires_in", 3600))
        self._token = token
        self._expires_at = time.time() + ttl
        log.info("토큰 발급 완료 (ttl=%.0fs, 갱신여유=%.0fs)", ttl, self._margin)
