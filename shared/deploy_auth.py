"""Bearer-token auth for HTTP transport. Local/offline-friendly, no OAuth server."""

from __future__ import annotations

import json
import os

from fastmcp.server.auth.providers.jwt import StaticTokenVerifier


def _named_tokens_from_file(path: str) -> dict[str, str]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {str(name): str(token) for name, token in data.items()}


def _named_tokens_from_inline(spec: str) -> dict[str, str]:
    pairs = [p for p in spec.split(",") if p.strip()]
    return {name.strip(): token.strip() for name, token in (p.split(":", 1) for p in pairs)}


def build_token_verifier(prefix: str) -> StaticTokenVerifier | None:
    """Build bearer auth from env vars, Folio-style priority order.

    <PREFIX>_TOKENS_FILE (named tokens, JSON {name: token})
      > <PREFIX>_TOKENS (inline "name:token,name2:token2")
      > <PREFIX>_API_KEY (single shared token)
      > None (open mode — no auth, for localhost/private-network use only).
    """
    tokens_file = os.environ.get(f"{prefix}_TOKENS_FILE", "").strip()
    if tokens_file:
        named = _named_tokens_from_file(tokens_file)
    else:
        inline = os.environ.get(f"{prefix}_TOKENS", "").strip()
        if inline:
            named = _named_tokens_from_inline(inline)
        else:
            api_key = os.environ.get(f"{prefix}_API_KEY", "").strip()
            named = {"default": api_key} if api_key else {}

    if not named:
        return None

    return StaticTokenVerifier(tokens={token: {"client_id": name, "scopes": []} for name, token in named.items()})
