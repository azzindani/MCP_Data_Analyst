"""Bearer-token auth for HTTP transport. Local/offline-friendly.

Optionally bridges an OAuth 2.0 surface (see oauth_bridge.py) for clients that
require it, such as claude.ai's Custom Connector — the OAuth-issued token maps
back to the same principal the plain bearer token would. Plain bearer tokens
keep working unchanged either way.
"""

from __future__ import annotations

import json
import os

from fastmcp.server.auth.auth import AccessToken, TokenVerifier

from shared.oauth_bridge import OAuthBridge


def _named_tokens_from_file(path: str) -> dict[str, str]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {str(name): str(token) for name, token in data.items()}


def _named_tokens_from_inline(spec: str) -> dict[str, str]:
    pairs = [p for p in spec.split(",") if p.strip()]
    return {name.strip(): token.strip() for name, token in (p.split(":", 1) for p in pairs)}


def load_named_tokens(prefix: str) -> dict[str, str]:
    """Resolve name -> token from env vars, Folio-style priority order.

    <PREFIX>_TOKENS_FILE (named tokens, JSON {name: token})
      > <PREFIX>_TOKENS (inline "name:token,name2:token2")
      > <PREFIX>_API_KEY (single shared token)
      > {} (open mode — no auth, for localhost/private-network use only).
    """
    tokens_file = os.environ.get(f"{prefix}_TOKENS_FILE", "").strip()
    if tokens_file:
        return _named_tokens_from_file(tokens_file)

    inline = os.environ.get(f"{prefix}_TOKENS", "").strip()
    if inline:
        return _named_tokens_from_inline(inline)

    api_key = os.environ.get(f"{prefix}_API_KEY", "").strip()
    return {"default": api_key} if api_key else {}


class _DynamicTokenVerifier(TokenVerifier):
    """Checks the static named-tokens dict first, then OAuth-issued tokens."""

    def __init__(self, named: dict[str, str], oauth_bridge: OAuthBridge | None, base_url: str | None = None) -> None:
        super().__init__(base_url=base_url)
        self._by_token = {token: name for name, token in named.items()}
        self._oauth_bridge = oauth_bridge

    async def verify_token(self, token: str) -> AccessToken | None:
        name = self._by_token.get(token)
        if name is not None:
            return AccessToken(token=token, client_id=name, scopes=[])
        if self._oauth_bridge is not None:
            principal = self._oauth_bridge.resolve_oauth_token(token)
            if principal is not None:
                return AccessToken(token=token, client_id=principal, scopes=[])
        return None


def build_token_verifier(
    prefix: str, oauth_bridge: OAuthBridge | None = None, base_url: str | None = None
) -> TokenVerifier | None:
    """Build bearer (+ optional OAuth) auth from env vars.

    Returns None in open mode (no tokens configured at all) — no auth, for
    localhost/private-network use only. oauth_bridge, if given, is consulted
    as a fallback whenever a presented token doesn't match a static one.

    base_url must be the PUBLIC HTTPS URL this server is reachable at,
    including any reverse-proxy mount prefix (e.g. "https://data.casava.space/basic")
    — fastmcp bakes it into the WWW-Authenticate `resource_metadata` hint on 401
    responses at app-build time (it can't be derived per-request the way
    oauth_bridge.py's own routes can via root_path). Without it, the 401 omits
    the hint entirely and mounted sub-servers (behind a path prefix) fail OAuth
    discovery — a bare, unprefixed deployment happens to still work because
    clients fall back to guessing the unprefixed default well-known path.
    """
    named = load_named_tokens(prefix)
    if not named:
        return None
    return _DynamicTokenVerifier(named, oauth_bridge, base_url=base_url)


def build_oauth_bridge(prefix: str, state_dir: str | None = None) -> OAuthBridge | None:
    """Build the OAuthBridge for this server, or None in open mode.

    state_dir defaults to a path derived from prefix alone (see oauth_bridge.py).
    Pass it explicitly when multiple sub-servers share one token prefix (e.g.
    Data_Analyst's 7 tiers) — each is a separate OS process, and without
    distinct state dirs they'd all persist to the same file and corrupt each
    other's state via uncoordinated concurrent writes.
    """
    named = load_named_tokens(prefix)
    if not named:
        return None
    by_token = {token: name for name, token in named.items()}

    def lookup_principal(presented: str) -> str | None:
        return by_token.get(presented)

    return OAuthBridge(prefix, lookup_principal, state_dir=state_dir)
