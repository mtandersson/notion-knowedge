#!/usr/bin/env python3
"""Bounded local OAuth public client; no credentials, tokens or callback logging."""
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import secrets
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import ProxyHandler, Request, build_opener
import json

ISSUER = "http://localhost:3300"
REDIRECT = "http://localhost:3301/callback"
CLIENT = "knowledge-chatgpt"


def main():
    verifier = secrets.token_urlsafe(48)
    state = secrets.token_urlsafe(32)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    result = {"done": False, "success": False}

    class Callback(BaseHTTPRequestHandler):
        timeout = 5
        def log_message(self, *_args):
            pass

        def do_GET(self):
            url = urlsplit(self.path)
            params = parse_qs(url.query, keep_blank_values=True)
            valid = (url.path == "/callback" and len(self.path) <= 8192
                     and self.headers.get("Host") == "localhost:3301"
                     and set(params) == {"code", "state"}
                     and all(len(v) == 1 and v[0] for v in params.values())
                     and secrets.compare_digest(params["state"][0], state))
            if not valid:
                self.send_error(400, "Invalid local callback")
                return
            result["done"] = True
            try:
                body = urlencode({
                    "grant_type": "authorization_code", "client_id": CLIENT,
                    "redirect_uri": REDIRECT, "resource": f"{ISSUER}/mcp",
                    "code": params["code"][0], "code_verifier": verifier,
                }).encode()
                request = Request(f"{ISSUER}/token", data=body,
                                  headers={"Content-Type": "application/x-www-form-urlencoded"})
                # No proxies or redirects: authorization material stays at the
                # preconfigured loopback endpoint.
                from urllib.request import HTTPRedirectHandler

                class NoRedirect(HTTPRedirectHandler):
                    def redirect_request(self, *_args, **_kwargs):
                        return None

                with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=20) as response:
                    token = json.loads(response.read(16385))
                result["success"] = (isinstance(token, dict)
                                     and isinstance(token.get("access_token"), str)
                                     and bool(token["access_token"])
                                     and token.get("token_type", "").lower() == "bearer")
            except (HTTPError, URLError, TimeoutError, ValueError, OSError):
                pass
            self.send_response(200 if result["success"] else 502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(b"Local OAuth test passed. You can close this tab." if result["success"]
                             else b"Local OAuth exchange failed. Check sanitized server diagnostics.")

    with HTTPServer(("127.0.0.1", 3301), Callback) as server:
        server.timeout = 1
        authorization = f"{ISSUER}/authorize?" + urlencode({
            "response_type": "code", "client_id": CLIENT, "redirect_uri": REDIRECT,
            "resource": f"{ISSUER}/mcp", "scope": "knowledge:read", "state": state,
            "code_challenge": challenge, "code_challenge_method": "S256",
        })
        print("Open this URL in your local browser (expires in five minutes):", flush=True)
        print(authorization, flush=True)
        deadline = time.monotonic() + 300
        while not result["done"] and time.monotonic() < deadline:
            server.handle_request()
    print("Local OAuth test passed." if result["success"] else "Local OAuth test incomplete or failed.")
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
