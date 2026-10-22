"""``python -m adf2fabric.api`` — start the API on loopback. Pass ``--demo`` to start with the demo backend selected by default."""

from __future__ import annotations

import argparse


def main() -> None:
    import uvicorn

    from adf2fabric.api.app import create_app

    ap = argparse.ArgumentParser(prog="python -m adf2fabric.api")
    ap.add_argument("--host", default="127.0.0.1", help="Bind address (default loopback; the API holds your Azure session).")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()
    app = create_app()
    mode = app.state.security.effective_mode(app.state.users.has_users())
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        if mode != "token":
            raise SystemExit("Refusing to bind beyond loopback without authentication. Create users (`adf2fabric auth-user-create`) or set "
                             "ADF2FABRIC_API_TOKEN, then start again. Put TLS in front of the API: it receives bearer tokens.")
        print("NOTE: bound beyond loopback. Terminate TLS in front of this server and set ADF2FABRIC_ALLOWED_HOSTS / _ORIGINS to your real names.")
    elif mode == "disabled":
        print("Authentication is OFF (local single-user mode: loopback only). Create a user to turn it on: adf2fabric auth-user-create --help")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info", server_header=False, proxy_headers=False)

if __name__ == "__main__":
    main()
