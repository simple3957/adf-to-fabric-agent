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
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        print("WARNING: binding beyond loopback exposes the API; set ADF2FABRIC_API_TOKEN and use TLS in front of it.")
    uvicorn.run(create_app(), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
