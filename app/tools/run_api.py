from __future__ import annotations

import argparse

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the TrailOps FastAPI server (local-first).")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host. Use 127.0.0.1 for local-only.")
    parser.add_argument("--port", type=int, default=8000, help="Bind port.")
    args = parser.parse_args()

    uvicorn.run(
        "app.api.server:app",
        host=args.host,
        port=args.port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
