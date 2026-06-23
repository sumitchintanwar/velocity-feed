#!/usr/bin/env python3
"""
scripts/ws_client.py — Quick WebSocket test client for RTMDS.

Usage:
    python scripts/ws_client.py --url ws://localhost:8080/ws --symbols AAPL TSLA

Requires: pip install websocket-client
"""
import argparse
import json
import signal
import sys

try:
    import websocket
except ImportError:
    sys.exit("Error: run 'pip install websocket-client' first")


def main() -> None:
    parser = argparse.ArgumentParser(description="RTMDS WebSocket test client")
    parser.add_argument("--url", default="ws://localhost:8080/ws", help="Server WebSocket URL")
    parser.add_argument("--symbols", nargs="+", default=["AAPL", "TSLA"], help="Symbols to subscribe")
    args = parser.parse_args()

    ws = websocket.WebSocket()
    print(f"Connecting to {args.url} …")
    ws.connect(args.url)

    subscribe_msg = json.dumps({"action": "subscribe", "symbols": args.symbols})
    ws.send(subscribe_msg)
    print(f"Subscribed to: {args.symbols}\n{'─' * 60}")

    def _shutdown(sig, frame):
        print("\nDisconnecting …")
        ws.send(json.dumps({"action": "unsubscribe", "symbols": args.symbols}))
        ws.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)

    while True:
        try:
            raw = ws.recv()
            msg = json.loads(raw)
            payload = msg.get("payload", {})
            if isinstance(payload, dict):
                print(
                    f"{payload.get('symbol', '?'):6s}  "
                    f"price={payload.get('price', 0):>10.4f}  "
                    f"bid={payload.get('bid', 0):>10.4f}  "
                    f"ask={payload.get('ask', 0):>10.4f}  "
                    f"ts={payload.get('timestamp', '')}"
                )
        except websocket.WebSocketConnectionClosedException:
            print("Connection closed by server.")
            break


if __name__ == "__main__":
    main()
